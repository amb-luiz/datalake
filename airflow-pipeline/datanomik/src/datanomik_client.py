"""Runtime-only Datanomik client. This module performs no I/O on import."""

import base64
import json
import os
import re
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests

DEFAULT_CREDENTIALS_FILE = "/opt/airflow/secrets/datanomik_airflow.json"
BASE_URL = "https://api.datanomik.com"
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
SAFE_MESSAGES = frozenset({
    "Unauthorized", "Invalid credentials", "Forbidden", "Bad Request",
    "Too Many Requests", "Too many requests", "Internal Server Error",
    "Service Unavailable", "Bad Gateway", "Gateway Timeout",
    "Invalid or missing Basic Auth credentials.",
    "Missing or invalid Basic Auth credentials.",
})


class CredentialsError(ValueError):
    """Contains a fixed diagnostic, never JSON content or parser exceptions."""


class DatanomikError(RuntimeError):
    def __init__(self, endpoint, status=None, trace_id=None, message="API request failed"):
        self.status_code = status
        self.permanent = status is not None and 400 <= status < 500 and status != 429
        super().__init__(
            f"GET {endpoint} status={status} traceId={trace_id} message={message}"
        )


def _reject_constant(_value):
    raise ValueError("Non-finite JSON number")


def decode_json(text):
    return json.loads(text, parse_float=Decimal, parse_constant=_reject_constant)


def date_filters(date_from=None, date_to=None):
    """Documented v1 query names; values are calendar dates without invented times."""
    result = {}
    for name, value in (("dateFrom", date_from), ("dateTo", date_to)):
        if value is None:
            continue
        if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError("Dates must use YYYY-MM-DD")
        try:
            date.fromisoformat(value)
        except ValueError:
            raise ValueError("Invalid calendar date") from None
        result[name] = value
    if date_from and date_to and date_from > date_to:
        raise ValueError("date_from must not exceed date_to")
    return result


class DatanomikClient:
    """Instantiate inside task execution only; one session per mapped task."""

    def __init__(self, credentials_file=None, *, session=None, max_pages=10000,
                 retries=3, timeout=(10, 60), backoff=1, max_delay=120, sleep=time.sleep):
        if max_pages < 1 or retries < 0 or min(timeout) <= 0:
            raise ValueError("Invalid HTTP limits")
        path = credentials_file or os.getenv("DATANOMIK_CREDENTIALS_FILE", DEFAULT_CREDENTIALS_FILE)
        try:
            config = decode_json(Path(path).read_text(encoding="utf-8"))["datanomik"]
            username, password = config["secretId"], config["secretPassword"]
            base_url = config.get("baseUrl", BASE_URL)
            valid = all(isinstance(v, str) and v.strip() for v in (username, password))
            valid = valid and ":" not in username and base_url.rstrip("/") == BASE_URL
            # requests Basic auth encodes Latin-1; validate before constructing requests.
            username.encode("latin-1")
            password.encode("latin-1")
            if not valid:
                raise ValueError
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            raise CredentialsError("Invalid or inaccessible Datanomik credentials file") from None
        self._session = session if session is not None else requests.Session()
        self._session.auth = (username, password)
        self._session.headers.update({"Accept": "application/json"})
        # Ignore .netrc/proxies; still permit the deployment's explicit CA bundle.
        self._session.trust_env = False
        self._session.verify = os.getenv("REQUESTS_CA_BUNDLE", True)
        self._secrets = (username, password, base64.b64encode(
            f"{username}:{password}".encode("latin-1")).decode("ascii"))
        self.max_pages, self.retries, self.timeout = max_pages, retries, timeout
        self.backoff, self.max_delay, self._sleep = backoff, max_delay, sleep

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self._session.close()

    def _error(self, endpoint, response):
        status, trace, message = response.status_code, None, "API request failed"
        try:
            body = decode_json(response.text)
        except (ValueError, TypeError):
            body = {}
        if isinstance(body, dict):
            candidate = body.get("traceId")
            if isinstance(candidate, str) and re.fullmatch(r"[a-zA-Z0-9_.-]{1,128}", candidate):
                if not any(secret in candidate for secret in self._secrets):
                    trace = candidate
            candidate = body.get("message")
            # Free-form messages may echo credentials, headers, documents or URLs.
            if isinstance(candidate, str) and candidate in SAFE_MESSAGES:
                if not any(secret in candidate for secret in self._secrets):
                    message = candidate
        return DatanomikError(endpoint, status, trace, message)

    def _delay(self, attempt, retry_after=None):
        delay = min(self.max_delay, self.backoff * (2 ** attempt))
        if retry_after:
            try:
                delay = max(delay, int(retry_after))
            except (TypeError, ValueError):
                try:
                    retry_date = parsedate_to_datetime(retry_after)
                    delay = max(delay, (retry_date - datetime.now(timezone.utc)).total_seconds())
                except (TypeError, ValueError, OverflowError):
                    pass
        self._sleep(min(self.max_delay, max(0, delay)))

    def _get(self, endpoint, params):
        if endpoint not in {"/v1/accounts", "/v1/transactions"}:
            raise ValueError("Unsupported endpoint")
        for attempt in range(self.retries + 1):
            try:
                response = self._session.get(
                    BASE_URL + endpoint, params=params, timeout=self.timeout,
                    allow_redirects=False,
                )
            except (requests.Timeout, requests.ConnectionError):
                if attempt < self.retries:
                    self._delay(attempt)
                    continue
                raise DatanomikError(endpoint, message="Network request failed") from None
            except requests.RequestException:
                raise DatanomikError(endpoint, message="Network request failed") from None
            try:
                if response.status_code in RETRY_STATUSES and attempt < self.retries:
                    retry_after = response.headers.get("Retry-After")
                elif response.status_code != 200:
                    raise self._error(endpoint, response) from None
                else:
                    try:
                        return decode_json(response.text)
                    except (ValueError, TypeError):
                        raise DatanomikError(endpoint, 200, message="Invalid JSON response") from None
            finally:
                response.close()
            self._delay(attempt, retry_after)
        raise AssertionError("Unreachable retry state")

    def iter_pages(self, endpoint, **filters):
        for page in range(self.max_pages):
            payload = self._get(endpoint, {**filters, "page": page, "size": 100})
            metadata = payload if isinstance(payload, dict) else {}
            if isinstance(payload, list):
                records = payload
            elif isinstance(payload, dict):
                records = next((payload[k] for k in ("content", "data", "items", "results")
                                if isinstance(payload.get(k), list)), None)
            else:
                records = None
            if records is None or any(not isinstance(row, dict) for row in records):
                raise DatanomikError(endpoint, 200, message="Invalid page structure")
            total = metadata.get("totalPages")
            last = metadata.get("last")
            if total is not None and (type(total) is not int or total < 0):
                raise DatanomikError(endpoint, 200, message="Invalid page metadata")
            if last is not None and not isinstance(last, bool):
                raise DatanomikError(endpoint, 200, message="Invalid page metadata")
            yield records
            if last is True or (total is not None and page + 1 >= total) or len(records) < 100:
                return
        raise DatanomikError(endpoint, 200, message="Page safety limit reached")

    def accounts(self):
        for page in self.iter_pages("/v1/accounts"):
            yield from page

    def transactions(self, account_id, date_from=None, date_to=None):
        for page in self.iter_pages(
            "/v1/transactions", accountId=account_id, **date_filters(date_from, date_to)
        ):
            yield from page
