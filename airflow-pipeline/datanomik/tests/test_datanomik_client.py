import base64
import contextlib
import io
import json
import logging
import secrets
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from requests.adapters import BaseAdapter

from datanomik_client import CredentialsError, DatanomikClient, DatanomikError, date_filters


class MockTransport(BaseAdapter):
    """Exercises real Session/PreparedRequest Basic authentication without sockets."""

    def __init__(self, responses):
        super().__init__()
        self.responses = iter(responses)
        self.calls = []

    def send(self, request, **kwargs):
        self.calls.append((request, kwargs))
        item = next(self.responses)
        if isinstance(item, Exception):
            raise item
        status, payload, headers = item
        response = requests.Response()
        response.status_code = status
        response._content = (payload if isinstance(payload, str) else json.dumps(payload)).encode()
        response._content_consumed = True
        response.headers.update(headers)
        response.request = request
        return response

    def close(self):
        pass


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.username, self.password = secrets.token_hex(16), secrets.token_hex(32)
        self.path = Path(self.directory.name) / "synthetic_credentials.json"
        self.path.write_text(json.dumps({"datanomik": {
            "secretId": self.username, "secretPassword": self.password,
            "baseUrl": "https://api.datanomik.com",
        }}))

    def client(self, responses, **kwargs):
        transport = MockTransport(responses)
        session = requests.Session()
        session.mount("https://", transport)
        client = DatanomikClient(self.path, session=session, sleep=Mock(), **kwargs)
        self.addCleanup(session.close)
        return client, transport

    def test_basic_auth_accept_and_timeout_without_logging_secrets(self):
        client, transport = self.client([(200, {"content": [], "last": True}, {})])
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        logging.getLogger().addHandler(handler)
        self.addCleanup(logging.getLogger().removeHandler, handler)
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            self.assertEqual(list(client.transactions("test-account")), [])
        request, kwargs = transport.calls[0]
        expected = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        self.assertTrue(request.headers["Authorization"] == "Basic " + expected)
        self.assertEqual(request.headers["Accept"], "application/json")
        self.assertEqual(kwargs["timeout"], (10, 60))
        self.assertFalse(any(s in output.getvalue() + repr(client) for s in client._secrets))

    def test_pages_start_at_zero_support_all_envelopes(self):
        for envelope in ("content", "data", "items", "results"):
            with self.subTest(envelope=envelope):
                client, transport = self.client([
                    (200, {envelope: [{"id": i} for i in range(100)], "totalPages": 2}, {}),
                    (200, {envelope: [{"id": 100}], "last": True}, {}),
                ])
                self.assertEqual(len(list(client.accounts())), 101)
                self.assertIn("page=0&size=100", transport.calls[0][0].url)
                self.assertIn("page=1&size=100", transport.calls[1][0].url)

    def test_total_pages_terminates_even_when_page_is_full(self):
        client, transport = self.client([(200, {"data": [{}] * 100, "totalPages": 1}, {})])
        self.assertEqual(len(list(client.accounts())), 100)
        self.assertEqual(len(transport.calls), 1)

    def test_retry_429_503_and_exponential_backoff(self):
        client, transport = self.client([
            (429, {"message": "Too many requests"}, {"Retry-After": "5"}),
            (503, {"message": "Service Unavailable"}, {}),
            (200, {"items": []}, {}),
        ])
        self.assertEqual(list(client.accounts()), [])
        self.assertEqual(len(transport.calls), 3)
        self.assertEqual([call.args[0] for call in client._sleep.call_args_list], [5, 2])

    def test_all_retryable_statuses_and_exhaustion(self):
        for status in (429, 500, 502, 503, 504):
            client, transport = self.client([(status, {}, {})] * 3, retries=2)
            with self.assertRaises(DatanomikError):
                list(client.accounts())
            self.assertEqual(len(transport.calls), 3)

    def test_401_fails_once_with_safe_message_and_trace(self):
        client, transport = self.client([(401, {"message": "Unauthorized", "traceId": "trace-test"}, {})])
        with self.assertRaises(DatanomikError) as caught:
            list(client.accounts())
        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(str(caught.exception),
                         "GET /v1/accounts status=401 traceId=trace-test message=Unauthorized")
        self.assertTrue(caught.exception.permanent)

    def test_permanent_statuses_and_redirects_are_never_retried(self):
        for status in (400, 401, 403, 404, 301, 302, 307):
            client, transport = self.client([(status, {}, {"Location": "https://invalid.example"})])
            with self.assertRaises(DatanomikError):
                list(client.accounts())
            self.assertEqual(len(transport.calls), 1)
            client._sleep.assert_not_called()

    def test_echoed_credentials_and_headers_are_discarded(self):
        client, _ = self.client([(401, {"message": self.username + self.password,
                                       "traceId": self.password,
                                       "Authorization": self.password}, {})])
        with self.assertRaises(DatanomikError) as caught:
            list(client.transactions("test-account"))
        self.assertFalse(any(s in str(caught.exception) for s in client._secrets))
        self.assertNotIn("Authorization", str(caught.exception))

    def test_transport_exception_does_not_leak_url_or_secrets(self):
        client, _ = self.client([requests.ConnectionError(self.password)] * 2, retries=1)
        with self.assertRaises(DatanomikError) as caught:
            list(client.accounts())
        self.assertNotIn(self.password, str(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)

    def test_page_limit_and_malformed_pages_fail(self):
        client, _ = self.client([(200, {"items": [{}] * 100}, {})], max_pages=1)
        with self.assertRaisesRegex(DatanomikError, "Page safety limit"):
            list(client.accounts())
        for payload in ({"unexpected": []}, {"data": ["bad"]}, {"data": [], "last": "false"}):
            client, _ = self.client([(200, payload, {})])
            with self.assertRaises(DatanomikError):
                list(client.accounts())

    def test_numeric_amount_decoded_as_decimal(self):
        client, _ = self.client([(200, '{"items":[{"amount":0.10}]}', {})])
        self.assertIsInstance(list(client.accounts())[0]["amount"], Decimal)

    def test_runtime_environment_path_and_invalid_credential_diagnostics(self):
        with patch.dict("os.environ", {"DATANOMIK_CREDENTIALS_FILE": str(self.path)}):
            with DatanomikClient() as client:
                self.assertTrue(client._session.auth[0] == self.username)
        self.path.write_text(self.password)
        with self.assertRaises(CredentialsError) as caught:
            DatanomikClient(self.path)
        self.assertNotIn(self.password, str(caught.exception))

    def test_untrusted_base_url_is_rejected(self):
        payload = json.loads(self.path.read_text())
        payload["datanomik"]["baseUrl"] = "https://invalid.example"
        self.path.write_text(json.dumps(payload))
        with self.assertRaises(CredentialsError):
            DatanomikClient(self.path)

    def test_documented_date_parameters(self):
        self.assertEqual(date_filters("2026-01-01", "2026-01-31"),
                         {"dateFrom": "2026-01-01", "dateTo": "2026-01-31"})
        self.assertEqual(date_filters(), {})
        client, transport = self.client([(200, [], {})])
        list(client.transactions("account", "2026-01-01", "2026-01-31"))
        self.assertIn("dateFrom=2026-01-01", transport.calls[0][0].url)
        for args in (("2026-02-30", None), ("2026-02-01", "2026-01-01"), ("20260101", None)):
            with self.assertRaises(ValueError):
                date_filters(*args)
