"""Task functions independent of the Airflow scheduler and metadata database."""

import hashlib
import os
import re
import shutil
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo

from datanomik_client import DatanomikClient, date_filters
from datanomik_io import checksum, json_text, read_json, write_json, write_jsonl
from datanomik_transform import GlobalTransform, write_parquet

PROCESS_NAME = "pl_ambipar_Datanomik_Fluxo_Caixa"
OWNER_KEYS = ("owner", "owners", "holder", "holders", "accountHolder", "account_holder")
DOCUMENT_KEYS = ("document", "document_number", "documentNumber", "cnpj", "cpf")


def _digits(value):
    return re.sub(r"[^0-9]", "", str(value)) if value is not None else ""


def _documents(owner):
    if isinstance(owner, list):
        for item in owner:
            yield from _documents(item)
    elif isinstance(owner, dict):
        for key in DOCUMENT_KEYS:
            value = owner.get(key)
            if isinstance(value, dict):
                for member in ("number", "value", "documentNumber", "document_number"):
                    if value.get(member) is not None:
                        yield _digits(value[member])
            elif isinstance(value, (str, int)):
                yield _digits(value)
        for value in owner.get("documents", []) or []:
            if isinstance(value, dict):
                for member in ("number", "value", *DOCUMENT_KEYS):
                    if isinstance(value.get(member), (str, int)):
                        yield _digits(value[member])


def select_account_ids(account_ids=None, cnpj=None, *, client_factory=DatanomikClient):
    if account_ids is not None and (
        not isinstance(account_ids, list)
        or any(not isinstance(a, str) or not a.strip() for a in account_ids)
    ):
        raise ValueError("account_ids must be an array of nonempty strings")
    requested = list(dict.fromkeys(account_ids or []))
    target = _digits(cnpj) if cnpj else None
    if cnpj is not None and (not isinstance(cnpj, str) or len(target or "") != 14):
        raise ValueError("cnpj must contain exactly 14 digits")
    if requested and not target:
        return requested
    selected, found = [], set()
    with client_factory() as client:
        for account in client.accounts():
            account_id = account.get("id")
            if account_id is None or str(account_id).strip() == "":
                raise ValueError("Account response contains no id")
            account_id = str(account_id)
            if requested and account_id not in requested:
                continue
            found.add(account_id)
            if target and not any(target == document for key in OWNER_KEYS
                                  for document in _documents(account.get(key))):
                continue
            if account_id not in selected:
                selected.append(account_id)
    if requested and set(requested) - found:
        raise ValueError("Some requested accounts are absent from the accounts response")
    if target and not selected:
        raise ValueError("No account has a matching holder document")
    # Requested order is authoritative; otherwise preserve accounts API order.
    return [account for account in requested if account in selected] if requested else selected


def run_directory(root, logical_date, run_id):
    local_date = logical_date.astimezone(ZoneInfo("America/Sao_Paulo")).date().isoformat()
    run_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    return Path(root) / PROCESS_NAME / f"date={local_date}" / f"run={run_hash}"


def account_partition(account_id):
    return "account=" + hashlib.sha256(account_id.encode("utf-8")).hexdigest()


def prepare_run(params, logical_date, run_id, *, client_factory=DatanomikClient):
    date_filters(params.get("date_from"), params.get("date_to"))
    accounts = select_account_ids(params.get("account_ids"), params.get("cnpj"),
                                  client_factory=client_factory)
    if len(accounts) > int(os.getenv("DATANOMIK_MAX_ACCOUNTS", "1024")):
        raise ValueError("Account mapping limit reached")
    root = os.getenv("DATANOMIK_DATA_ROOT", "/opt/airflow/data/datanomik")
    directory = run_directory(root, logical_date, run_id)
    write_json(directory / "selection.json", {
        "accounts": accounts, "date_from": params.get("date_from"), "date_to": params.get("date_to"),
        "logical_date": logical_date.isoformat(), "run_id": run_id,
    })
    return {"run_dir": str(directory), "accounts": accounts}


def extract_account(account_id, run_dir, *, client_factory=DatanomikClient):
    directory = Path(run_dir)
    selection = read_json(directory / "selection.json")
    if account_id not in selection["accounts"]:
        raise ValueError("Account is not in the run selection")
    path = directory / "raw" / account_partition(account_id) / "transactions.jsonl"
    with client_factory(max_pages=int(os.getenv("DATANOMIK_MAX_PAGES", "10000"))) as client:
        descriptor = write_jsonl(path, client.transactions(
            account_id, selection["date_from"], selection["date_to"]
        ))
    descriptor["account_id"] = account_id
    write_json(path.parent / "manifest.json", descriptor)
    return str(path.parent / "manifest.json")


def transform_run(run_dir, manifests):
    directory = Path(run_dir)
    selection = read_json(directory / "selection.json")
    by_account = {}
    for manifest_path in manifests:
        descriptor = read_json(manifest_path)
        account = descriptor["account_id"]
        if account in by_account:
            raise ValueError("Repeated extraction manifest")
        by_account[account] = descriptor
    if set(by_account) != set(selection["accounts"]):
        raise ValueError("Extraction incomplete; refusing partial deduplication")
    descriptors = [by_account[account] for account in selection["accounts"]]
    for descriptor in descriptors:
        if checksum(descriptor["path"]) != descriptor["sha256"]:
            raise ValueError("Raw checksum mismatch")
    generation = hashlib.sha256(json_text(descriptors).encode("utf-8")).hexdigest()
    final_directory = directory / "treated" / f"generation={generation}"
    directory.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".tmp-transform-", dir=directory))
    try:
        with GlobalTransform(staging / "index.sqlite") as transformer:
            # Complete global staging is a barrier, including validation of every count.
            for descriptor in descriptors:
                count = transformer.stage(descriptor["account_id"], descriptor["path"])
                if count != descriptor["count"]:
                    raise ValueError("Raw count mismatch")
            report = transformer.deduplicate()
            report.update({"processo": PROCESS_NAME, "logical_date": selection["logical_date"],
                           "generation": generation, "ordemContas": selection["accounts"],
                           "raw": descriptors, "parquet": []})
            for account in selection["accounts"]:
                relative = Path(account_partition(account)) / "transactions.parquet"
                output = staging / relative
                count = write_parquet(output, lambda account=account: transformer.records(account))
                if count != report["porConta"][account]["quantidadeFinal"]:
                    raise ValueError("Parquet count mismatch")
                report["parquet"].append({"account_id": account,
                                          "path": str(final_directory / relative), "count": count,
                                          "sha256": checksum(output)})
        (staging / "index.sqlite").unlink()
        write_json(staging / "data_quality.json", report)
        final_directory.parent.mkdir(parents=True, exist_ok=True)
        if final_directory.exists():
            # Deterministic replays must agree; never replace a complete generation in place.
            if read_json(final_directory / "data_quality.json") != report:
                raise ValueError("Existing generation differs from replay")
        else:
            os.rename(staging, final_directory)
            parent_fd = os.open(final_directory.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        # This atomic report is the commit marker. Consumers must use its exact paths.
        write_json(directory / "data_quality.json", report)
        return {"report_path": str(directory / "data_quality.json"),
                "quantidadeFinal": report["consolidado"]["quantidadeFinal"]}
    finally:
        if staging.exists():
            shutil.rmtree(staging)
