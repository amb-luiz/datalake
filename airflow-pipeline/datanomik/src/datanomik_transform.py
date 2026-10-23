"""Global stable deduplication on disk, exact money and lossless Parquet records."""

import sqlite3
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path

from datanomik_client import decode_json
from datanomik_io import atomic_path, json_text, read_jsonl

TEXT_NAMES = {
    "accountnumber", "accountid", "branch", "branchnumber", "branchcode",
    "agency", "agencynumber", "agencycode", "agencia", "document",
    "documentnumber", "documentid", "cnpj", "cpf", "externalid", "id",
}


def normalize_text_fields(value, parent=""):
    if isinstance(value, list):
        return [normalize_text_fields(item, parent) for item in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        name = key.replace("_", "").lower()
        textual = name in TEXT_NAMES or (
            name == "number" and parent in {"account", "branch", "agency", "document"}
        ) or (name == "account" and not isinstance(item, (dict, list)))
        if textual and item is not None and not isinstance(item, (dict, list)):
            result[key] = str(item)
        else:
            result[key] = normalize_text_fields(item, name)
    return result


def money(record):
    value = record.get("amount")
    if value is None or value == "":
        return None
    if isinstance(value, (bool, float)) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("Invalid transaction amount; expected a decimal scalar")
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise ValueError("Invalid transaction amount") from None
    if not result.is_finite() or len(result.as_tuple().digits) > 76 or abs(result.adjusted()) > 75:
        raise ValueError("Transaction amount exceeds supported decimal range")
    return result


def _key(record):
    account = record.get("account")
    values = (record.get("external_id"), account.get("id") if isinstance(account, dict) else None)
    if any(v is None or (isinstance(v, str) and not v.strip()) for v in values):
        return None
    if any(not isinstance(v, (str, int)) or isinstance(v, bool) for v in values):
        raise ValueError("Invalid deduplication identifier type")
    return tuple(str(v) for v in values)


def _stats():
    return {
        "quantidadeOriginal": 0, "quantidadeFinal": 0, "duplicidadesRemovidas": 0,
        "valorDuplicadoRemovido": Decimal("0.00"),
        "valorDuplicadoRemovidoAbsoluto": Decimal("0.00"),
        "gruposComValorDivergente": 0, "registrosSemChave": 0,
        "registrosSemValor": 0, "duplicidadesSemValor": 0,
    }


def _report_stats(stats):
    return {key: (format(value, "f") if isinstance(value, Decimal) else value)
            for key, value in stats.items()}


class GlobalTransform:
    """Stage ALL source files before running deduplication. Memory stays bounded."""

    def __init__(self, database):
        self.connection = sqlite3.connect(database)
        self.connection.executescript("""
            CREATE TABLE raw (
                seq INTEGER PRIMARY KEY, source_account TEXT NOT NULL, payload TEXT NOT NULL
            );
            CREATE TABLE seen (
                external_id TEXT, account_id TEXT, first_amount TEXT,
                source_account TEXT, divergent INTEGER DEFAULT 0,
                PRIMARY KEY(external_id, account_id)
            );
            CREATE TABLE kept (
                seq INTEGER PRIMARY KEY, source_account TEXT NOT NULL, payload TEXT NOT NULL
            );
            CREATE INDEX kept_account ON kept(source_account, seq);
        """)
        self.accounts = []
        self._deduplicated = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.connection.close()

    def stage(self, account_id, path):
        if self._deduplicated:
            raise RuntimeError("All staging must precede deduplication")
        if account_id not in self.accounts:
            self.accounts.append(account_id)
        count = 0
        for record in read_jsonl(path):
            if not isinstance(record, dict):
                raise ValueError("Transaction must be an object")
            self.connection.execute("INSERT INTO raw(source_account,payload) VALUES (?,?)",
                                    (account_id, json_text(record)))
            count += 1
        self.connection.commit()
        return count

    def deduplicate(self):
        if self._deduplicated:
            raise RuntimeError("Deduplication already completed")
        self._deduplicated = True
        per_account = {account: _stats() for account in self.accounts}
        # 76 source digits + ample headroom for sums. No binary floats or rounding.
        with localcontext() as context:
            context.prec = 170
            for seq, account, payload in self.connection.execute(
                "SELECT seq,source_account,payload FROM raw ORDER BY seq"
            ):
                record = decode_json(payload)
                key, amount, stats = _key(record), money(record), per_account[account]
                stats["quantidadeOriginal"] += 1
                if amount is None:
                    stats["registrosSemValor"] += 1
                first = None
                if key is None:
                    stats["registrosSemChave"] += 1
                else:
                    first = self.connection.execute(
                        "SELECT first_amount FROM seen WHERE external_id=? AND account_id=?", key
                    ).fetchone()
                if first is not None:
                    stats["duplicidadesRemovidas"] += 1
                    if amount is None:
                        stats["duplicidadesSemValor"] += 1
                    else:
                        stats["valorDuplicadoRemovido"] += amount
                        stats["valorDuplicadoRemovidoAbsoluto"] += abs(amount)
                    original = Decimal(first[0]) if first[0] is not None else None
                    if original != amount:
                        self.connection.execute(
                            "UPDATE seen SET divergent=1 WHERE external_id=? AND account_id=?", key
                        )
                    continue
                stats["quantidadeFinal"] += 1
                if key is not None:
                    self.connection.execute(
                        "INSERT INTO seen(external_id,account_id,first_amount,source_account) "
                        "VALUES (?,?,?,?)", (*key, str(amount) if amount is not None else None, account)
                    )
                self.connection.execute("INSERT INTO kept VALUES (?,?,?)", (seq, account, payload))
            for account, count in self.connection.execute(
                "SELECT source_account,COUNT(*) FROM seen WHERE divergent=1 GROUP BY source_account"
            ):
                per_account[account]["gruposComValorDivergente"] = count
            consolidated = _stats()
            for stats in per_account.values():
                for key, value in stats.items():
                    consolidated[key] += value
        self.connection.commit()
        return {
            "porConta": {a: _report_stats(s) for a, s in per_account.items()},
            "consolidado": _report_stats(consolidated),
        }

    def records(self, account_id):
        for (payload,) in self.connection.execute(
            "SELECT payload FROM kept WHERE source_account=? ORDER BY seq", (account_id,)
        ):
            yield decode_json(payload)


def _shape(value):
    if value is None:
        return {"kind": "null"}
    if isinstance(value, dict):
        return {"kind": "struct", "fields": {k: _shape(v) for k, v in value.items()}}
    if isinstance(value, list):
        item_shape = {"kind": "null"}
        for item in value:
            item_shape = _merge(item_shape, _shape(item))
        return {"kind": "list", "item": item_shape}
    if isinstance(value, bool):
        return {"kind": "bool"}
    if isinstance(value, (int, Decimal)):
        value = Decimal(value)
        if not value.is_finite():
            raise ValueError("Non-finite source number")
        exponent = value.as_tuple().exponent
        return {"kind": "decimal", "scale": max(0, -exponent),
                "integer": max(0, len(value.as_tuple().digits) + exponent)}
    return {"kind": "string"}


def _merge(left, right):
    if left["kind"] == "null":
        return right
    if right["kind"] == "null":
        return left
    if left["kind"] != right["kind"]:
        return {"kind": "json"}  # Heterogeneous source: lossless JSON in this field.
    kind = left["kind"]
    if kind == "struct":
        fields = dict(left["fields"])
        for key, value in right["fields"].items():
            fields[key] = _merge(fields.get(key, {"kind": "null"}), value)
        return {"kind": kind, "fields": fields}
    if kind == "list":
        return {"kind": kind, "item": _merge(left["item"], right["item"])}
    if kind == "decimal":
        return {"kind": kind, "scale": max(left["scale"], right["scale"]),
                "integer": max(left["integer"], right["integer"])}
    return left


def _arrow_type(shape, pa):
    kind = shape["kind"]
    if kind == "struct" and shape["fields"]:
        return pa.struct([pa.field(k, _arrow_type(v, pa)) for k, v in shape["fields"].items()])
    if kind == "list":
        return pa.list_(_arrow_type(shape["item"], pa))
    if kind == "bool":
        return pa.bool_()
    if kind == "decimal":
        precision = max(1, shape["integer"] + shape["scale"])
        if precision <= 76:
            factory = pa.decimal128 if precision <= 38 else pa.decimal256
            return factory(precision, shape["scale"])
    return pa.string()


def _arrow_value(value, shape):
    if value is None:
        return None
    kind = shape["kind"]
    if kind == "struct":
        if not shape["fields"]:
            return json_text(value)
        return {k: _arrow_value(value.get(k), v) for k, v in shape["fields"].items()}
    if kind == "list":
        return [_arrow_value(item, shape["item"]) for item in value]
    if kind == "json":
        return json_text(value)
    if kind == "decimal":
        if max(1, shape["integer"] + shape["scale"]) > 76:
            return str(value)
        return Decimal(value)
    return value


def write_parquet(path, records_factory, batch_size=1000):
    """Two streaming passes: union ALL schemas, then write with bounded buffers.

    `source_record` preserves the complete original JSON (including absent keys,
    empty objects and conflicting source types). `record` is a normalized struct.
    Wrapping avoids name collisions with arbitrary source fields.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    shape = {"kind": "struct", "fields": {}}
    for record in records_factory():
        shape = _merge(shape, _shape(normalize_text_fields(record)))
    schema = pa.schema([
        pa.field("record", _arrow_type(shape, pa)),
        pa.field("source_record", pa.string(), nullable=False),
    ], metadata={b"datanomik.shape": json_text(shape).encode("utf-8"),
                 b"datanomik.source_record": b"Lossless source JSON; authoritative field presence/types"})
    count = 0
    with atomic_path(Path(path)) as temporary:
        with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
            batch = []
            for record in records_factory():
                batch.append({"record": _arrow_value(normalize_text_fields(record), shape),
                              "source_record": json_text(record)})
                count += 1
                if len(batch) >= batch_size:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                    batch.clear()
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
    return count
