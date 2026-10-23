import importlib.util
import os
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from datanomik_io import atomic_path, read_jsonl, write_jsonl
from datanomik_transform import GlobalTransform, normalize_text_fields, write_parquet

HAS_ARROW = importlib.util.find_spec("pyarrow") is not None


class TransformTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def transform(self, groups):
        engine = GlobalTransform(self.root / "test.sqlite")
        self.addCleanup(engine.connection.close)
        for index, (account, records) in enumerate(groups):
            path = self.root / f"{index}.jsonl"
            write_jsonl(path, records)
            engine.stage(account, path)
        report = engine.deduplicate()
        return engine, report

    def test_exactly_ten_duplicates_absolute_total_49600(self):
        first = [{"external_id": str(i), "account": {"id": "a"},
                  "amount": Decimal("4960.00"), "description": "first"} for i in range(10)]
        duplicates = [{**row, "amount": Decimal("-4960.00") if i % 2 else Decimal("4960.00"),
                       "description": "duplicate"} for i, row in enumerate(first)]
        engine, report = self.transform([("a", first), ("a", duplicates)])
        stats = report["consolidado"]
        self.assertEqual(stats["quantidadeOriginal"], 20)
        self.assertEqual(stats["quantidadeFinal"], 10)
        self.assertEqual(stats["duplicidadesRemovidas"], 10)
        self.assertEqual(stats["valorDuplicadoRemovidoAbsoluto"], "49600.00")
        self.assertEqual(Decimal(stats["valorDuplicadoRemovido"]), Decimal("0.00"))
        self.assertEqual(stats["gruposComValorDivergente"], 5)
        self.assertEqual(list(engine.records("a")), first)

    def test_same_external_id_in_different_accounts_survives(self):
        engine, report = self.transform([
            ("a", [{"external_id": "x", "account": {"id": "a"}, "amount": "0.1"}]),
            ("b", [{"external_id": "x", "account": {"id": "b"}, "amount": "0.1"}]),
        ])
        self.assertEqual(report["consolidado"]["quantidadeFinal"], 2)
        self.assertEqual(len(list(engine.records("b"))), 1)

    def test_missing_keys_are_kept_including_repeated_missing_rows(self):
        records = [{"external_id": "", "account": {"id": "a"}},
                   {"external_id": "x", "account": {}},
                   {"external_id": None, "account": {"id": "a"}},
                   {"external_id": "x", "account": {"id": " "}}, {}]
        engine, report = self.transform([("a", records * 2)])
        self.assertEqual(report["consolidado"]["registrosSemChave"], 10)
        self.assertEqual(len(list(engine.records("a"))), 10)

    def test_global_dedup_across_all_files_keeps_first_selection_order(self):
        record = {"external_id": "x", "account": {"id": "a"}, "amount": "1.00"}
        engine, report = self.transform([("first", [record]), ("second", [{**record, "amount": "2.00"}])])
        self.assertEqual(report["consolidado"]["duplicidadesRemovidas"], 1)
        self.assertEqual(report["porConta"]["first"]["gruposComValorDivergente"], 1)
        self.assertEqual(report["porConta"]["second"]["valorDuplicadoRemovido"], "2.00")
        self.assertEqual(list(engine.records("second")), [])

    def test_text_aliases_zeros_nested_objects_and_empty_sender_document(self):
        record = {"account": {"id": "id", "number": "0000079-5", "branch": "0001"},
                  "account_number": "0000079-5", "accountNumber": "0000079-5",
                  "branch": "0001", "agency": "001", "agencia": 3,
                  "document": "001234", "document_number": "001234", "documentNumber": 123,
                  "cnpj": "00123456789012", "cpf": "00012345678",
                  "sender": {"document": None}, "beneficiary": {"document": "0012"},
                  "date": "2026-01-01", "nested": {"empty": {}, "list": [1, 2]}}
        normalized = normalize_text_fields(record)
        self.assertEqual(normalized["account"]["number"], "0000079-5")
        self.assertEqual(normalized["agency"], "001")
        self.assertEqual(normalized["agencia"], "3")
        self.assertEqual(normalized["documentNumber"], "123")
        self.assertIsNone(normalized["sender"]["document"])
        self.assertNotIn("settlement_time", normalized)
        self.assertEqual(normalized["nested"], record["nested"])
        self.assertEqual(normalize_text_fields({"sender": {"document": ""}})["sender"]["document"], "")

    def test_missing_money_is_counted_without_fabricating_amount(self):
        first = {"external_id": "x", "account": {"id": "a"}}
        engine, report = self.transform([("a", [first, first])])
        self.assertEqual(report["consolidado"]["duplicidadesSemValor"], 1)
        self.assertNotIn("amount", list(engine.records("a"))[0])

    def test_fractional_values_use_decimal_without_rounding(self):
        rows = [{"external_id": "x", "account": {"id": "a"}, "amount": Decimal("0.10")}] * 4
        _, report = self.transform([("a", rows)])
        self.assertEqual(report["consolidado"]["valorDuplicadoRemovido"], "0.30")

    def test_invalid_amount_fails_instead_of_silent_zero(self):
        with self.assertRaises(ValueError):
            self.transform([("a", [{"amount": "invalid"}])])

    def test_atomic_failed_write_preserves_previous_output(self):
        path = self.root / "output.jsonl"
        path.write_text("old")
        with self.assertRaises(RuntimeError):
            with atomic_path(path) as temporary:
                temporary.write_text("partial")
                raise RuntimeError("interrupted")
        self.assertEqual(path.read_text(), "old")
        self.assertEqual(list(self.root.glob(".tmp-*")), [])

    def test_raw_jsonl_roundtrip_preserves_decimal_and_no_float(self):
        path = self.root / "raw.jsonl"
        rows = [{"amount": Decimal("12345678901234567890.001"), "text": "00001", "nested": {}}]
        write_jsonl(path, rows)
        self.assertEqual(list(read_jsonl(path)), rows)
        with self.assertRaises(ValueError):
            write_jsonl(path, [{"amount": 0.1}])
        self.assertEqual(list(read_jsonl(path)), rows)

    @unittest.skipUnless(HAS_ARROW, "PyArrow unavailable in host")
    def test_real_parquet_nested_schema_union_decimal_and_text(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        rows = [
            {"account": {"id": "a", "number": "0000079-5", "branch": "0001"},
             "amount": Decimal("12.30"), "sender": {"document": None}, "empty": {},
             "document_number": "000123", "mixed": {"child": True}},
            {"account": {"id": "a", "number": "0000079-5", "branch": "0001"},
             "amount": Decimal("1.1234"), "sender": {"document": ""}, "new_field": ["x"],
             "mixed": "text"},
        ]
        path = self.root / "transactions.parquet"
        self.assertEqual(write_parquet(path, lambda: iter(rows), batch_size=1), 2)
        table = pq.ParquetFile(path).read()
        result = table.to_pylist()
        self.assertEqual(result[0]["record"]["account"]["number"], "0000079-5")
        self.assertIsNone(result[0]["record"]["sender"]["document"])
        self.assertEqual(result[1]["record"]["sender"]["document"], "")
        self.assertTrue(pa.types.is_decimal(table.schema.field("record").type.field("amount").type))
        self.assertEqual(result[1]["record"]["amount"], Decimal("1.1234"))
        from datanomik_client import decode_json
        self.assertEqual([decode_json(row["source_record"]) for row in result], rows)

    @unittest.skipUnless(HAS_ARROW, "PyArrow unavailable in host")
    def test_empty_parquet_is_valid(self):
        import pyarrow.parquet as pq

        path = self.root / "empty.parquet"
        self.assertEqual(write_parquet(path, lambda: iter([])), 0)
        self.assertEqual(pq.ParquetFile(path).read().num_rows, 0)


class FullEnvironmentTests(unittest.TestCase):
    def test_required_dependencies_present_in_full_validation(self):
        if os.getenv("DATANOMIK_REQUIRE_FULL_TESTS") == "1":
            self.assertTrue(HAS_ARROW, "Full validation requires PyArrow")
            self.assertIsNotNone(importlib.util.find_spec("airflow"), "Full validation requires Airflow")
