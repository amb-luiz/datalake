import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from datanomik_io import read_json
from datanomik_pipeline import (
    extract_account,
    prepare_run,
    run_directory,
    select_account_ids,
    transform_run,
)
from test_datanomik_transform import HAS_ARROW


def factory_with(accounts=(), transactions=()):
    client = Mock()
    client.__enter__ = Mock(return_value=client)
    client.__exit__ = Mock(return_value=False)
    client.accounts.return_value = iter(accounts)
    client.transactions.side_effect = lambda *_args: iter(transactions)
    return Mock(return_value=client)


class SelectionTests(unittest.TestCase):
    def test_explicit_accounts_need_no_credentials_or_accounts_api(self):
        factory = Mock(side_effect=AssertionError("Must not instantiate client"))
        self.assertEqual(select_account_ids(["b", "a", "b"], client_factory=factory), ["b", "a"])
        factory.assert_not_called()

    def test_cnpj_matches_only_holder_documents_and_digits(self):
        factory = factory_with(accounts=[
            {"id": "a", "owner": {"document_number": "00.123.456/7890-12"}},
            {"id": "b", "owner": {"document": "99999999999999"},
             "beneficiary": {"document": "00123456789012"}},
            {"id": "c", "document": "00123456789012"},
        ])
        self.assertEqual(select_account_ids(cnpj="00123456789012", client_factory=factory), ["a"])

    def test_cnpj_with_explicit_ids_intersects_and_preserves_order(self):
        factory = factory_with(accounts=[
            {"id": "a", "owner": {"document": "00123456789012"}},
            {"id": "b", "owners": [{"documentNumber": "00123456789012"}]},
        ])
        self.assertEqual(select_account_ids(["b", "a"], "00.123.456/7890-12",
                                            client_factory=factory), ["b", "a"])

    def test_no_owner_document_match_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "No account"):
            select_account_ids(cnpj="00123456789012",
                               client_factory=factory_with(accounts=[{"id": "a"}]))

    def test_nested_owner_documents(self):
        factory = factory_with(accounts=[{"id": "a", "owner": {
            "documents": [{"type": "CNPJ", "number": "00123456789012"}]}}])
        self.assertEqual(select_account_ids(cnpj="00123456789012", client_factory=factory), ["a"])

    def test_invalid_filters_rejected_before_any_call(self):
        for kwargs in ({"account_ids": "a"}, {"account_ids": [""]}, {"cnpj": ""}, {"cnpj": "12"}):
            with self.assertRaises(ValueError):
                select_account_ids(**kwargs, client_factory=Mock(side_effect=AssertionError))

    def test_unfiltered_discovery_preserves_unique_api_order(self):
        self.assertEqual(select_account_ids(client_factory=factory_with(
            accounts=[{"id": "b"}, {"id": "a"}, {"id": "b"}]
        )), ["b", "a"])

    def test_local_date_and_run_isolation_are_deterministic(self):
        logical = datetime(2026, 1, 2, 1, 0, tzinfo=timezone.utc)
        path = run_directory("/tmp/test", logical, "run1")
        self.assertIn("date=2026-01-01", str(path))
        self.assertEqual(path, run_directory("/tmp/test", logical, "run1"))
        self.assertNotEqual(path, run_directory("/tmp/test", logical, "run2"))


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        environment = patch.dict("os.environ", {"DATANOMIK_DATA_ROOT": self.directory.name})
        environment.start()
        self.addCleanup(environment.stop)
        self.logical = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    def prepared(self, accounts=None):
        return prepare_run({"account_ids": accounts or ["a", "b"]}, self.logical, "test-run")

    def test_partial_extraction_is_not_deduplicated(self):
        selection = self.prepared()
        manifest = extract_account("a", selection["run_dir"], client_factory=factory_with())
        with self.assertRaisesRegex(ValueError, "Extraction incomplete"):
            transform_run(selection["run_dir"], [manifest])
        self.assertFalse((Path(selection["run_dir"]) / "data_quality.json").exists())

    def test_empty_account_selection_commits_zero_report(self):
        selection = prepare_run({}, self.logical, "empty", client_factory=factory_with())
        summary = transform_run(selection["run_dir"], [])
        self.assertEqual(read_json(summary["report_path"])["consolidado"]["quantidadeFinal"], 0)

    def test_corrupted_raw_file_is_rejected(self):
        selection = self.prepared(["a"])
        manifest = extract_account("a", selection["run_dir"], client_factory=factory_with())
        Path(read_json(manifest)["path"]).write_text("{}\n")
        with self.assertRaisesRegex(ValueError, "checksum"):
            transform_run(selection["run_dir"], [manifest])

    def test_raw_output_and_xcom_descriptor_are_small(self):
        selection = self.prepared(["a"])
        factory = factory_with(transactions=[{"external_id": "x", "account": {"id": "a"}}] * 150)
        manifest = extract_account("a", selection["run_dir"], client_factory=factory)
        self.assertIsInstance(manifest, str)
        self.assertEqual(read_json(manifest)["count"], 150)
        self.assertNotIn("secretPassword", Path(manifest).read_text())

    @unittest.skipUnless(HAS_ARROW, "PyArrow unavailable in host")
    def test_full_mocked_etl_replay_and_failed_publication(self):
        selection = self.prepared()
        a = {"external_id": "same", "account": {"id": "a", "number": "0000079-5"}, "amount": "4960.00"}
        b = {"external_id": "same", "account": {"id": "b"}, "amount": "10.00"}
        manifest_a = extract_account("a", selection["run_dir"],
                                     client_factory=factory_with(transactions=[a, a]))
        manifest_b = extract_account("b", selection["run_dir"],
                                     client_factory=factory_with(transactions=[b]))
        # Completion order must not change the order selected from /accounts.
        summary = transform_run(selection["run_dir"], [manifest_b, manifest_a])
        report = read_json(summary["report_path"])
        self.assertEqual(summary["quantidadeFinal"], 2)
        self.assertEqual(report["consolidado"]["valorDuplicadoRemovidoAbsoluto"], "4960.00")
        self.assertEqual(report["ordemContas"], ["a", "b"])
        self.assertEqual(summary, transform_run(selection["run_dir"], [manifest_a, manifest_b]))
        with patch("datanomik_pipeline.write_parquet", side_effect=RuntimeError("interruption")):
            with self.assertRaises(RuntimeError):
                transform_run(selection["run_dir"], [manifest_a, manifest_b])
        self.assertEqual(report, read_json(summary["report_path"]))
        self.assertEqual(list(Path(selection["run_dir"]).glob(".tmp-transform-*")), [])
