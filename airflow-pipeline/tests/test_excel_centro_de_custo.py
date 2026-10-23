import io
import ntpath
import tempfile
import unittest
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from cdl_planejamentocaixa.pipelines.parameters.ds_dl_ambipar_excel_centro_de_custo import transfer


ROOT = "1_landing/sourcedata/centro_de_custo/"


class MemoryContainer:
    def __init__(self, blobs=None, fail_on=None):
        self.blobs = dict(blobs or {})
        self.fail_on = fail_on
        self.deleted = []
        self.uploaded = []

    def list_blob_names(self, name_starts_with):
        return [name for name in self.blobs if name.startswith(name_starts_with)]

    def upload_blob(self, name, data, length, overwrite, content_settings):
        if name == self.fail_on:
            raise OSError("Upload interrompido")
        content = data.read()
        assert length == len(content)
        assert overwrite is True
        assert content_settings.content_type == transfer.EXCEL_CONTENT_TYPE
        self.blobs[name] = content
        self.uploaded.append(name)

    def delete_blob(self, name):
        del self.blobs[name]
        self.deleted.append(name)


class DailyCopyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def local_file(self, name, content):
        path = Path(self.directory.name) / name
        path.write_bytes(content)
        return path

    def test_same_day_replaces_files_and_removes_stale_files_only_in_that_day(self):
        current = ROOT + "20260908/"
        yesterday = ROOT + "20260907/Centro de Custo - A.xlsx"
        adjacent = ROOT + "202609080/Centro de Custo - A.xlsx"
        other_dataset = "1_landing/sourcedata/outro/20260908/dados.xlsx"
        container = MemoryContainer({
            current + "Centro de Custo - A.xlsx": b"anterior",
            current + "Centro de Custo - Removido.xlsx": b"obsoleto",
            yesterday: b"historico",
            adjacent: b"fora da pasta",
            other_dataset: b"outro conjunto",
        })
        local_files = [self.local_file("Centro de Custo - A.xlsx", b"atualizado")]
        result = transfer._replace_daily_files(container, local_files, "20260908")
        self.assertEqual(container.blobs, {
            current + "Centro de Custo - A.xlsx": b"atualizado",
            yesterday: b"historico",
            adjacent: b"fora da pasta",
            other_dataset: b"outro conjunto",
        })
        self.assertEqual(result["files_copied"], 1)
        self.assertEqual(result["obsolete_files_removed"], 1)
        self.assertEqual(result["bytes_copied"], len(b"atualizado"))
        self.assertEqual(result["target_path"], "ambipar/" + current)

        # A second execution leaves one current copy in the same dated folder.
        local_files[0].write_bytes(b"segunda execucao")
        transfer._replace_daily_files(container, local_files, "20260908")
        self.assertEqual(container.blobs[current + local_files[0].name], b"segunda execucao")
        self.assertEqual(len([name for name in container.blobs if name.startswith(current)]), 1)

    def test_next_day_preserves_previous_day(self):
        container = MemoryContainer()
        path = self.local_file("Centro de Custo - A.xlsx", b"dia um")
        transfer._replace_daily_files(container, [path], "20260908")
        path.write_bytes(b"dia dois")
        transfer._replace_daily_files(container, [path], "20260909")
        self.assertEqual(container.blobs[ROOT + "20260908/" + path.name], b"dia um")
        self.assertEqual(container.blobs[ROOT + "20260909/" + path.name], b"dia dois")
        self.assertEqual(container.deleted, [])

    def test_upload_failure_does_not_delete_stale_files(self):
        prefix = ROOT + "20260908/"
        obsolete = prefix + "Centro de Custo - Obsoleto.xlsx"
        container = MemoryContainer({obsolete: b"anterior"}, fail_on=prefix + "Centro de Custo - B.xlsx")
        files = [self.local_file("Centro de Custo - A.xlsx", b"A"),
                 self.local_file("Centro de Custo - B.xlsx", b"B")]
        with self.assertRaises(OSError):
            transfer._replace_daily_files(container, files, "20260908")
        self.assertEqual(container.blobs[obsolete], b"anterior")
        self.assertEqual(container.deleted, [])
        container.fail_on = None
        transfer._replace_daily_files(container, files, "20260908")
        self.assertEqual(set(container.blobs), {prefix + path.name for path in files})

    def test_empty_source_does_not_touch_destination(self):
        container = Mock()
        with self.assertRaises(ValueError):
            transfer._replace_daily_files(container, [], "20260908")
        self.assertEqual(container.mock_calls, [])

    def test_invalid_dates_cannot_widen_deletion_scope(self):
        path = self.local_file("Centro de Custo - A.xlsx", b"A")
        for day in ("", "202609", "20260230", "../", "20260908/..", "２０２６０９０８"):
            with self.subTest(day=day):
                container = Mock()
                with self.assertRaises(ValueError):
                    transfer._replace_daily_files(container, [path], day)
                self.assertEqual(container.mock_calls, [])

    def test_copy_uses_current_utc_day_and_cleans_local_files(self):
        class FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                if tz is not timezone.utc:
                    raise AssertionError("Expected UTC")
                # Still the previous day in Sao Paulo.
                return cls(2026, 9, 9, 0, 30, tzinfo=tz)

        downloaded = []

        def download(directory):
            path = Path(directory) / "Centro de Custo - A.xlsx"
            path.write_bytes(b"excel original")
            downloaded.append(path)
            return [path]

        container = MemoryContainer()
        with patch.object(transfer, "datetime", FrozenDatetime), \
                patch.object(transfer, "_download_source_files", side_effect=download), \
                patch.object(transfer, "_azure_container", return_value=nullcontext(container)):
            result = transfer.copy_excel_files()
        self.assertEqual(result["execution_date_utc"], "20260909")
        self.assertEqual(container.blobs[ROOT + "20260909/" + downloaded[0].name], b"excel original")
        self.assertFalse(downloaded[0].parent.exists())

    def test_source_failure_does_not_open_azure_destination(self):
        with patch.object(transfer, "_download_source_files", side_effect=OSError("SMB indisponivel")), \
                patch.object(transfer, "_azure_container") as azure:
            with self.assertRaises(OSError):
                transfer.copy_excel_files()
            azure.assert_not_called()

    def test_smb_filters_files_and_preserves_original_bytes(self):
        connection = SimpleNamespace(host="172.19.1.9", login="test", password="test", port=445)
        names = [("Centro de Custo - Brasil.xlsx", True),
                 ("centro de custo - Exterior.XLSX", True),
                 ("Centro de Custo - Pasta.xlsx", False),
                 ("~$Centro de Custo - Brasil.xlsx", True),
                 ("Outra planilha.xlsx", True)]
        entries = [SimpleNamespace(name=name, is_file=Mock(return_value=is_file))
                   for name, is_file in names]
        original = b"PK\x03\x04\x00\xffExcel bytes"
        with patch("airflow.models.connection.Connection.get_connection_from_secrets", return_value=connection), \
                patch("smbclient.register_session"), \
                patch("smbclient.scandir", return_value=iter(entries)), \
                patch("smbclient.open_file", side_effect=lambda *a, **kw: io.BytesIO(original)) as open_file, \
                patch("smbclient.reset_connection_cache") as reset:
            files = transfer._download_source_files(self.directory.name)
        self.assertEqual({path.name for path in files}, {names[0][0], names[1][0]})
        self.assertTrue(all(path.read_bytes() == original for path in files))
        self.assertEqual(open_file.call_count, 2)
        expected_directory = ntpath.join("\\\\172.19.1.9", transfer.SOURCE_DIRECTORY)
        self.assertTrue(all(ntpath.dirname(call.args[0]) == expected_directory
                            for call in open_file.call_args_list))
        reset.assert_called_once()

    def test_no_matching_smb_files_fails_and_closes_session(self):
        connection = SimpleNamespace(host="172.19.1.9", login="test", password="test", port=445)
        with patch("airflow.models.connection.Connection.get_connection_from_secrets", return_value=connection), \
                patch("smbclient.register_session"), \
                patch("smbclient.scandir", return_value=iter([])), \
                patch("smbclient.reset_connection_cache") as reset:
            with self.assertRaises(FileNotFoundError):
                transfer._download_source_files(self.directory.name)
        reset.assert_called_once()


if __name__ == "__main__":
    unittest.main()
