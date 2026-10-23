"""Copy Excel files from the Ambipar SMB share into daily Parquet datasets."""

import fnmatch
import json
import logging
import ntpath
import os
import re
import shutil
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory


LOGGER = logging.getLogger(__name__)
SMB_CONN_ID = "ambipar_excel_centro_de_custo_smb"
TARGET_CONTAINER = "ambipar"
TARGET_ROOT = "datalake/delta/raw/sourcedata"
SUPPORTED_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}
_RANGE = re.compile(r"^([A-Z]+)([1-9][0-9]*):([A-Z]+)([1-9][0-9]*)$")
_FOLDER = re.compile(r"^[a-z0-9_]+$")


class ExcelLayoutMismatch(ValueError):
    """Raised when an Excel file does not contain the columns in its spec."""


def load_specs():
    with Path(__file__).with_name("specs.json").open(encoding="utf-8") as stream:
        return json.load(stream)


def _validate_day(day):
    if not isinstance(day, str) or len(day) != 8 or not day.isascii() or not day.isdigit():
        raise ValueError("A particao deve usar o formato AAAAMMDD.")
    datetime.strptime(day, "%Y%m%d")


def _is_excel(name):
    return ntpath.splitext(name)[1].lower() in SUPPORTED_EXTENSIONS and not name.startswith("~$")


def _iter_matching_files(smbclient, directory, pattern, recursive, options):
    for entry in smbclient.scandir(directory, **options):
        name = entry.name
        if name in (".", "..") or name != ntpath.basename(name):
            continue
        remote_path = ntpath.join(directory, name)
        if entry.is_file() and _is_excel(name) and fnmatch.fnmatchcase(name.casefold(), pattern.casefold()):
            yield remote_path
        elif recursive and entry.is_dir():
            yield from _iter_matching_files(smbclient, remote_path, pattern, recursive, options)


def _download_files(spec, local_directory):
    import smbclient
    from airflow.models.connection import Connection

    connection = Connection.get_connection_from_secrets(SMB_CONN_ID)
    if not all((connection.host, connection.login, connection.password)):
        raise ValueError("Configure host, login e senha na conexao {}.".format(SMB_CONN_ID))
    root = ntpath.join(r"\\{}".format(connection.host), spec["source_folder"].replace("/", "\\"))
    pattern = spec["source_file"]
    cache = {}
    options = {"connection_cache": cache, "port": connection.port or 445}
    try:
        smbclient.register_session(
            connection.host,
            username=connection.login,
            password=connection.password,
            connection_timeout=30,
            **options
        )
        source_files = spec.get("source_files")
        if source_files:
            if any(not _is_excel(name) or name != ntpath.basename(name) for name in source_files):
                raise ValueError("Lista de arquivos Excel invalida para {}.".format(spec["task_id"]))
            remote_paths = [ntpath.join(root, name) for name in source_files]
        elif any(char in pattern for char in "*?["):
            remote_paths = sorted(set(_iter_matching_files(
                smbclient, root, pattern, spec["recursive"], options
            )))
        else:
            if not _is_excel(pattern) or pattern != ntpath.basename(pattern):
                raise ValueError("Nome de arquivo Excel invalido: {}".format(pattern))
            remote_paths = [ntpath.join(root, pattern)]
        if not remote_paths:
            if spec["skip_if_empty"]:
                LOGGER.info("Nenhum arquivo %s em %s; carga ignorada.", pattern, root)
                return []
            raise FileNotFoundError("Nenhum arquivo {} encontrado em {}".format(pattern, root))

        files = []
        for index, remote_path in enumerate(remote_paths):
            suffix = ntpath.splitext(remote_path)[1].lower()
            local_path = Path(local_directory) / ("source_{:04d}{}".format(index, suffix))
            with smbclient.open_file(
                remote_path, mode="rb", share_access=spec.get("share_access", "rw"), **options
            ) as source:
                with local_path.open("wb") as destination:
                    shutil.copyfileobj(source, destination, length=1024 * 1024)
            relative = ntpath.relpath(remote_path, root)
            if relative == ".." or relative.startswith("..\\"):
                raise ValueError("Arquivo fora da pasta SMB configurada.")
            files.append({"local": local_path, "remote": remote_path, "relative": relative})
            LOGGER.info("Arquivo recebido: %s (%d bytes)", remote_path, local_path.stat().st_size)
        return files
    finally:
        smbclient.reset_connection_cache(connection_cache=cache)


def _read_excel(file, spec):
    import pandas as pd

    kwargs = {
        "sheet_name": spec["sheet"],
        "header": 0 if spec["header"] else None,
        "dtype": str,
        "keep_default_na": False,
        "na_filter": False,
        "engine": "xlrd" if file["local"].suffix == ".xls" else "openpyxl",
    }
    if spec["range"]:
        match = _RANGE.fullmatch(spec["range"])
        if not match:
            raise ValueError("Intervalo Excel invalido: {}".format(spec["range"]))
        start_row, end_row = int(match.group(2)), int(match.group(4))
        if end_row < start_row:
            raise ValueError("Intervalo Excel invertido: {}".format(spec["range"]))
        kwargs["usecols"] = "{}:{}".format(match.group(1), match.group(3))
        kwargs["skiprows"] = start_row - 1
        kwargs["nrows"] = end_row - start_row + (0 if spec["header"] else 1)

    frame = pd.read_excel(str(file["local"]), **kwargs)
    # Remove blank Excel rows before adding constant provenance columns.
    frame = frame.where(frame.ne(""), None).dropna(how="all")
    for name, value in spec["extra_columns"]:
        frame[name] = file["remote"] if value == "$$FILEPATH" else value

    mappings = spec["mappings"]
    if mappings:
        projected = pd.DataFrame(index=frame.index)
        for source, target in mappings:
            if isinstance(source, int):
                if source < 1 or source > len(frame.columns):
                    raise ExcelLayoutMismatch("Coluna ordinal {} ausente em {}".format(source, file["remote"]))
                projected[target] = frame.iloc[:, source - 1]
            else:
                if source not in frame.columns:
                    raise ExcelLayoutMismatch("Coluna {!r} ausente em {}".format(source, file["remote"]))
                projected[target] = frame[source]
        frame = projected

    frame = frame.where(frame.ne(""), None).dropna(how="all")
    if not frame.empty:
        frame = frame.astype(object)
        for column in frame.columns:
            frame[column] = frame[column].map(lambda value: None if pd.isna(value) else str(value))
    return frame


def _build_parquet(files, spec, local_directory):
    import pyarrow as pa
    import pyarrow.parquet as pq

    outputs = []
    if spec.get("preserve_hierarchy"):
        for index, file in enumerate(files):
            try:
                frame = _read_excel(file, spec)
            except ExcelLayoutMismatch as exc:
                if not spec.get("skip_layout_mismatch"):
                    raise
                LOGGER.warning("Arquivo ignorado por divergencia de layout: %s", exc)
                continue
            if frame.empty:
                continue
            relative = file["relative"].replace("\\", "/")
            relative = relative.rsplit(".", 1)[0] + ".parquet"
            path = Path(local_directory) / ("output_{:04d}.parquet".format(index))
            schema = pa.schema([(name, pa.string()) for name in frame.columns])
            pq.write_table(pa.Table.from_pandas(frame, schema=schema, preserve_index=False), path, compression="snappy")
            outputs.append({"local": path, "relative": relative, "rows": len(frame)})
    else:
        path = Path(local_directory) / (spec["target_folder"] + ".parquet")
        writer = None
        count = 0
        try:
            for file in files:
                try:
                    frame = _read_excel(file, spec)
                except ExcelLayoutMismatch as exc:
                    if not spec.get("skip_layout_mismatch"):
                        raise
                    LOGGER.warning("Arquivo ignorado por divergencia de layout: %s", exc)
                    continue
                if frame.empty:
                    continue
                schema = pa.schema([(name, pa.string()) for name in frame.columns])
                table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(path, schema, compression="snappy")
                elif schema != writer.schema:
                    raise ValueError("Colunas divergentes entre arquivos Excel de {}".format(spec["task_id"]))
                writer.write_table(table)
                count += len(frame)
        finally:
            if writer is not None:
                writer.close()
        if count:
            outputs.append({"local": path, "relative": path.name, "rows": count})
    if not outputs:
        raise ValueError("Nenhuma linha encontrada em {}. Destino preservado.".format(spec["task_id"]))
    return outputs


@contextmanager
def _azure_container():
    from azure.identity import ClientSecretCredential
    from azure.storage.blob import BlobServiceClient

    required = ("AZURE_ADLS_ACCOUNT_NAME", "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise ValueError("Variaveis Azure ausentes: {}".format(", ".join(missing)))
    with ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    ) as credential:
        with BlobServiceClient(
            account_url="https://{}.blob.core.windows.net".format(os.environ["AZURE_ADLS_ACCOUNT_NAME"]),
            credential=credential,
        ) as service:
            yield service.get_container_client(TARGET_CONTAINER)


def _replace_daily_partition(container, outputs, target_folder, day):
    from azure.storage.blob import ContentSettings

    _validate_day(day)
    if not _FOLDER.fullmatch(target_folder):
        raise ValueError("Pasta de destino invalida: {}".format(target_folder))
    if not outputs:
        raise ValueError("Nenhum Parquet preparado. Destino preservado.")
    prefix = "{}/{}/{}/".format(TARGET_ROOT, target_folder, day)
    names = []
    for output in outputs:
        relative = output["relative"].replace("\\", "/")
        if relative.startswith("/") or ".." in relative.split("/") or not relative.endswith(".parquet"):
            raise ValueError("Nome de Parquet invalido: {}".format(relative))
        names.append(prefix + relative)
    if len(names) != len(set(names)):
        raise ValueError("Arquivos Parquet duplicados na pasta diaria.")

    previous = list(container.list_blob_names(name_starts_with=prefix))
    for name in sorted(previous, key=lambda value: (value.count("/"), value), reverse=True):
        if not name.startswith(prefix):
            raise ValueError("Recusada exclusao fora da pasta diaria.")
        container.delete_blob(name)
    uploaded = []
    for output, name in zip(outputs, names):
        with output["local"].open("rb") as content:
            container.upload_blob(
                name=name,
                data=content,
                length=output["local"].stat().st_size,
                overwrite=True,
                content_settings=ContentSettings(content_type="application/octet-stream"),
            )
        uploaded.append(name)
    return {"target_path": "{}/{}".format(TARGET_CONTAINER, prefix),
            "execution_date_utc": day, "files_written": uploaded,
            "rows_copied": sum(output["rows"] for output in outputs),
            "old_files_removed": len(previous)}


def run_copy(dag_id, task_id):
    spec = next(copy for copy in load_specs()[dag_id] if copy["task_id"] == task_id)
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    with TemporaryDirectory(prefix="ambipar_excel_") as directory:
        files = _download_files(spec, directory)
        if not files:
            return {"status": "skipped", "reason": "no_matching_files", "task_id": task_id}
        outputs = _build_parquet(files, spec, directory)
        with _azure_container() as container:
            result = _replace_daily_partition(container, outputs, spec["target_folder"], day)
    LOGGER.info("Carga Excel concluida: %s", result)
    return result
