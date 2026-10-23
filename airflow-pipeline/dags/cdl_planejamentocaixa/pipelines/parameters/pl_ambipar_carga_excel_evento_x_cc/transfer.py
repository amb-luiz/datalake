import logging
import ntpath
import os
import shutil
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from cdl_planejamentocaixa.pipelines.parameters.pl_ambipar_carga_excel_evento_x_cc.common import (
    DATA_COLUMNS,
    PARQUET_FILE_NAME,
    SHEET_NAME,
    SMB_CONN_ID,
    SOURCE_DIRECTORY,
    SOURCE_FILE_NAME,
    TARGET_CONTAINER,
    TARGET_DIRECTORY,
)


LOGGER = logging.getLogger(__name__)


def _download_source_file(local_directory):
    import smbclient
    from airflow.models.connection import Connection

    connection = Connection.get_connection_from_secrets(SMB_CONN_ID)
    if not all((connection.host, connection.login, connection.password)):
        raise ValueError("Configure host, login e senha na conexao {}.".format(SMB_CONN_ID))

    source = ntpath.join(r"\\{}".format(connection.host), SOURCE_DIRECTORY, SOURCE_FILE_NAME)
    local_path = Path(local_directory) / SOURCE_FILE_NAME
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
        with smbclient.open_file(source, mode="rb", share_access="rw", **options) as remote:
            with local_path.open("wb") as local:
                shutil.copyfileobj(remote, local, length=1024 * 1024)
        LOGGER.info("Excel recebido: %s (%d bytes)", SOURCE_FILE_NAME, local_path.stat().st_size)
        return local_path
    finally:
        smbclient.reset_connection_cache(connection_cache=cache)


def _build_parquet(local_path):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    frame = pd.read_excel(
        local_path,
        sheet_name=SHEET_NAME,
        header=0,
        engine="openpyxl",
        dtype=str,
        keep_default_na=False,
    )
    missing = [column for column in DATA_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("Colunas obrigatorias ausentes em {}: {}".format(SHEET_NAME, ", ".join(missing)))

    frame = frame[DATA_COLUMNS]
    # Keep identifiers as strings, including leading zeros, and preserve literal NA/NULL values.
    frame = frame.where(frame.ne(""), None).dropna(how="all")
    if frame.empty:
        raise ValueError("A aba {} nao possui dados. Destino preservado.".format(SHEET_NAME))
    schema = pa.schema([(column, pa.string()) for column in DATA_COLUMNS])
    table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)
    parquet_path = Path(local_path).parent / PARQUET_FILE_NAME
    pq.write_table(table, parquet_path, compression="snappy")
    return parquet_path, table.num_rows


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


def _daily_prefix(day):
    if not isinstance(day, str) or len(day) != 8 or not day.isascii() or not day.isdigit():
        raise ValueError("A particao deve usar o formato yyyyMMdd.")
    datetime.strptime(day, "%Y%m%d")
    return "{}/{}/".format(TARGET_DIRECTORY, day)


def _delete_daily_partition(container, prefix):
    previous_names = list(container.list_blob_names(name_starts_with=prefix))
    if any(not name.startswith(prefix) for name in previous_names):
        raise ValueError("Recusada exclusao fora da pasta diaria.")
    # Children first also supports directory entries in accounts with hierarchical namespaces.
    for name in sorted(previous_names, key=lambda value: (value.count("/"), value), reverse=True):
        container.delete_blob(name)
    LOGGER.info("Delete concluido: %s/%s (%d itens)", TARGET_CONTAINER, prefix, len(previous_names))
    return len(previous_names)


def _replace_daily_parquet(container, parquet_path, row_count, day):
    from azure.storage.blob import ContentSettings

    prefix = _daily_prefix(day)
    size = parquet_path.stat().st_size
    target_name = prefix + PARQUET_FILE_NAME
    # Open the prepared file before Delete; a deletion error propagates and prevents Copy.
    with parquet_path.open("rb") as content:
        deleted = _delete_daily_partition(container, prefix)
        container.upload_blob(
            name=target_name,
            data=content,
            length=size,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/octet-stream"),
        )
    return {
        "target_path": "{}/{}".format(TARGET_CONTAINER, prefix),
        "execution_date_utc": day,
        "parquet_file": target_name,
        "rows_copied": row_count,
        "files_copied": 1,
        "bytes_copied": size,
        "deleted_blobs": deleted,
    }


def copy_excel_to_parquet():
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    # One task keeps the validated snapshot on the same worker throughout Delete and Copy.
    with TemporaryDirectory(prefix="ambipar_evento_x_cc_") as local_directory:
        local_path = _download_source_file(local_directory)
        parquet_path, row_count = _build_parquet(local_path)
        with _azure_container() as container:
            result = _replace_daily_parquet(container, parquet_path, row_count, day)
    LOGGER.info("Carga de evento por conta contabil concluida: %s", result)
    return result
