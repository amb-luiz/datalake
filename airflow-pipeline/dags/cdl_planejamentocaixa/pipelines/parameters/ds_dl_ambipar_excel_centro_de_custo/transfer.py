import fnmatch
import logging
import ntpath
import os
import shutil
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from cdl_planejamentocaixa.pipelines.parameters.ds_dl_ambipar_excel_centro_de_custo.common import (
    FILE_PATTERN,
    SMB_CONN_ID,
    SOURCE_DIRECTORY,
    TARGET_CONTAINER,
    TARGET_DIRECTORY,
)


LOGGER = logging.getLogger(__name__)
EXCEL_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
PARQUET_CONTENT_TYPE = "application/octet-stream"
PARQUET_FILE_NAME = "centro_de_custo.parquet"
DATA_COLUMNS = ["UNID.NEG.", "Cód. C.C.", "Descrição", "Classe"]


def _download_source_files(local_directory):
    import smbclient
    from airflow.models.connection import Connection

    connection = Connection.get_connection_from_secrets(SMB_CONN_ID)
    if not all((connection.host, connection.login, connection.password)):
        raise ValueError("Configure host, login e senha na conexao {}.".format(SMB_CONN_ID))

    server = connection.host
    directory = ntpath.join(r"\\{}".format(server), SOURCE_DIRECTORY)
    # Keep this task's SMB sessions separate from other tasks in the worker.
    cache = {}
    options = {"connection_cache": cache, "port": connection.port or 445}
    try:
        smbclient.register_session(
            server,
            username=connection.login,
            password=connection.password,
            connection_timeout=30,
            **options
        )
        names = sorted(
            entry.name
            for entry in smbclient.scandir(directory, **options)
            if fnmatch.fnmatchcase(entry.name.casefold(), FILE_PATTERN.casefold())
            and entry.is_file()
        )
        if not names:
            raise FileNotFoundError(
                "Nenhum arquivo {} encontrado em {}. Destino preservado.".format(
                    FILE_PATTERN, directory
                )
            )

        local_files = []
        for name in names:
            if name != ntpath.basename(name) or "/" in name:
                raise ValueError("Nome de arquivo invalido na origem SMB.")
            local_path = Path(local_directory) / name
            with smbclient.open_file(
                ntpath.join(directory, name), mode="rb", share_access="rw", **options
            ) as source:
                with local_path.open("wb") as destination:
                    shutil.copyfileobj(source, destination, length=1024 * 1024)
            local_files.append(local_path)
            LOGGER.info("Arquivo recebido: %s (%d bytes)", name, local_path.stat().st_size)
        return local_files
    finally:
        smbclient.reset_connection_cache(connection_cache=cache)


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


def _build_parquet(local_files):
    import pandas as pd

    frames = []
    for local_path in local_files:
        frame = pd.read_excel(local_path, engine="openpyxl", dtype=str, usecols=lambda column: column in DATA_COLUMNS)
        for column in DATA_COLUMNS:
            if column not in frame.columns:
                frame[column] = None
        frame = frame[DATA_COLUMNS].dropna(how="all")
        frame["arquivo_origem"] = local_path.name
        frames.append(frame)
    if not frames:
        raise ValueError("Nenhum dado encontrado nos arquivos Excel.")
    combined = pd.concat(frames, ignore_index=True)
    combined[DATA_COLUMNS] = combined[DATA_COLUMNS].fillna("")
    parquet_path = Path(local_files[0].parent) / PARQUET_FILE_NAME
    combined.to_parquet(parquet_path, engine="pyarrow", index=False)
    return parquet_path, len(combined)


def _replace_daily_files(container, local_files, day):
    from azure.storage.blob import ContentSettings

    if len(day) != 8 or not day.isascii() or not day.isdigit():
        raise ValueError("A particao deve usar o formato AAAAMMDD.")
    datetime.strptime(day, "%Y%m%d")
    if not local_files:
        raise ValueError("Lista de arquivos vazia. Destino preservado.")

    prefix = "{}/{}/".format(TARGET_DIRECTORY, day)
    previous_names = set(container.list_blob_names(name_starts_with=prefix))
    parquet_path, row_count = _build_parquet(local_files)
    target_name = prefix + PARQUET_FILE_NAME
    size = parquet_path.stat().st_size
    with parquet_path.open("rb") as content:
        container.upload_blob(
            name=target_name,
            data=content,
            length=size,
            overwrite=True,
            content_settings=ContentSettings(content_type=PARQUET_CONTENT_TYPE),
        )
    obsolete = previous_names - {target_name}
    for name in sorted(obsolete, key=lambda value: (value.count("/"), value), reverse=True):
        if not name.startswith(prefix):
            raise ValueError("Recusada exclusao fora da pasta diaria.")
        container.delete_blob(name)
        LOGGER.info("Arquivo obsoleto removido: %s/%s", TARGET_CONTAINER, name)

    return {
        "target_path": "{}/{}".format(TARGET_CONTAINER, prefix),
        "execution_date_utc": day,
        "files_copied": len(local_files),
        "rows_copied": row_count,
        "bytes_copied": size,
        "parquet_file": target_name,
        "obsolete_files_removed": len(obsolete),
    }


def copy_excel_files():
    # utcnow(), rather than ds_nodash: a manual run may have an old logical date.
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    with TemporaryDirectory(prefix="ambipar_centro_de_custo_") as local_directory:
        local_files = _download_source_files(local_directory)
        with _azure_container() as container:
            result = _replace_daily_files(container, local_files, day)
    LOGGER.info("Copia de centro de custo concluida: %s", result)
    return result
