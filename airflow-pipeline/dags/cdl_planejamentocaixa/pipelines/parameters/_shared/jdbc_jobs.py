from airflow.models.connection import Connection
from cdl_planejamentocaixa.pipelines.parameters._shared.jdbc_databases import DATABASES


SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
DEFAULT_CONN_ID = "35"
DEFAULT_FETCH_SIZE = "10000"


def _sqlserver_url(connection, database_name):
    return (
        f"jdbc:sqlserver://{connection.host}:{connection.port};"
        f"databaseName={database_name};"
        "encrypt=false;trustServerCertificate=true;sslProtocol=TLSv1.2"
    )


def resolve_database(database_key, conn_id=None, database_name=None):
    if database_key is not None:
        database_config = DATABASES[database_key]
        return {
            "conn_id": database_config["conn_id"],
            "database_name": database_config["database_name"],
        }
    if database_name is None:
        raise ValueError("database_name is required when database_key is not provided")
    return {
        "conn_id": conn_id or DEFAULT_CONN_ID,
        "database_name": database_name,
    }


def jdbc_input(
    *,
    database_key=None,
    database_name=None,
    dbtable,
    conn_id=None,
    input_alias="df_input",
    fetch_size=DEFAULT_FETCH_SIZE,
):
    database_config = resolve_database(
        database_key=database_key,
        conn_id=conn_id,
        database_name=database_name,
    )
    connection = Connection.get_connection_from_secrets(database_config["conn_id"])
    return {
        "InputType": "Full",
        "InputAlias": input_alias,
        "ReadFormat": "jdbc",
        "url": _sqlserver_url(connection, database_config["database_name"]),
        "conn_id": database_config["conn_id"],
        "database_name": database_config["database_name"],
        "driver": SQLSERVER_DRIVER,
        "dbtable": dbtable,
        "ReadOptions": [
            {
                "OptionName": "fetchsize",
                "OptionValue": str(fetch_size),
            }
        ],
    }


def build_landing_job(
    *,
    source_system,
    dataset_name,
    database_key=None,
    database_name=None,
    dbtable,
    conn_id=None,
    input_alias="df_input",
):
    return {
        "DatasetName": dataset_name,
        "SourceSystem": source_system,
        "Stages": [
            {
                "Input": [
                    jdbc_input(
                        database_key=database_key,
                        database_name=database_name,
                        dbtable=dbtable,
                        conn_id=conn_id,
                        input_alias=input_alias,
                    )
                ],
                "ExternalFunction": {
                    "Module": "pyspark_library.dummy",
                    "Parameters": [],
                },
                "Output": {
                    "Coalesce": 1,
                },
            }
        ],
    }
