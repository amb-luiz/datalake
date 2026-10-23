import os
from datetime import datetime

class AzureConfigurationError(RuntimeError):
    pass


def _require_env(var_name):
    value = os.getenv(var_name)
    if not value:
        raise AzureConfigurationError(f"Missing required environment variable: {var_name}")
    return value


def _import_azure_dependencies():
    try:
        from azure.identity import ClientSecretCredential
        from azure.keyvault.secrets import SecretClient
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError as exc:
        raise AzureConfigurationError(
            "Azure SDK dependencies are not installed in the Airflow environment. "
            "Rebuild the Airflow image after updating airflow/requirements.txt."
        ) from exc

    return ClientSecretCredential, SecretClient, DataLakeServiceClient


def get_key_vault_secret():
    ClientSecretCredential, SecretClient, _ = _import_azure_dependencies()
    tenant_id = _require_env("AZURE_TENANT_ID")
    client_id = _require_env("AZURE_CLIENT_ID")
    client_secret = _require_env("AZURE_CLIENT_SECRET")
    vault_url = _require_env("AZURE_KEY_VAULT_URL")
    secret_name = _require_env("AZURE_ADLS_SECRET_NAME")

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )
    client = SecretClient(vault_url=vault_url, credential=credential)
    return client.get_secret(secret_name).value


def get_storage_options():
    account_name = _require_env("AZURE_ADLS_ACCOUNT_NAME")
    secret_type = os.getenv("AZURE_ADLS_SECRET_TYPE", "account_key").strip().lower()
    secret_value = get_key_vault_secret()

    options = {"account_name": account_name, "secret_type": secret_type}
    if secret_type == "account_key":
        options["account_key"] = secret_value
        return options
    if secret_type == "connection_string":
        options["connection_string"] = secret_value
        return options
    if secret_type == "sas_token":
        options["sas_token"] = secret_value
        return options
    raise AzureConfigurationError(f"Unsupported AZURE_ADLS_SECRET_TYPE: {secret_type}")


def build_bronze_path(dataset_name, run_date=None):
    filesystem = os.getenv("AZURE_ADLS_FILESYSTEM", "datalake").strip("/")
    bronze_root = os.getenv("AZURE_ADLS_BRONZE_ROOT", "bronze/SBO").strip("/")
    partition = (run_date or datetime.utcnow()).strftime("%Y%m%d")
    return f"{filesystem}/{bronze_root}/{dataset_name}/{partition}"


def build_abfss_path(dataset_name, run_date=None):
    account_name = _require_env("AZURE_ADLS_ACCOUNT_NAME")
    filesystem = os.getenv("AZURE_ADLS_FILESYSTEM", "datalake").strip("/")
    bronze_root = os.getenv("AZURE_ADLS_BRONZE_ROOT", "bronze/SBO").strip("/")
    partition = (run_date or datetime.utcnow()).strftime("%Y%m%d")
    return (
        f"abfss://{filesystem}@{account_name}.dfs.core.windows.net/"
        f"{bronze_root}/{dataset_name}/{partition}"
    )


def get_datalake_service_client():
    _, _, DataLakeServiceClient = _import_azure_dependencies()
    account_name = _require_env("AZURE_ADLS_ACCOUNT_NAME")
    storage_options = get_storage_options()

    if storage_options["secret_type"] == "connection_string":
        return DataLakeServiceClient.from_connection_string(storage_options["connection_string"])

    account_url = f"https://{account_name}.dfs.core.windows.net"
    if storage_options["secret_type"] == "account_key":
        credential = storage_options["account_key"]
    else:
        credential = storage_options["sas_token"]
    return DataLakeServiceClient(account_url=account_url, credential=credential)
