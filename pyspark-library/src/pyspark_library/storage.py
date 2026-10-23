import os
from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class StorageSettings:
    backend: str
    landing_bucket: str
    raw_bucket: str
    integration_bucket: str
    business_bucket: str
    endpoint: str = ""
    region: str = "us-east-1"
    access_key: str = ""
    secret_key: str = ""
    session_token: str = ""
    path_style_access: bool = False
    ssl_enabled: bool = True


def _read_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_storage_settings_from_env() -> StorageSettings:
    backend = os.getenv("STORAGE_BACKEND", "s3").strip().lower()

    if backend == "minio":
        return StorageSettings(
            backend="minio",
            endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            region=os.getenv("MINIO_REGION", "us-east-1"),
            access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            landing_bucket=os.getenv("MINIO_LANDING_BUCKET", "planejamento-landing"),
            raw_bucket=os.getenv("MINIO_RAW_BUCKET", "planejamento-raw"),
            integration_bucket=os.getenv("MINIO_INTEGRATION_BUCKET", "planejamento-integration"),
            business_bucket=os.getenv("MINIO_BUSINESS_BUCKET", "planejamento-business"),
            path_style_access=_read_bool(os.getenv("MINIO_PATH_STYLE_ACCESS"), True),
            ssl_enabled=_read_bool(os.getenv("MINIO_SSL_ENABLED"), False),
        )

    return StorageSettings(
        backend="s3",
        region=os.getenv("AWS_REGION", "sa-east-1"),
        access_key=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        session_token=os.getenv("AWS_SESSION_TOKEN", ""),
        landing_bucket=os.getenv("S3_LANDING_BUCKET", ""),
        raw_bucket=os.getenv("S3_RAW_BUCKET", ""),
        integration_bucket=os.getenv("S3_INTEGRATION_BUCKET", ""),
        business_bucket=os.getenv("S3_BUSINESS_BUCKET", ""),
        path_style_access=_read_bool(os.getenv("S3_PATH_STYLE_ACCESS"), False),
        ssl_enabled=_read_bool(os.getenv("S3_SSL_ENABLED"), True),
    )


def build_spark_s3a_conf(settings: StorageSettings) -> Dict[str, str]:
    hadoop_aws_package = os.getenv("SPARK_HADOOP_AWS_PACKAGE", "org.apache.hadoop:hadoop-aws:3.2.0")
    aws_bundle_package = os.getenv(
        "SPARK_AWS_JAVA_BUNDLE_PACKAGE",
        "com.amazonaws:aws-java-sdk-bundle:1.11.901",
    )
    extra_packages = [
        package.strip()
        for package in os.getenv("SPARK_EXTRA_JARS_PACKAGES", "").split(",")
        if package.strip()
    ]
    extra_jars = [
        jar.strip()
        for jar in os.getenv("SPARK_EXTRA_JARS", "").split(",")
        if jar.strip()
    ]
    jar_packages = [hadoop_aws_package, aws_bundle_package, *extra_packages]

    conf = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": str(settings.path_style_access).lower(),
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(settings.ssl_enabled).lower(),
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.endpoint.region": settings.region,
        "spark.jars.packages": ",".join(jar_packages),
    }

    if extra_jars:
        joined_jars = ",".join(extra_jars)
        joined_classpath = ":".join(extra_jars)
        conf["spark.jars"] = joined_jars
        conf["spark.driver.extraClassPath"] = joined_classpath
        conf["spark.executor.extraClassPath"] = joined_classpath

    if settings.endpoint:
        conf["spark.hadoop.fs.s3a.endpoint"] = settings.endpoint

    if settings.access_key:
        conf["spark.hadoop.fs.s3a.access.key"] = settings.access_key

    if settings.secret_key:
        conf["spark.hadoop.fs.s3a.secret.key"] = settings.secret_key

    if settings.session_token:
        conf["spark.hadoop.fs.s3a.session.token"] = settings.session_token

    if settings.backend == "s3" and not settings.access_key and not settings.secret_key:
        conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )

    return conf


def bucket_uri(bucket_name: str, prefix: str = "") -> str:
    if not prefix:
        return f"s3a://{bucket_name}"
    clean_prefix = prefix.lstrip("/")
    return f"s3a://{bucket_name}/{clean_prefix}"
