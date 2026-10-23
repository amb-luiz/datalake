from pyspark_library.storage import build_spark_s3a_conf, load_storage_settings_from_env


def get_storage_conf():
    settings = load_storage_settings_from_env()
    return build_spark_s3a_conf(settings)
