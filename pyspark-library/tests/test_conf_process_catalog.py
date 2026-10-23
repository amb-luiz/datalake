import importlib.util
import pathlib
import sys
import types
import unittest


CONF_PROCESS_PATH = pathlib.Path(
    "/opt/apps/datalake/pyspark-library/src/spark_pyutil/conf_process.py"
)


def load_conf_process_module():
    utility_module = types.ModuleType("spark_pyutil.utility")
    utility_module.exists_files = lambda path: True
    utility_module.get_and_validate_json = lambda path, schema: {}
    utility_module.is_dir = lambda prefix, key: False
    utility_module.delete_path = lambda path: None

    glue_module = types.ModuleType("spark_pyutil.glue_connection")

    class DummyGlueJDBCConection:
        pass

    glue_module.GlueJDBCConection = DummyGlueJDBCConection

    package_module = types.ModuleType("spark_pyutil")
    package_module.__path__ = []

    sys.modules["spark_pyutil"] = package_module
    sys.modules["spark_pyutil.utility"] = utility_module
    sys.modules["spark_pyutil.glue_connection"] = glue_module

    spec = importlib.util.spec_from_file_location(
        "spark_pyutil.conf_process",
        str(CONF_PROCESS_PATH),
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["spark_pyutil.conf_process"] = module
    spec.loader.exec_module(module)
    return module


class FakeDataType:
    def __init__(self, type_name):
        self._type_name = type_name

    def simpleString(self):
        return self._type_name


class FakeField:
    def __init__(self, name, type_name):
        self.name = name
        self.dataType = FakeDataType(type_name)


class FakeSchema:
    def __init__(self, fields):
        self.fields = fields


class FakeDataFrame:
    def __init__(self, fields):
        self.schema = FakeSchema(fields)


class FakeCatalog:
    def __init__(self, table_exists):
        self._table_exists = table_exists
        self.refreshed_tables = []

    def tableExists(self, table_name):
        return self._table_exists

    def refreshTable(self, table_name):
        self.refreshed_tables.append(table_name)


class FakeSpark:
    def __init__(self, table_exists, existing_schema=None):
        self.catalog = FakeCatalog(table_exists)
        self._existing_schema = existing_schema
        self.sql_calls = []

    def sql(self, statement):
        self.sql_calls.append(" ".join(statement.split()))

    def table(self, table_name):
        return types.SimpleNamespace(schema=self._existing_schema)


class ConfProcessCatalogSyncTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conf_process_module = load_conf_process_module()

    def build_conf_process(self, output):
        conf_process = self.conf_process_module.ConfProcess.__new__(
            self.conf_process_module.ConfProcess
        )
        conf_process._ConfProcess__job = {"Output": output}
        return conf_process

    def test_creates_hive_compatible_partitioned_table(self):
        conf_process = self.build_conf_process(
            {
                "OutputDatabase": "cdl_planejamentocaixa_integration",
                "OutputTable": "an_test",
                "OutputPath": "s3a://integration/analitico/teste",
            }
        )
        spark = FakeSpark(table_exists=False)
        df_input = FakeDataFrame(
            [
                FakeField("id", "bigint"),
                FakeField("amount", "double"),
                FakeField("execution_ts", "string"),
            ]
        )

        conf_process._ConfProcess__sync_catalog_table(spark, df_input, ["execution_ts"])

        create_sql = next(
            statement
            for statement in spark.sql_calls
            if statement.startswith("CREATE EXTERNAL TABLE IF NOT EXISTS")
        )
        self.assertIn(
            "CREATE EXTERNAL TABLE IF NOT EXISTS `cdl_planejamentocaixa_integration`.`an_test` (`id` bigint, `amount` double)",
            create_sql,
        )
        self.assertIn("PARTITIONED BY (`execution_ts` string)", create_sql)
        self.assertIn("STORED AS PARQUET", create_sql)
        self.assertIn("LOCATION 's3a://integration/analitico/teste'", create_sql)
        self.assertIn("TBLPROPERTIES ('classification'='parquet')", create_sql)
        self.assertIn(
            "ALTER TABLE `cdl_planejamentocaixa_integration`.`an_test` SET TBLPROPERTIES ('classification'='parquet')",
            spark.sql_calls,
        )
        self.assertIn(
            "MSCK REPAIR TABLE `cdl_planejamentocaixa_integration`.`an_test`",
            spark.sql_calls,
        )
        self.assertEqual(
            ["`cdl_planejamentocaixa_integration`.`an_test`"],
            spark.catalog.refreshed_tables,
        )

    def test_adds_only_missing_non_partition_columns(self):
        conf_process = self.build_conf_process(
            {
                "OutputDatabase": "cdl_planejamentocaixa_business",
                "OutputTable": "an_test",
                "OutputPath": "s3a://business/analitico/teste",
            }
        )
        spark = FakeSpark(
            table_exists=True,
            existing_schema=FakeSchema(
                [
                    FakeField("id", "bigint"),
                    FakeField("execution_ts", "string"),
                ]
            ),
        )
        df_input = FakeDataFrame(
            [
                FakeField("id", "bigint"),
                FakeField("amount", "double"),
                FakeField("execution_ts", "string"),
            ]
        )

        conf_process._ConfProcess__sync_catalog_table(spark, df_input, ["execution_ts"])

        self.assertIn(
            "ALTER TABLE `cdl_planejamentocaixa_business`.`an_test` ADD COLUMNS (`amount` double)",
            spark.sql_calls,
        )
        add_columns_sql = next(
            statement for statement in spark.sql_calls if "ADD COLUMNS" in statement
        )
        self.assertNotIn("execution_ts", add_columns_sql)


if __name__ == "__main__":
    unittest.main()
