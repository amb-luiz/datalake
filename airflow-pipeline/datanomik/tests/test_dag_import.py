import ast
import builtins
import importlib.util
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

DAG_PATH = Path(__file__).resolve().parents[1] / "dags/cdl_planejamentocaixa/pipelines/pl_ambipar_Datanomik_Fluxo_Caixa.py"
HAS_AIRFLOW = importlib.util.find_spec("airflow") is not None


class DagTests(unittest.TestCase):
    def test_dag_compiles_without_executing_or_loading_credentials(self):
        source = DAG_PATH.read_text()
        tree = ast.parse(source)
        compile(tree, str(DAG_PATH), "exec")
        decorator = next(node.decorator_list[0] for node in tree.body
                         if isinstance(node, ast.FunctionDef) and node.name == "datanomik_transactions_etl")
        keywords = {kw.arg: kw.value for kw in decorator.keywords}
        self.assertIsNone(ast.literal_eval(keywords["schedule"]))
        self.assertFalse(ast.literal_eval(keywords["catchup"]))
        self.assertEqual(ast.literal_eval(keywords["dag_id"]), "pl_ambipar_Datanomik_Fluxo_Caixa")

    @unittest.skipUnless(HAS_AIRFLOW, "Real Airflow unavailable in host")
    def test_real_dag_import_no_network_no_credential_read(self):
        import airflow.sdk  # noqa: F401 -- initialize framework before guarding DAG parse.
        from airflow.models.dagbag import DagBag
        from airflow.serialization.serialized_objects import SerializedDAG

        original_open, original_io_open = builtins.open, io.open

        def guarded(original):
            def checked(file, *args, **kwargs):
                name = str(file).lower()
                if "credential" in name or "credencia" in name or "/secrets/" in name:
                    raise AssertionError("Credential file accessed during DAG parse")
                return original(file, *args, **kwargs)
            return checked

        with tempfile.TemporaryDirectory() as directory, \
                patch.dict("os.environ", {"AIRFLOW_HOME": directory,
                                          "DATANOMIK_CREDENTIALS_FILE": "/secrets/forbidden.json"}), \
                patch("builtins.open", side_effect=guarded(original_open)), \
                patch("io.open", side_effect=guarded(original_io_open)), \
                patch("socket.socket.connect", side_effect=AssertionError("Network during DAG parse")), \
                patch("datanomik_client.DatanomikClient", side_effect=AssertionError("Client during parse")):
            bag = DagBag(dag_folder=str(DAG_PATH), include_examples=False, safe_mode=False)
            self.assertEqual(bag.import_errors, {})
            dag = bag.dags["pl_ambipar_Datanomik_Fluxo_Caixa"]
            self.assertEqual(str(dag.timezone), "America/Sao_Paulo")
            self.assertEqual(dag.max_active_runs, 1)
            self.assertEqual(dag.default_args["retries"], 2)
            self.assertTrue(dag.default_args["retry_exponential_backoff"])
            self.assertEqual(dag.get_task("extrair_conta").upstream_task_ids, {"selecionar_contas"})
            self.assertEqual(dag.get_task("extrair_conta").max_active_tis_per_dag, 3)
            self.assertEqual(dag.task_ids, ["selecionar_contas", "extrair_conta", "tratar_e_publicar"])
            SerializedDAG.to_dict(dag)

    @unittest.skipUnless(HAS_AIRFLOW, "Real Airflow unavailable in host")
    def test_permanent_http_errors_fail_task_without_airflow_retry(self):
        from airflow.exceptions import AirflowFailException
        from datanomik_client import DatanomikError

        spec = importlib.util.spec_from_file_location("datanomik_task_test", DAG_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        def denied():
            raise DatanomikError("/v1/accounts", 401, message="Unauthorized")

        with self.assertRaises(AirflowFailException):
            module._execute(denied)
