# pl_ambipar_Datanomik_Fluxo_Caixa

Integração manual Datanomik → JSON Lines → Parquet. O identificador definitivo do
DAG é `pl_ambipar_Datanomik_Fluxo_Caixa`; a função TaskFlow interna chama-se
`datanomik_transactions_etl`.

## Compatibilidade e instalação

O projeto original em `/opt/apps/datalake/airflow-pipeline` usa imagem
`apache/airflow:2.2.2-python3.8`. Essa versão não suporta dynamic task mapping.
Esta entrega adiciona o subprojeto `airflow-pipeline/datanomik`, com Airflow
**3.3.1 / Python 3.12**, PostgreSQL **16.13**, LocalExecutor, banco de metadados,
rede, logs e dados próprios. O padrão `dags/cdl_planejamentocaixa/pipelines`
foi mantido. Não instale este DAG na pasta monitorada pelo Airflow 2.2.2.
Nenhuma dependência, configuração ou DAG legado precisa ser removido ou migrado.

Nesta sessão só foi possível escrever em `/home/administrador` e `/tmp`.
Os arquivos estão em `/home/administrador/datanomik_entrega`. Para aplicá-los:

```bash
python3 /home/administrador/datanomik_entrega/install.py
python3 /home/administrador/datanomik_entrega/install.py --apply
cd /opt/apps/datalake/airflow-pipeline/datanomik
```

O instalador faz uma inspeção completa antes de escrever, recusa sobrescrever
arquivos diferentes, permite reaplicação idêntica e acrescenta apenas regras
ausentes ao `.gitignore` da raiz. Não toca no conteúdo de `secrets`, no Compose,
Dockerfile ou requirements do ambiente legado. Não inicia contêineres.

## Credencial existente

O Compose monta **o arquivo existente**:

```text
/opt/apps/datalake/secrets/credenciais_datanomik.json
    → /opt/airflow/secrets/datanomik_airflow.json (somente leitura)
```

O arquivo deve conter `datanomik.secretId`, `datanomik.secretPassword` e,
opcionalmente, `datanomik.baseUrl` igual a `https://api.datanomik.com`.
Não há arquivo de exemplo com valores, cópia do JSON, uso de Airflow Variables
ou XCom para autenticação. O Dockerfile copia somente requirements; o contexto
de build exclui todo o restante por padrão. O caminho do arquivo no host pode
ser alterado por `DATANOMIK_CREDENTIALS_HOST_FILE` (inclusive um caminho Windows).
O cliente aceita `DATANOMIK_CREDENTIALS_FILE` em tempo de execução.
Ao alterar o caminho interno, ajuste também o destino do bind mount.

O arquivo só é aberto ao instanciar `DatanomikClient` dentro de uma tarefa.
Seu usuário do contêiner precisa ter permissão de leitura; o bootstrap define
`AIRFLOW_UID` como o UID do operador. Não é necessário tornar o JSON público.

## Inicialização e validação

Pré-requisitos: Docker Engine acessível, Docker Compose, acesso aos registros de
imagens e PyPI durante o build; saída HTTPS para Datanomik durante a carga real.

```bash
cd /opt/apps/datalake/airflow-pipeline/datanomik
python3 scripts/bootstrap.py
bash scripts/compose.sh config --quiet
bash scripts/compose.sh build
bash scripts/compose.sh --profile test build tests
bash scripts/compose.sh --profile test run --rm --no-deps tests
bash scripts/compose.sh up -d postgres
bash scripts/compose.sh run --rm airflow-init
bash scripts/compose.sh up -d airflow-api-server airflow-scheduler airflow-dag-processor
bash scripts/compose.sh run --rm airflow-api-server airflow users create \
  --username admin --firstname Operador --lastname Airflow \
  --role Admin --email operador@example.invalid
```

O último comando solicita uma **nova senha da interface Airflow** interativamente.
Use seu e-mail operacional. Essa conta não é a credencial Datanomik.
`bootstrap.py` cria apenas segredos novos da infraestrutura isolada em
`.runtime/stack.env`, com permissão `0600`, sem imprimi-los; preserva um arquivo já
existente e nunca abre o JSON Datanomik. Guarde esse arquivo para reinicializações
do mesmo banco. Evite `docker compose config` sem `--quiet`, pois a expansão contém
os segredos da infraestrutura.

Interface em `http://127.0.0.1:8089`. O bind fica restrito ao host; use o acesso
administrativo/reverse proxy da organização para acesso remoto. A imagem inclui
FAB Auth Manager. Essa composição é uma implantação local de nó único; HA,
backup dos volumes e acesso remoto devem seguir a infraestrutura do ambiente.

O serviço `tests` tem `network_mode: none`, não monta credenciais, não herda a
conexão do banco e usa mocks. Executa Ruff, unittest, importação/serialização
real do DAG, leitura real do Parquet e `pip check`. A variável
`DATANOMIK_REQUIRE_FULL_TESTS=1` transforma a falta de Airflow/PyArrow em falha,
evitando confundir testes ignorados com validação completa.

Teste apenas de importação, sem chamadas à API:

```bash
bash scripts/compose.sh --profile test run --rm --no-deps tests \
  -c 'cd /workspace && python -m unittest discover -s tests -p test_dag_import.py -v'
```

Checagem do registro do DAG após o parser processá-lo:

```bash
bash scripts/compose.sh exec airflow-scheduler airflow dags list-import-errors
bash scripts/compose.sh exec airflow-scheduler airflow tasks list pl_ambipar_Datanomik_Fluxo_Caixa
```

No host, a suíte parcial sem Airflow/PyArrow pode ser executada com:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 scripts/lint_offline.py
```

O lint offline verifica apenas sintaxe e espaços; não substitui Ruff.
Veja `VALIDACAO.md` para os resultados efetivamente obtidos nesta entrega.

## Carga manual

O DAG nasce pausado, `schedule=None`, `catchup=False`, timezone
`America/Sao_Paulo`, uma execução ativa por vez. **Nenhuma carga real foi executada
na implementação.** Os comandos abaixo fazem chamadas reais e devem ser usados
quando você decidir executar a carga.

```bash
bash scripts/compose.sh exec airflow-scheduler airflow dags unpause pl_ambipar_Datanomik_Fluxo_Caixa
bash scripts/compose.sh exec airflow-scheduler airflow dags trigger \
  --logical-date '2026-10-14T12:00:00-03:00' \
  --run-id 'manual_datanomik_20261014_001' \
  --conf '{"date_from":"2026-10-01","date_to":"2026-10-14"}' \
  pl_ambipar_Datanomik_Fluxo_Caixa
bash scripts/compose.sh exec airflow-scheduler airflow tasks states-for-dag-run \
  pl_ambipar_Datanomik_Fluxo_Caixa manual_datanomik_20261014_001
```

Troque período/data lógica e `run-id` conforme a carga desejada. Para todas as
transações sem filtro de data, use `--conf '{}'`. Para filtrar a empresa, acrescente
`"cnpj":"DOCUMENTO_DA_EMPRESA"`; para contas específicas, acrescente
`"account_ids":["ID_DA_CONTA"]`. Esses textos são marcadores, não valores de API.
Com CNPJ, inclusive quando há IDs explícitos, o cliente consulta contas para
validar o titular. Sem correspondência de documento, a tarefa falha; não amplia
a seleção silenciosamente. A ausência de contas sem CNPJ produz relatório zerado.

Teste operacional do DAG **com API real**, se desejar diagnosticar uma execução
fora do scheduler (não faz parte dos testes automatizados):

```bash
bash scripts/compose.sh run --rm airflow-scheduler airflow dags test \
  pl_ambipar_Datanomik_Fluxo_Caixa '2026-10-14T12:00:00-03:00' \
  --dagfile-path /opt/airflow/dags/cdl_planejamentocaixa/pipelines/pl_ambipar_Datanomik_Fluxo_Caixa.py \
  --conf '{"date_from":"2026-10-01","date_to":"2026-10-14"}'
```

Despausar habilita execuções manuais; não cria agenda com `schedule=None`.
Nenhuma agenda foi adicionada e nenhuma dependência em processos existentes foi criada.

## Fluxo, retries e limites

1. `selecionar_contas`: valida datas; usa os IDs informados ou pagina `/v1/accounts`;
   aplica comparação de dígitos do CNPJ nos documentos do titular. Objetos
   `owner`, `owners`, `holder`, `holders`, `accountHolder` e `account_holder` são
   reconhecidos, com documentos escalares ou estruturados. Documento de
   beneficiário/remetente não seleciona conta. Se a versão da resposta não trouxer
   documentos do titular, o filtro falha de forma explícita.
2. `extrair_conta.expand(...)`: uma tarefa por conta, preservando ordem das
   páginas e registros; grava JSONL e manifesto com contagem e SHA-256.
3. `tratar_e_publicar`: espera as extrações, valida todos os manifestos/checksums,
   reúne todos os registros em SQLite temporário e só então deduplica; gera
   Parquet por conta e publica `data_quality.json` como marcador de conclusão.

XCom contém somente IDs de contas, caminhos e um resumo pequeno. Os dados e o
índice de deduplicação ficam em disco. A união do esquema Parquet exige duas
passagens pelos registros retidos; a escrita usa lotes de 1.000 registros.

`requests.Session` usa Basic Auth, `Accept: application/json`, TLS verificado,
timeout de conexão de 10 s e leitura de 60 s. Segue até três retentativas HTTP
por página para 429/500/502/503/504 e erros transitórios de conexão, backoff
exponencial e `Retry-After` limitado a 120 s. Não segue redirects, não usa netrc
ou proxies herdados. Para CA corporativa, monte o certificado e configure
`REQUESTS_CA_BUNDLE` sem desabilitar TLS.

No Airflow, `retries=2` significa **duas retentativas além da tentativa inicial**,
com backoff exponencial. 400/401/403 e demais 4xx permanentes viram
`AirflowFailException`, sem retentativa de tarefa. Erros de credenciais também
falham diretamente. Exceções HTTP contêm apenas método, endpoint, status,
traceId validado e mensagem da API de uma lista pública permitida; mensagens
arbitrárias são substituídas para impedir eco de segredos ou dados bancários.

Limites configuráveis via ambiente do Compose:

| Variável | Padrão | Função |
|---|---:|---|
| `DATANOMIK_ACCOUNT_CONCURRENCY` | 3 | Tarefas de extração simultâneas |
| `DATANOMIK_MAX_ACCOUNTS` | 1024 | Limite do mapeamento e da seleção |
| `DATANOMIK_MAX_PAGES` | 10000 | Páginas por endpoint/conta |
| `DATANOMIK_UI_PORT` | 8089 | Porta local da interface |

A paginação começa em `page=0`, `size=100`, aceita `content`, `data`, `items`,
`results` ou lista direta; termina com `last=true`, `totalPages` atingido ou
página menor que 100. Atingir o limite sem término falha, sem publicar extração
parcial. Paginação por número de página não garante snapshot se o banco alterar
as transações durante a leitura; o relatório remove duplicidades observadas,
mas não consegue recuperar registros omitidos pela paginação da própria API.

## Dados, ordem e relatório

Volume nomeado `ambipar-datanomik-fluxo-caixa_datanomik-data`:

```text
/opt/airflow/data/datanomik/pl_ambipar_Datanomik_Fluxo_Caixa/
  date=AAAA-MM-DD/run=SHA256_DO_RUN_ID/
    selection.json
    raw/account=SHA256_DO_ID/transactions.jsonl
    raw/account=SHA256_DO_ID/manifest.json
    treated/generation=SHA256_DOS_MANIFESTOS/
      account=SHA256_DO_ID/transactions.parquet
      data_quality.json
    data_quality.json
```

Data lógica convertida para São Paulo; quando a execução manual do Airflow 3 não
tem `logical_date`, usa `dag_run.run_after`, que é estável na execução. O hash do
run-id separa cargas da mesma data e preserva caminhos nas retentativas. IDs de
conta são mapeados para os diretórios nos manifestos e no relatório, evitando
traversal ou colisões por caracteres especiais.

Toda escrita de arquivo usa temporário no mesmo diretório, fsync e rename
atômico. O conjunto tratado é preparado em diretório temporário e renomeado
para uma geração imutável. O relatório na raiz é atualizado por último, de
forma atômica. Consumidores devem ler **somente os caminhos da geração indicados
nesse relatório**, nunca inferir sucesso pela existência de um Parquet isolado.
Falhas preservam o relatório anterior. Limpeza de temporários abandonados por
SIGKILL e retenção de cargas antigas são operações externas; não há limpeza
destrutiva automática. Não execute duas transformações manuais simultâneas para
o mesmo run-id fora do controle do scheduler.

Deduplicação usa estritamente `(external_id, account.id)`, comparados como texto,
sem preencher IDs ausentes com o `accountId` consultado. A primeira ocorrência
vence pela ordem de contas selecionadas, página e posição na resposta. A ordem
de término das tarefas paralelas não interfere. Chave incompleta (nulo, vazio
ou somente espaços) é mantida e contabilizada. O índice global em SQLite impede
deduplicação apenas por página ou somente dentro de cada tarefa.

`data_quality.json` traz `porConta` e `consolidado`, contendo:

- `quantidadeOriginal`, `quantidadeFinal`, `duplicidadesRemovidas`;
- `valorDuplicadoRemovido`: soma com sinal dos `amount` das ocorrências removidas;
- `valorDuplicadoRemovidoAbsoluto`: soma dos valores absolutos dessas ocorrências;
- `gruposComValorDivergente`: grupos cuja quantia difere da primeira, contados uma vez;
- `registrosSemChave`, mais `registrosSemValor` e `duplicidadesSemValor`.

Totais monetários são strings decimais exatas no relatório, sem float. Quantias
ausentes não são fabricadas e ficam fora das somas; quantias inválidas falham.
O campo monetário usado é `amount`. Se o contrato da sua conta usar outro campo,
ele precisa ser configurado explicitamente no código, sem adivinhação. Os totais
por conta são atribuídos à conta consultada; um grupo divergente pertence à conta
da primeira ocorrência. Em seleções multimoeda, as somas consolidadas são apenas
métricas aritméticas de qualidade e não representam conversão cambial.

Cada linha Parquet tem:

- `record`: estrutura aninhada com campos originais tratados. Identificadores,
  números de conta/agência e documentos permanecem como texto. Reconhece variantes
  snake_case/camelCase; `account.number` também é protegido. Zeros existentes são
  preservados; números recebidos como números não recebem zeros inventados.
- `source_record`: JSON completo do registro original, com valores numéricos
  decimais, objetos vazios e presença/ausência exata de campos. É a representação
  autoritativa quando um campo muda de tipo entre linhas. Campos heterogêneos de
  `record` viram JSON textual; o esquema dessa conversão está nos metadados Parquet.

Valores numéricos são lidos com `Decimal`; Parquet usa decimal128/decimal256 quando
possível. Valores numéricos de campos não monetários que excedam a capacidade de
Decimal256 ficam em texto, preservados também em `source_record`. Nenhuma hora de
liquidação ou identificação de remetente é inferida. `sender.document` nulo/vazio
continua nulo/vazio. O JSONL mantém todos os registros originais, inclusive duplicados.

A interface `TransactionSink.load(report_path)` permite implementar depois um
carregador idempotente por geração para banco/object storage em outro DAG. A
carga local não depende da escolha desse destino.

## Referências verificadas em 14/10/2026

- [Datanomik: transações](https://docs.datanomik.com/reference/list-transactions):
  `accountId`, `dateFrom` e `dateTo`; datas em `YYYY-MM-DD`.
- [Datanomik: contas](https://docs.datanomik.com/reference/list-accounts).
- [Datanomik: autenticação](https://docs.datanomik.com/reference/authentication):
  secretId como usuário e secretPassword como senha de Basic Auth.
- [Datanomik: recursos para desenvolvedores](https://www.datanomik.com/en-us/desenvolvedores):
  paginação com `page` iniciado em zero e `size`.
- [Airflow: dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/3.3.1/authoring-and-scheduling/dynamic-task-mapping.html).
- [Constraints Airflow 3.3.1/Python 3.12](https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt).
- [CLI Airflow 3.3.1](https://airflow.apache.org/docs/apache-airflow/3.3.1/cli-and-env-variables-ref.html).

O OpenAPI público retornou erro de acesso/404 durante a consulta; os filtros
foram confirmados na referência oficial do endpoint. Não foi consultada nenhuma
conta ou transação real para inferir o esquema.
