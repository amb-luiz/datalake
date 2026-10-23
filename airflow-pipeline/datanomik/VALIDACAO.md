# Validação da entrega — 14/10/2026

Processo: `pl_ambipar_Datanomik_Fluxo_Caixa`.

## Resultados efetivamente executados

| Verificação | Resultado |
|---|---|
| unittest, transporte HTTP com mocks e nenhuma API real | 43 testes descobertos: 38 aprovados, 5 ignorados |
| Basic Auth pelo PreparedRequest real de requests, sem rede | Aprovado |
| Paginação com todas as quatro chaves e limite de segurança | Aprovado |
| Retry de 429/500/502/503/504, backoff e Retry-After | Aprovado |
| 401 imediato; mensagens que ecoam segredos descartadas | Aprovado |
| Dez duplicidades; soma absoluta 49600.00 | Aprovado |
| Deduplicação global, primeira ocorrência, contas diferentes, chave ausente | Aprovado |
| Filtro CNPJ por titular, zeros/texto, sender.document vazio | Aprovado nas funções Python |
| JSONL com Decimal; arquivo anterior preservado após falha | Aprovado |
| Extração parcial e checksum divergente impedem publicação | Aprovado |
| Compilação de todos os módulos e DAG | Aprovado |
| Lint offline de sintaxe e espaços | Aprovado — 12 arquivos Python |
| `bash -n` dos scripts | Aprovado |
| `docker compose config --quiet` com valores sintéticos | Aprovado |
| Mount do segredo somente leitura; testes sem rede/segredo | Aprovado na configuração |
| Instalador em diretório temporário: reaplicação, conflitos, preservação | Aprovado |
| Ruff | Não executado: módulo não instalado |
| Build/inicialização/testes em Docker | Bloqueado: acesso negado a `/var/run/docker.sock` |

Os cinco testes ignorados precisam ser executados no serviço Docker `tests`:

1. Importação/serialização real do DAG com bloqueio de rede e leitura de credenciais.
2. Conversão de erro HTTP permanente em `AirflowFailException` no runtime real.
3. Round-trip real Parquet: esquema aninhado, campos de texto, Decimal e tipos heterogêneos.
4. Parquet vazio válido.
5. ETL completo com API mockada, PyArrow real, replay e falha de publicação.

O host tem Python 3.12 e requests, mas não tem Airflow, PyArrow nem Ruff.
A tentativa de instalar dependências falhou por resolução de nome/acesso ao PyPI.
Foi tentado `docker compose ... run --rm --no-deps tests`; o daemon não pôde ser
acessado. Portanto, **não há validação completa de produção, importação real do
DAG, build da imagem, execução em scheduler ou carga real** nesta entrega.

## Credenciais e repositório

- O JSON real `/opt/apps/datalake/secrets/credenciais_datanomik.json` não foi aberto,
  copiado, alterado nem impresso. Nenhuma chamada à API real foi feita.
- `git ls-files --error-unmatch secrets/credenciais_datanomik.json` confirmou que
  esse arquivo não está rastreado no índice atual do repositório original.
- `git check-ignore secrets/credenciais_datanomik.json` confirmou a exclusão atual.
  Isso não é uma auditoria do histórico Git.
- Os testes criam credenciais aleatórias sintéticas somente em diretórios
  temporários; as saídas de testes não mostram seus valores.
- No DAG, o cliente é importado/instanciado somente dentro da execução das
  tarefas. A leitura do segredo não acontece no corpo do módulo do DAG.
- O Dockerfile usa contexto restrito a requirements/Dockerfile. O bind do arquivo
  existente é somente leitura, com `create_host_path: false`.
- Os arquivos foram preparados em `/home/administrador/datanomik_entrega` porque
  a sessão não permite escrita em `/opt/apps/datalake`. O instalador foi testado
  em `/tmp`; **não foi aplicado ao projeto original**.

## Comando para completar a validação

Após aplicar a entrega, em um terminal com Docker e acesso aos registros/PyPI:

```bash
cd /opt/apps/datalake/airflow-pipeline/datanomik
python3 scripts/bootstrap.py
bash scripts/compose.sh --profile test build tests
bash scripts/compose.sh --profile test run --rm --no-deps tests
```

O serviço exige Airflow e PyArrow: ausência dessas dependências causa falha.
Ele executa Ruff, toda a suíte (incluindo os cinco casos acima) e `pip check`,
com rede desativada e sem montar o JSON real.
