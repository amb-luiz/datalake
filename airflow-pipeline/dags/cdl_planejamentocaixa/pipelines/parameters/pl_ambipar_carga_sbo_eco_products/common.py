DAG_ID = "pl_ambipar_carga_SBO_ECO_PRODUCTS"
SOURCE_SYSTEM = "sbo"
DATABASE_NAME = "SBO_ECOSORB"

# id_ctrl is retained as source metadata; all queries use the sap connection.
CARGAS_ECO = [
    {
        "nome": "ORTT_ECO_PRODUCTS",
        "sql": "SELECT * FROM dbo.ORTT WITH (NOLOCK)",
    },
    {
        "nome": "OFRC_ECO_PRODUCTS",
        "id_ctrl": 2,
        "sql": """
            SELECT *, 5 AS empresaID
            FROM dbo.OFRC WITH (NOLOCK)
        """,
    },
    {
        "nome": "OPRC_ECO_PRODUCTS",
        "id_ctrl": 3,
        "sql": """
            SELECT *, 5 AS empresaID
            FROM dbo.OPRC WITH (NOLOCK)
        """,
    },
    {
        "nome": "OFRT_ECO_PRODUCTS",
        "id_ctrl": 3,
        "sql": """
            SELECT *, 5 AS empresaID
            FROM dbo.OFRT WITH (NOLOCK)
        """,
    },
    {
        "nome": "FRC1_ECO_PRODUCTS",
        "id_ctrl": 3,
        "sql": """
            SELECT *, 5 AS empresaID
            FROM dbo.FRC1 WITH (NOLOCK)
        """,
    },
    {
        "nome": "OACT_ECO_PRODUCTS",
        "id_ctrl": 3,
        "sql": """
            SELECT *, 5 AS empresaID
            FROM dbo.OACT WITH (NOLOCK)
        """,
    },
    {
        "nome": "JDT1_ECO_PRODUCTS",
        "id_ctrl": 3,
        "sql": """
            SELECT
                Line_ID,
                Account,
                ContraAct,
                Credit,
                Debit,
                LineMemo,
                RefDate,
                ProfitCode,
                TransId,
                Project,
                DebCred,
                TransType,
                BaseRef,
                BPLId,
                5 AS empresaID
            FROM dbo.JDT1 WITH (NOLOCK)
            WHERE YEAR(RefDate) >= YEAR(GETDATE()) - 5
        """,
    },
    {
        "nome": "OPCH_ECO_PRODUCTS",
        "id_ctrl": 3,
        "sql": """
            SELECT
                DocEntry,
                DocDate,
                U_nchamado,
                Comments,
                CardCode,
                CardName,
                Serial,
                5 AS empresaID
            FROM dbo.OPCH WITH (NOLOCK)
            WHERE YEAR(DocDate) >= YEAR(GETDATE()) - 5
        """,
    },
    {
        "nome": "POR1_ECO_PRODUCTS",
        "id_ctrl": 3,
        "sql": """
            SELECT
                TrgetEntry,
                AcctCode,
                DocEntry,
                Dscription,
                TargetType,
                5 AS empresaID
            FROM dbo.POR1 WITH (NOLOCK)
            WHERE YEAR(DocDate) >= YEAR(GETDATE()) - 5
        """,
    },
    {
        "nome": "OPOR_ECO_PRODUCTS",
        "id_ctrl": 3,
        "sql": """
            SELECT *, 5 AS empresaID
            FROM dbo.OPOR WITH (NOLOCK)
            WHERE YEAR(DocDate) >= YEAR(GETDATE()) - 5
        """,
    },
    {
        "nome": "OINV_ECO_PRODUCTS",
        "id_ctrl": 3,
        "sql": """
            SELECT
                DocEntry,
                DocDate,
                U_nchamado,
                Comments,
                CardCode,
                CardName,
                Serial,
                5 AS empresaID
            FROM dbo.OINV WITH (NOLOCK)
            WHERE YEAR(DocDate) >= YEAR(GETDATE()) - 5
        """,
    },
    {
        "nome": "RDR1_ECO_PRODUCTS",
        "id_ctrl": 3,
        "sql": """
            SELECT *, 5 AS empresaID
            FROM dbo.RDR1 WITH (NOLOCK)
        """,
    },
    {
        "nome": "OBPL_ECO_PRODUCTS",
        "id_ctrl": 3,
        "sql": """
            SELECT
                BPLId,
                COALESCE(
                    AliasName,
                    (SELECT COALESCE(AliasName, CompnyName) FROM dbo.OADM)
                ) AS AliasName,
                5 AS empresaID
            FROM dbo.OBPL WITH (NOLOCK)
            WHERE Disabled = 'N'
        """,
    },
    {
        "nome": "ORDR_ECO_PRODUCTS",
        "id_ctrl": 93,
        "sql": """
            SELECT *, 5 AS empresaID
            FROM dbo.ORDR WITH (NOLOCK)
        """,
    },
]
