SOURCE_SYSTEM = "sbo_bioenv"


TABLE_SPECS = [
    {
        "table_name": "OFRC",
        "landing_dataset": "OFRC_Landing",
        "sql_query": """
            SELECT *, 93 AS empresaID
            FROM dbo.OFRC WITH(NOLOCK)
        """.strip(),
    },
    {
        "table_name": "OFRT",
        "landing_dataset": "OFRT_Landing",
        "sql_query": """
            SELECT *, 93 AS empresaID
            FROM dbo.OFRT WITH(NOLOCK)
        """.strip(),
    },
    {
        "table_name": "OPRC",
        "landing_dataset": "OPRC_Landing",
        "sql_query": """
            SELECT *, 93 AS empresaID
            FROM dbo.OPRC WITH(NOLOCK)
        """.strip(),
    },
    {
        "table_name": "FRC1",
        "landing_dataset": "FRC1_Landing",
        "sql_query": """
            SELECT *, 93 AS empresaID
            FROM dbo.FRC1 WITH(NOLOCK)
        """.strip(),
    },
    {
        "table_name": "OACT",
        "landing_dataset": "OACT_Landing",
        "sql_query": """
            SELECT *, 93 AS empresaID
            FROM dbo.OACT WITH(NOLOCK)
        """.strip(),
    },
    {
        "table_name": "JDT1",
        "landing_dataset": "JDT1_Landing",
        "sql_query": """
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
                BPLid,
                93 AS empresaID
            FROM dbo.JDT1 WITH(NOLOCK)
            WHERE YEAR(RefDate) >= YEAR(GETDATE()) - 5
        """.strip(),
    },
    {
        "table_name": "OPCH",
        "landing_dataset": "OPCH_Landing",
        "sql_query": """
            SELECT
                DocEntry,
                DocDate,
                U_nchamado,
                Comments,
                CardCode,
                CardName,
                93 AS empresaID
            FROM dbo.OPCH WITH(NOLOCK)
            WHERE YEAR(DocDate) >= YEAR(GETDATE()) - 5
        """.strip(),
    },
    {
        "table_name": "POR1",
        "landing_dataset": "POR1_Landing",
        "sql_query": """
            SELECT
                TrgetEntry,
                AcctCode,
                DocEntry,
                Dscription,
                TargetType,
                93 AS empresaID
            FROM dbo.POR1 WITH(NOLOCK)
            WHERE YEAR(DocDate) >= YEAR(GETDATE()) - 5
        """.strip(),
    },
    {
        "table_name": "OPOR",
        "landing_dataset": "OPOR_Landing",
        "sql_query": """
            SELECT *, 93 AS empresaID
            FROM dbo.OPOR WITH(NOLOCK)
            WHERE YEAR(DocDate) >= YEAR(GETDATE()) - 5
        """.strip(),
    },
    {
        "table_name": "OINV",
        "landing_dataset": "OINV_Landing",
        "sql_query": """
            SELECT
                DocEntry,
                DocDate,
                U_nchamado,
                Comments,
                CardCode,
                CardName,
                Serial,
                93 AS empresaID
            FROM dbo.OINV WITH(NOLOCK)
            WHERE YEAR(DocDate) >= YEAR(GETDATE()) - 5
        """.strip(),
    },
    {
        "table_name": "RDR1",
        "landing_dataset": "RDR1_Landing",
        "sql_query": """
            SELECT *, 93 AS empresaID
            FROM dbo.RDR1 WITH(NOLOCK)
        """.strip(),
    },
    {
        "table_name": "OBPL",
        "landing_dataset": "OBPL_Landing",
        "sql_query": """
            SELECT
                BPLId,
                COALESCE(
                    AliasName,
                    (
                        SELECT COALESCE(AliasName, CompnyName)
                        FROM dbo.OADM
                    )
                ) AS AliasName,
                93 AS empresaID
            FROM dbo.OBPL WITH(NOLOCK)
            WHERE Disabled = 'N'
        """.strip(),
    },
    {
        "table_name": "ORDR",
        "landing_dataset": "ORDR_Landing",
        "sql_query": """
            SELECT *, 93 AS empresaID
            FROM dbo.ORDR WITH(NOLOCK)
        """.strip(),
    },
]
