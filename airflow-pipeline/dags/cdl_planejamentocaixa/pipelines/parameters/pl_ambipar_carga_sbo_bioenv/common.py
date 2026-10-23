SOURCE_SYSTEM = "sbo"


TABLE_SPECS = [
    {
        "table_name": "ORTT",
        "landing_dataset": "ORTT_BIOENV",
        "sql_query": "SELECT * FROM dbo.ORTT WITH (NOLOCK)",
    },
    {
        "table_name": "OFRC",
        "landing_dataset": "OFRC_BIOENV",
        "sql_query": """
            SELECT *, 93 as empresaID FROM dbo.OFRC WITH(NOLOCK)
        """.strip(),
    },
    {
        "table_name": "OFRT",
        "landing_dataset": "OFRT_BIOENV",
        "sql_query": """
            SELECT *, 93 as empresaID FROM dbo.OFRT WITH(NOLOCK)
        """.strip(),
    },
    {
        "table_name": "OPRC",
        "landing_dataset": "OPRC_BIOENV",
        "sql_query": """
            SELECT *, 93 as empresaID FROM dbo.OPRC WITH(NOLOCK)
        """.strip(),
    },
    {
        "table_name": "FRC1",
        "landing_dataset": "FRC1_BIOENV",
        "sql_query": """
            SELECT *, 93 as empresaID FROM dbo.FRC1 WITH(NOLOCK)
        """.strip(),
    },
    {
        "table_name": "OACT",
        "landing_dataset": "OACT_BIOENV",
        "sql_query": """
            SELECT *, 93 as empresaID FROM dbo.OACT WITH(NOLOCK)
        """.strip(),
    },
    {
        "table_name": "JDT1",
        "landing_dataset": "JDT1_BIOENV",
        "sql_query": """
            SELECT Line_ID, Account, ContraAct, Credit, Debit, LineMemo, RefDate, ProfitCode, TransId, Project, DebCred, TransType, BaseRef, BPLid, 93 as empresaID
            FROM dbo.JDT1 WITH(NOLOCK)
            WHERE year(refDate) >= year(getdate())-5
        """.strip(),
    },
    {
        "table_name": "OPCH",
        "landing_dataset": "OPCH_BIOENV",
        "sql_query": """
            SELECT DocEntry, DocDate, U_nchamado, Comments, CardCode, CardName, 93 as empresaID
            FROM dbo.OPCH WITH(NOLOCK)
            WHERE year(DocDate) >= year(getdate())-5
        """.strip(),
    },
    {
        "table_name": "POR1",
        "landing_dataset": "POR1_BIOENV",
        "sql_query": """
            SELECT TrgetEntry, AcctCode, DocEntry, Dscription, TargetType, 93 as empresaID
            FROM dbo.POR1 WITH(NOLOCK)
            WHERE year(DocDate) >= year(getdate())-5
        """.strip(),
    },
    {
        "table_name": "OPOR",
        "landing_dataset": "OPOR_BIOENV",
        "sql_query": """
            SELECT *, 93 as empresaID
            FROM dbo.OPOR WITH(NOLOCK)
            WHERE year(DocDate) >= year(getdate())-5
        """.strip(),
    },
    {
        "table_name": "OINV",
        "landing_dataset": "OINV_BIOENV",
        "sql_query": """
            SELECT DocEntry, DocDate, U_nchamado, Comments, CardCode, CardName, Serial, 93 as empresaID
            FROM dbo.OINV WITH(NOLOCK)
            WHERE year(DocDate) >= year(getdate())-5
        """.strip(),
    },
    {
        "table_name": "RDR1",
        "landing_dataset": "RDR1_BIOENV",
        "sql_query": """
            SELECT *, 93 as empresaID FROM dbo.RDR1 WITH(NOLOCK)
        """.strip(),
    },
    {
        "table_name": "OBPL",
        "landing_dataset": "OBPL_BIOENV",
        "sql_query": """
            SELECT BPLId, COALESCE(AliasName,(select COALESCE(AliasName,CompnyName) from dbo.OADM)) AliasName, 93 as empresaID
            FROM dbo.OBPL WITH(NOLOCK)
            where Disabled = 'N'
        """.strip(),
    },
    {
        "table_name": "ORDR",
        "landing_dataset": "ORDR_BIOENV",
        "sql_query": """
            SELECT *, 93 as empresaID FROM dbo.ORDR WITH(NOLOCK)
        """.strip(),
    },
]
