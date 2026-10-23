SOURCE_SYSTEM = "sbo"


def _empresa_literal(value):
    return str(value)


def _standard_specs(
    *,
    alias,
    empresa_sql,
    opch_has_serial=True,
    opch_empresa_sql=None,
    jdt1_bplid_sql="BPLid",
    jdt1_landing_dataset=None,
    obpl_empresa_sql=None,
    include_ortt=True,
):
    empresa_literal = _empresa_literal(empresa_sql)
    opch_literal = _empresa_literal(opch_empresa_sql or empresa_sql)
    obpl_literal = _empresa_literal(obpl_empresa_sql or empresa_sql)

    specs = []
    if include_ortt:
        specs.append(
            {
                "table_name": "ORTT",
                "landing_dataset": f"ORTT_{alias}",
                "sql_query": "SELECT * FROM dbo.ORTT WITH(NOLOCK)",
            }
        )

    specs.extend(
        [
            {
                "table_name": "OFRC",
                "landing_dataset": f"OFRC_{alias}",
                "sql_query": f"SELECT *, {empresa_literal} AS empresaID FROM dbo.OFRC WITH(NOLOCK)",
            },
            {
                "table_name": "OPRC",
                "landing_dataset": f"OPRC_{alias}",
                "sql_query": f"SELECT *, {empresa_literal} AS empresaID FROM dbo.OPRC WITH(NOLOCK)",
            },
            {
                "table_name": "OFRT",
                "landing_dataset": f"OFRT_{alias}",
                "sql_query": f"SELECT *, {empresa_literal} AS empresaID FROM dbo.OFRT WITH(NOLOCK)",
            },
            {
                "table_name": "FRC1",
                "landing_dataset": f"FRC1_{alias}",
                "sql_query": f"SELECT *, {empresa_literal} AS empresaID FROM dbo.FRC1 WITH(NOLOCK)",
            },
            {
                "table_name": "OACT",
                "landing_dataset": f"OACT_{alias}",
                "sql_query": f"SELECT *, {empresa_literal} AS empresaID FROM dbo.OACT WITH(NOLOCK)",
            },
            {
                "table_name": "JDT1",
                "landing_dataset": jdt1_landing_dataset or f"JDT1_{alias}",
                "sql_query": (
                    "SELECT Line_ID, Account, ContraAct, Credit, Debit, LineMemo, RefDate, "
                    f"ProfitCode, TransId, Project, DebCred, TransType, BaseRef, {jdt1_bplid_sql} AS BPLId, {empresa_literal} AS empresaID "
                    "FROM dbo.JDT1 WITH(NOLOCK) "
                    "WHERE YEAR(RefDate) >= YEAR(GETDATE()) - 5"
                ),
            },
            {
                "table_name": "OPCH",
                "landing_dataset": f"OPCH_{alias}",
                "sql_query": (
                    "SELECT DocEntry, DocDate, U_nchamado, Comments, CardCode, CardName"
                    + (", Serial" if opch_has_serial else "")
                    + f", {opch_literal} AS empresaID "
                    "FROM dbo.OPCH WITH(NOLOCK) "
                    "WHERE YEAR(DocDate) >= YEAR(GETDATE()) - 5"
                ),
            },
            {
                "table_name": "POR1",
                "landing_dataset": f"POR1_{alias}",
                "sql_query": (
                    "SELECT TrgetEntry, AcctCode, DocEntry, Dscription, TargetType, "
                    f"{empresa_literal} AS empresaID "
                    "FROM dbo.POR1 WITH(NOLOCK) "
                    "WHERE YEAR(DocDate) >= YEAR(GETDATE()) - 5"
                ),
            },
            {
                "table_name": "OPOR",
                "landing_dataset": f"OPOR_{alias}",
                "sql_query": (
                    f"SELECT *, {empresa_literal} AS empresaID "
                    "FROM dbo.OPOR WITH(NOLOCK) "
                    "WHERE YEAR(DocDate) >= YEAR(GETDATE()) - 5"
                ),
            },
            {
                "table_name": "OINV",
                "landing_dataset": f"OINV_{alias}",
                "sql_query": (
                    "SELECT DocEntry, DocDate, U_nchamado, Comments, CardCode, CardName, Serial, "
                    f"{empresa_literal} AS empresaID "
                    "FROM dbo.OINV WITH(NOLOCK) "
                    "WHERE YEAR(DocDate) >= YEAR(GETDATE()) - 5"
                ),
            },
            {
                "table_name": "RDR1",
                "landing_dataset": f"RDR1_{alias}",
                "sql_query": f"SELECT *, {empresa_literal} AS empresaID FROM dbo.RDR1 WITH(NOLOCK)",
            },
            {
                "table_name": "OBPL",
                "landing_dataset": f"OBPL_{alias}",
                "sql_query": (
                    "SELECT BPLId, "
                    "COALESCE(AliasName, (SELECT COALESCE(AliasName, CompnyName) FROM dbo.OADM)) AS AliasName, "
                    f"{obpl_literal} AS empresaID "
                    "FROM dbo.OBPL WITH(NOLOCK) "
                    "WHERE Disabled = 'N'"
                ),
            },
            {
                "table_name": "ORDR",
                "landing_dataset": f"ORDR_{alias}",
                "sql_query": f"SELECT *, {empresa_literal} AS empresaID FROM dbo.ORDR WITH(NOLOCK)",
            },
        ]
    )
    return specs


COMPANIES = [
    {
        "dag_name": "pl_ambipar_carga_SBO_ES",
        "database_name": "SBO_ES",
        "company_alias": "ES",
        "table_specs": _standard_specs(alias="CONTROL_ES", empresa_sql="42", opch_has_serial=True),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_CONTROLPAR",
        "database_name": "SBO_CONTROLPAR",
        "company_alias": "CONTROLPAR",
        "table_specs": _standard_specs(alias="CONTROLPAR", empresa_sql="82", opch_has_serial=True),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_CSAFETY",
        "database_name": "SBO_CSAFETY",
        "company_alias": "CSAFETY",
        "table_specs": _standard_specs(alias="CSAFETY", empresa_sql="'97'", opch_has_serial=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_CTA",
        "database_name": "SBO_CTA",
        "company_alias": "CTA",
        "table_specs": _standard_specs(alias="CTA", empresa_sql="95", opch_has_serial=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_CTANK",
        "database_name": "SBO_CTANK",
        "company_alias": "CTANK",
        "table_specs": _standard_specs(alias="CTANK", empresa_sql="'98'", opch_has_serial=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_DRACARES",
        "database_name": "SBO_DRACARES",
        "company_alias": "DRACARES",
        "table_specs": _standard_specs(alias="DRACARES", empresa_sql="'89'", opch_has_serial=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_EKMAN",
        "database_name": "SBO_EKMAN",
        "company_alias": "EKMAN",
        "table_specs": _standard_specs(alias="EKMAN", empresa_sql="'A8'", opch_has_serial=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_EMERGENCIA",
        "database_name": "SBO_3GA",
        "company_alias": "EMERGENCIA",
        "table_specs": _standard_specs(alias="EMERGENCIA", empresa_sql="90", opch_has_serial=True, jdt1_bplid_sql="1", include_ortt=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_EMERGENCIA_ACTIVES",
        "database_name": "SBO_EMERGENCIA",
        "company_alias": "EMERGENCIA_ACTIVES",
        "table_specs": [
            {
                "table_name": "ORTT",
                "landing_dataset": "ORTT_EMERGENCIA_ACTIVES",
                "sql_query": "SELECT * FROM dbo.ORTT WITH(NOLOCK)",
            },
            {
                "table_name": "ACTIVES",
                "landing_dataset": "ACTIVES_EMERGENCIA",
                "sql_query": """
                    SELECT 
                        cliente.CardCode AS PN,
                        cliente.CardName AS Cliente,
                        assunto.Name AS Atividade,
                        OUSR.U_NAME AS Departamento,
                        FORMAT(atv.Recontact, 'dd/MM/yyyy') AS DataIncio,
                        FORMAT(atv.EndDate, 'dd/MM/yyyy') AS DataFim
                    FROM dbo.OCLG atv
                    INNER JOIN dbo.OCRD cliente 
                        ON atv.CardCode = cliente.CardCode
                    INNER JOIN dbo.OUSR 
                        ON atv.AttendUser = OUSR.USERID
                    LEFT JOIN dbo.OCLS assunto 
                        ON atv.CntctSbjct = assunto.Code
                    WHERE CAST(atv.Recontact AS date) = DATEADD(DAY, -1, CAST(GETDATE() AS date))
                      AND atv.AttendUser IN (390, 391)
                """.strip(),
            }
        ],
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_FENIX",
        "database_name": "SBO_FENIX",
        "company_alias": "FENIX",
        "table_specs": _standard_specs(alias="FENIX", empresa_sql="75", opch_has_serial=True),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_FLYONE",
        "database_name": "SBO_FLYONE",
        "company_alias": "FLYONE",
        "table_specs": _standard_specs(alias="FLYONE", empresa_sql="88", opch_has_serial=True),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_GEOCIENCIAS",
        "database_name": "SBO_GEOCIENCIAS",
        "company_alias": "GEOCIENCIAS",
        "table_specs": _standard_specs(alias="GEOCIENCIAS", empresa_sql="71", opch_has_serial=True),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_GEOWEB",
        "database_name": "SBO_GEOWEB",
        "company_alias": "GEOWEB",
        "table_specs": _standard_specs(alias="GEOWEB", empresa_sql="70", opch_has_serial=True),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_GIRASSOL",
        "database_name": "SBO_GIRASSOL",
        "company_alias": "GIRASSOL",
        "table_specs": _standard_specs(alias="GIRASSOL", empresa_sql="'A6'", opch_has_serial=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_INSURANCE",
        "database_name": "SBO_INSURANCE",
        "company_alias": "INSURANCE",
        "table_specs": _standard_specs(alias="INSURANCE", empresa_sql="24", opch_has_serial=True, include_ortt=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_JM_SOLUCOES",
        "database_name": "SBO_JM",
        "company_alias": "JM",
        "table_specs": _standard_specs(alias="JM_SOLUCOES", empresa_sql="7", opch_has_serial=True),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_JM_LOC",
        "database_name": "SBO_JM_LOC",
        "company_alias": "JM_LOC",
        "table_specs": _standard_specs(alias="JM_LOC", empresa_sql="8", opch_has_serial=True, obpl_empresa_sql="6", include_ortt=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_MDTECH",
        "database_name": "SBO_MDTECH",
        "company_alias": "MDTECH",
        "table_specs": _standard_specs(alias="MDTECH", empresa_sql="9", opch_has_serial=True),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_MULTIFILIAL_3GA",
        "database_name": "SBO_MULTIFILIAL_3GA",
        "company_alias": "MULTIFILIAL_3GA",
        "table_specs": _standard_specs(alias="EMERGENCIA_SA", empresa_sql="91", opch_has_serial=True, jdt1_bplid_sql="1", include_ortt=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_OGTEC",
        "database_name": "SBO_OGTEC",
        "company_alias": "OGTEC",
        "table_specs": _standard_specs(alias="OGTEC", empresa_sql="69", opch_has_serial=True),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_ORBITGEO",
        "database_name": "SBO_ORBITGEO",
        "company_alias": "ORBITGEO",
        "table_specs": _standard_specs(alias="ORBITGEO", empresa_sql="72", opch_has_serial=True),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_PLIMSOL",
        "database_name": "SBO_PLIMSOL",
        "company_alias": "PLIMSOL",
        "table_specs": _standard_specs(alias="PLIMSOL", empresa_sql="'A7'", opch_has_serial=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_RESPONSE",
        "database_name": "SBO_EMERGENCIA",
        "company_alias": "RESPONSE",
        "table_specs": _standard_specs(
            alias="RESPONSE",
            empresa_sql="3",
            opch_has_serial=False,
            include_ortt=True,
            jdt1_bplid_sql="1",
        ),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_3GA",
        "database_name": "SBO_3GA",
        "company_alias": "3GA",
        "table_specs": _standard_specs(alias="3GA", empresa_sql="3", opch_has_serial=False, include_ortt=True, jdt1_bplid_sql="1"),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_RGCBA",
        "database_name": "SBO_RG_CBA",
        "company_alias": "RG_CBA",
        "table_specs": _standard_specs(alias="RGCBA", empresa_sql="'84'", opch_has_serial=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_RGSP",
        "database_name": "SBO_RG_SP",
        "company_alias": "RG_SP",
        "table_specs": _standard_specs(alias="RGSP", empresa_sql="'85'", opch_has_serial=False),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_SEGMIR",
        "database_name": "SBO_SEGMIR",
        "company_alias": "SEGMIR",
        "table_specs": _standard_specs(alias="SEGMIR", empresa_sql="22", opch_has_serial=False, jdt1_bplid_sql="1"),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_SERMAGE",
        "database_name": "SBO_SERMAGE",
        "company_alias": "SERMAGE",
        "table_specs": _standard_specs(alias="SERMAGE", empresa_sql="78", opch_has_serial=True),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_WWCONTROL",
        "database_name": "SBO_WWCONTROL",
        "company_alias": "WWCONTROL",
        "table_specs": _standard_specs(alias="WWCONTROL", empresa_sql="81", opch_has_serial=True),
    },
    {
        "dag_name": "pl_ambipar_carga_SBO_ZENITH",
        "database_name": "SBO_ZENITH",
        "company_alias": "ZENITH",
        "table_specs": _standard_specs(alias="ZENITH", empresa_sql="'C7'", opch_has_serial=False),
    },
]
