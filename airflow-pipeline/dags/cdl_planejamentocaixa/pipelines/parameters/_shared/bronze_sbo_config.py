BRONZE_TABLE_SPECS = [
    {"table_name": "ORTT"},
    {"table_name": "OFRC"},
    {"table_name": "OFRT"},
    {"table_name": "OPRC"},
    {"table_name": "FRC1"},
    {"table_name": "OACT"},
    {"table_name": "JDT1"},
    {"table_name": "OPCH"},
    {"table_name": "POR1"},
    {"table_name": "OPOR"},
    {"table_name": "OINV"},
    {"table_name": "RDR1"},
    {"table_name": "OBPL"},
    {"table_name": "ORDR"},
]


BRONZE_COMPANIES = [
    {"database_name": "SBO_BIOENV", "company_code": "93", "company_alias": "BIOENV"},
    {"database_name": "SBO_CONTROLPAR", "company_code": "2", "company_alias": "CONTROLPAR"},
    {"database_name": "SBO_CSAFETY", "company_code": "97", "company_alias": "CSAFETY"},
    {"database_name": "SBO_CTA", "company_code": "95", "company_alias": "CTA"},
    {"database_name": "SBO_CTANK", "company_code": "98", "company_alias": "CTANK"},
    {"database_name": "SBO_DRACARES", "company_code": "89", "company_alias": "DRACARES"},
    {"database_name": "SBO_ECOSORB", "company_code": "5", "company_alias": "ECOSORB"},
    {"database_name": "SBO_EKMAN", "company_code": "A8", "company_alias": "EKMAN"},
    {"database_name": "SBO_EMERGENCIA", "company_code": "90", "company_alias": "EMERGENCIA"},
    {"database_name": "SBO_ES", "company_code": "93", "company_alias": "ES"},
    {"database_name": "SBO_FENIX", "company_code": "75", "company_alias": "FENIX"},
    {"database_name": "SBO_FLYONE", "company_code": "88", "company_alias": "FLYONE"},
    {"database_name": "SBO_GEOCIENCIAS", "company_code": "71", "company_alias": "GEOCIENCIAS"},
    {"database_name": "SBO_GEOWEB", "company_code": "70", "company_alias": "GEOWEB"},
    {"database_name": "SBO_GIRASSOL", "company_code": "A6", "company_alias": "GIRASSOL"},
    {"database_name": "SBO_INSURANCE", "company_code": "24", "company_alias": "INSURANCE"},
    {"database_name": "SBO_JM", "company_code": "7", "company_alias": "JM"},
    {"database_name": "SBO_JM_LOC", "company_code": "8", "company_alias": "JM_LOC"},
    {"database_name": "SBO_MDTECH", "company_code": "9", "company_alias": "MDTECH"},
    {"database_name": "SBO_MULTIFILIAL_3GA", "company_code": "91", "company_alias": "MULTIFILIAL_3GA"},
    {"database_name": "SBO_OGTEC", "company_code": "69", "company_alias": "OGTEC"},
    {"database_name": "SBO_ORBITGEO", "company_code": "72", "company_alias": "ORBITGEO"},
    {"database_name": "SBO_PLIMSOL", "company_code": "A7", "company_alias": "PLIMSOL"},
    {"database_name": "SBO_RESISOLUTION", "company_code": "22", "company_alias": "RESISOLUTION"},
    {"database_name": "SBO_RG_CBA", "company_code": "84", "company_alias": "RG_CBA"},
    {"database_name": "SBO_RG_SP", "company_code": "85", "company_alias": "RG_SP"},
    {"database_name": "SBO_SEGMIR", "company_code": "22", "company_alias": "SEGMIR"},
    {"database_name": "SBO_SERMAGE", "company_code": "78", "company_alias": "SERMAGE"},
    {"database_name": "SBO_WWCONTROL", "company_code": "81", "company_alias": "WWCONTROL"},
    {"database_name": "SBO_ZENITH", "company_code": "C7", "company_alias": "ZENITH"},
]


def find_company(company_alias):
    for company in BRONZE_COMPANIES:
        if company["company_alias"] == company_alias:
            return company
    raise KeyError(f"Company alias not found: {company_alias}")
