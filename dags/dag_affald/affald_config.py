
GENBRUGSPLADSEN_CUSTOMER_NAMES: list[str] = [
    "Genbrugspladsen Øster Tørslev",
    "Genbrugspladsen Langå",
    "Afl.plads private husstande",
    "Genbrugspladsen Asferg",
]

SHEET_SPECS = [
    # Data from Genbrugspladsen
    {
        "articles": ["4206"],
        "sheet_name": "Pap",
        "title": "Pap til genbrug - mængder genbrugspladser hele året",
        "customer_names": GENBRUGSPLADSEN_CUSTOMER_NAMES,
    },
    {
        "articles": ["4019"],
        "sheet_name": "Træ til genbrug",
        "title": "Træ til genbrug - mængder genbrugspladser hele året",
        "customer_names": GENBRUGSPLADSEN_CUSTOMER_NAMES,
    },
    {
        "articles": ["6014", "6015", "6019"],
        "sheet_name": "Beton og tegl",
        "title": "Beton og tegl - mængder genbrugspladser hele året",
        "customer_names": GENBRUGSPLADSEN_CUSTOMER_NAMES,
    },
    {
        "articles": ["2005", "7007", "1001", "2029"],
        "sheet_name": "Forbrænding sortering",
        "title": "Forbrænding sortering - mængder genbrugspladser hele året",
        "customer_names": GENBRUGSPLADSEN_CUSTOMER_NAMES,
    },
    {
        "articles": ["2", "7008"],
        "sheet_name": "Deponi Genbrugspladsen",
        "title": "Deponi - mængder genbrugspladser hele året",
        "customer_names": GENBRUGSPLADSEN_CUSTOMER_NAMES,
    },
    {
        "articles": ["16"],
        "sheet_name": "Asbest",
        "title": "Asbest - mængder genbrugspladser hele året",
        "customer_names": GENBRUGSPLADSEN_CUSTOMER_NAMES,
    },
]
