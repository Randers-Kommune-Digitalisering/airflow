
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
        "customer_order": [
            "Afl.plads private husstande",
            "Genbrugspladsen Langå",
            "Genbrugspladsen Asferg",
            "Genbrugspladsen Øster Tørslev",
        ]
    },
    {
        "articles": ["4019"],
        "sheet_name": "Træ til genbrug",
        "title": "Træ til genbrug - mængder genbrugspladser hele året",
        "customer_names": GENBRUGSPLADSEN_CUSTOMER_NAMES,
        "customer_order": [
            "Afl.plads private husstande",
            "Genbrugspladsen Langå",
            "Genbrugspladsen Asferg",
            "Genbrugspladsen Øster Tørslev",
        ]
    },
    {
        "articles": ["6014", "6015", "6019"],
        "sheet_name": "Beton og tegl",
        "title": "Beton og tegl - mængder genbrugspladser hele året",
        "customer_names": GENBRUGSPLADSEN_CUSTOMER_NAMES,
        "customer_order": [
            "Afl.plads private husstande",
            "Genbrugspladsen Langå",
            "Genbrugspladsen Asferg",
            "Genbrugspladsen Øster Tørslev",
        ]
    },
    {
        "articles": ["2005", "7007", "1001", "2029"],
        "sheet_name": "Forbrænding sortering",
        "title": "Forbrænding sortering - mængder genbrugspladser hele året",
        "customer_names": GENBRUGSPLADSEN_CUSTOMER_NAMES,
        "customer_order": [
            "Afl.plads private husstande",
            "Genbrugspladsen Langå",
            "Genbrugspladsen Asferg",
            "Genbrugspladsen Øster Tørslev",
        ],
        "row_label_mode": "legacy",
        "ton_label_mode": "legacy",
        "auto_append_vare_nr": False,

        "material_groups": [
            {"articles": ["2005", "7007"], "label": "2005, 7007"},
        ],
    },
    {
        "articles": ["2", "7008", "46", "26"],
        "sheet_name": "Deponi Genbrugspladsen",
        "title": "Deponi - mængder genbrugspladser hele året",
        "customer_names": GENBRUGSPLADSEN_CUSTOMER_NAMES,
        "customer_order": [
            "Afl.plads private husstande",
            "Genbrugspladsen Langå",
            "Genbrugspladsen Asferg",
            "Genbrugspladsen Øster Tørslev",
        ],
        "row_label_mode": "legacy",
        "ton_label_mode": "legacy",
        "auto_append_vare_nr": False,
        "material_groups": [
            {"articles": ["2", "7008"], "label": "2, 7008"},
        ],
    },
    {
        "articles": ["16"],
        "sheet_name": "Asbest",
        "title": "Asbest - mængder genbrugspladser hele året",
        "customer_names": GENBRUGSPLADSEN_CUSTOMER_NAMES,
        "customer_order": [
            "Afl.plads private husstande",
            "Genbrugspladsen Langå",
            "Genbrugspladsen Asferg",
            "Genbrugspladsen Øster Tørslev",
        ]
    },
    # Data from Affaldsterminalen
    {
        "sheet_name": "Hård plast til sortering",
        "title": "Hård plast - mængder Affaldsterminalen hele året",
        "row_label_mode": "generic",
        "ton_label_mode": "generic",
        "sort_mode": "material_then_year",
        "blank_between": "material",
        "customer_names": None,
        "customer_label": "Randers",
        "material_groups": [
            {"articles": ["4200", "4209", "4211", "4064"], "label": "Hård plast til sortering"},
            {"articles": ["4233"], "label": "Hård plast direkte fra GP og til DAM"},
        ],
    },
    {
        "sheet_name": "Organisk affald",
        "title": "Organisk affald - mængder Affaldsterminalen hele året",
        "row_label_mode": "generic",
        "ton_label_mode": "generic",
        "sort_mode": "material_then_year",
        "blank_between": "material",
        "customer_names": None,
        "customer_label": "Randers",
        "material_groups": [{"articles": ["1500"], "label": "Organisk affald"}],
    },
    {
        "sheet_name": "MGP Sortering",
        "title": "MGP-mængder til sortering fra indsamlingsordningerne og genbrugspladserne - Affaldsterminalen - hele året",
        "row_label_mode": "generic",
        "ton_label_mode": "generic",
        "sort_mode": "material_then_year",
        "blank_between": "material",
        "customer_names": None,
        "customer_label": "Randers",
        "material_groups": [
            {"articles": ["4006", "4004", "4009"], "label": " Metal og Glas"},
            {"articles": ["4005", "4007"], "label": "Plast og MDK"}
        ],
    },
    {
        "sheet_name": "Omlasten",
        "title": "Forbrændingsmængder i Omlasten Affaldsterminalen - hele året",
        "row_label_mode": "generic",
        "ton_label_mode": "generic",
        "sort_mode": "material_then_year",
        "blank_between": "material",
        "customer_names": None,
        "customer_label": "Randers",
        "material_order": [
            "Restaffald til forbrænding (vare nr. 1000)",
            "Storskrald fra genbrugsplads (vare nr. 1001)",
            "Erhvervsaffald til forbrænding (vare nr. 1002)"
        ],
        "material_groups": [
            {"articles": ["1000"], "label": "Restaffald til forbrænding"},
            {"articles": ["1001"], "label": "Storskrald fra genbrugsplads"},
            {"articles": ["1002"], "label": "Erhvervsaffald til forbrænding"},
        ],
    },
    {
        "sheet_name": "Meldep",
        "title": "Forbrændingsmængder til Mel-Dep Affaldsterminalen - hele året",
        "row_label_mode": "generic",
        "ton_label_mode": "generic",
        "sort_mode": "material_then_year",
        "blank_between": "material",
        "customer_names": None,
        "customer_label": "Randers",
        "material_order": [
            "Storskrald til forbrænding (vare nr. 2005, 7005, 7111)",
            "Erhvervsaffald til forbrænding (vare nr. 2006)",
        ],
        "material_groups": [
            {"articles": ["2005", "7005", "7111"], "label": " Storskrald til forbrænding"},
            {"articles": ["2006"], "label": "Erhvervsaffald til forbrænding"},
        ],
    },
    {
        "sheet_name": "Deponi Affaldsterminalen",
        "title": "Deponimængder Affaldsterminalen - hele året",
        "row_label_mode": "generic",
        "ton_label_mode": "generic",
        "sort_mode": "material_then_year",
        "blank_between": "material",
        "customer_names": None,
        "customer_label": "Randers",
        "material_groups": [
            {"articles": ["2", "3", "5", "14", "7110", "46", "4063"], "label": " Blandet Deponi"},
            {"articles": ["6", "16", "7", "36"], "label": "Mineralsk Deponi "},
            {"articles": ["26"], "label": "Asbestfri fra genbrugsplads "},
        ],
    },
    # Data from Indsamlingsmængder
    {
        "sheet_name": "Restaffald",
        "title": "Indsamlingsmængder Restaffald - hele året",
        "row_label_mode": "generic",
        "ton_label_mode": "generic",
        "sort_mode": "material_then_year",
        "blank_between": "material",
        "customer_names": None,
        "customer_label": "Randers",
        "drop_unmapped_group_rows": True,  # drop rest rows for articles listed in material_groups that remain unmapped after applying per-group customer/carrier filters
        "material_groups": [
            {
                "articles": ["1000"],
                "label": "MP -undergrund Affaldsterminalen",
                "carrier_names": [
                    "Joca Trading A/S",
                    "Marius Pedersen A/S Restaffald Undergrund",
                ],
            },
            {
                "articles": ["1000"],
                "label": "Små containere Affaldsterminalen",
                "carrier_names": [
                    "Marius Pedersen A/S - Restaffald",
                ],
            },
        ],
    },
    {
        "sheet_name": "Madaffald",
        "title": "Indsamlingsmængder Madaffald - hele året",
        "row_label_mode": "generic",
        "ton_label_mode": "generic",
        "sort_mode": "material_then_year",
        "blank_between": "material",
        "customer_names": None,
        "customer_label": "Randers",
        "drop_unmapped_group_rows": True,
        "material_groups": [
            {
                "articles": ["1500"],
                "label": "MP -undergrund Affaldsterminalen",
                "carrier_names": [
                    "Joca Trading A/S",
                    "Marius Pedersen A/S Madaffald Undergrund",
                ],
            },
            {
                "articles": ["1500"],
                "label": "Små containere Affaldsterminalen",
                "carrier_names": [
                    "Marius Pedersen A/S - Madaffald",
                ],
            },
        ],
    },
    {
        "sheet_name": "Genbrug",
        "title": "Indsamlingsmængder Genbrug - hele året",
        "row_label_mode": "generic",
        "ton_label_mode": "generic",
        "sort_mode": "material_then_year",
        "blank_between": "material",
        "customer_names": None,
        "customer_label": "Randers",
        "drop_unmapped_group_rows": True,
        "material_groups": [
            {
                "articles": ["3100", "3101"],
                "label": "Papir/Pap: MP undergrund Affaldsterminalen",
                "carrier_names": [
                    "Marius Pedersen A/S Papir/Pap Undergrund",
                ],
            },
            {
                "articles": ["3100", "3101"],
                "label": "Papir/Pap: Små containere Affaldsterminalen",
                "carrier_names": [
                    "Marius Pedersen A/S - Pap & papir",
                ],
            },
            {
                "articles": ["4004"],
                "label": "Glas/Metal: MP undergrund Affaldsterminalen",
                "carrier_names": [
                    "Marius Pedersen A/S Glas/Metal Undergrund",
                ],
            },
            {
                "articles": ["4004"],
                "label": "Glas/Metal: Små containere Affaldsterminalen",
                "carrier_names": [
                    "Marius Pedersen  A/S - Glas & Metal emballage",
                ],
            },
            {
                "articles": ["4005"],
                "label": "Plast/MDK: MP undergrund Affaldsterminalen",
                "carrier_names": [
                    "Marius Pedersen A/S Plast/MDK Undergrund",
                ],
            },
            {
                "articles": ["4005"],
                "label": "Plast/MDK: Små containere Affaldsterminalen",
                "carrier_names": [
                    "Marius Pedersen A/S - Plast & Mad- drikkekartoner",
                ],
            },
        ],
    },
    {
        "sheet_name": "Storeskrald",
        "title": "Indsamlingsmængder Storeskrald - hele året",
        "row_label_mode": "generic",
        "ton_label_mode": "generic",
        "sort_mode": "material_then_year",
        "blank_between": "material",
        "customer_names": None,
        "customer_label": "Randers",
        "drop_unmapped_group_rows": True,
        "material_order": [
            "Brændbart (vare nr. 7005)",
            "Jern og Metal (vare nr. 4002)",
            "Elektronik (vare nr. 4061)",
            "Pap - løs fra husstande (vare nr. 4207)",
            "Papir/Pap (vare nr. 3102)",
            "Tekstiler (vare nr. 4220)",
            "Hård plast (vare nr. 4200)",
            "Plastfolie (vare nr. 4062)",
            "Træ til genbrug(Del af 4019 indtil 2026) (vare nr. 4019)",
            "Træ til genbrug(Ny) (vare nr. 4067)",
            "Trykimprægneret træ (vare nr. 4066)",
            "Deponi(Del af 2 indtil 2026) (vare nr. 2)",
            "Deponi(Ny) (vare nr. 4063)",
            "Flamingo(Del af 2004 indtil 2026) (vare nr. 2004)",
            "Flamingo(Ny) (vare nr. 4065)",
            ""
        ],
        "material_groups": [
            {
                "articles": ["7005"],
                "label": "Brændbart",
            },
            {
                "articles": ["4002"],
                "label": "Jern og Metal",
            },
            {
                "articles": ["4061"],
                "label": "Elektronik",
            },
            {
                "articles": ["4207"],
                "label": "Pap - løs fra husstande",
            },
            {
                "articles": ["4220"],
                "label": "Tekstiler",
            },
            {
                "articles": ["4200"],
                "label": "Hård plast",
                "customer_names": ["EHJ Energi & Miljø A/S  Hård Plast Storskrald"],
            },
            {
                "articles": ["4062"],
                "label": "Plastfolie",
            },
            {
                "articles": ["4019"],
                "label": "Træ til genbrug(Del af 4019 indtil 2026)",
                "customer_names": ["EHJ Energi & Miljø A/S  Træ Storskrald"],
            },
            {
                "articles": ["4067"],
                "label": "Træ til genbrug(Ny)",
                "customer_names": ["EHJ Energi & Miljø A/S  Træ Storskrald"],
            },
            {
                "articles": ["2"],
                "label": "Deponi(Del af 2 indtil 2026)",
                "customer_names": ["EHJ Energi & Miljø A/S  Deponi Storskrald"],
            },
            {
                "articles": ["4063"],
                "label": "Deponi(Ny)",
                "customer_names": ["EHJ Energi & Miljø A/S  Deponi Storskrald"],
            },
            {
                "articles": ["2004"],
                "label": "Flamingo(Del af 2004 indtil 2026)",
                "customer_names": ["EHJ Energi & Miljø A/S  Flamingo Storskrald"],
            },
            {
                "articles": ["4065"],
                "label": "Flamingo(Ny)",
                "customer_names": ["EHJ Energi & Miljø A/S  Flamingo Storskrald"],
            },
            {
                "articles": ["3102"],
                "label": "Papir/Pap",
            },
            {
                "articles": ["4066"],
                "label": "Trykimprægneret træ",
            },
        ],
    },

]
