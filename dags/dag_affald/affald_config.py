
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
        "articles": ["2", "7008", "46"],
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
        "articles": ["16", "26", "46"],
        "sheet_name": "Asbest",
        "title": "Asbest - mængder genbrugspladser hele året",
        "customer_names": GENBRUGSPLADSEN_CUSTOMER_NAMES,
        "customer_order": [
            "Afl.plads private husstande",
            "Genbrugspladsen Langå",
            "Genbrugspladsen Asferg",
            "Genbrugspladsen Øster Tørslev",
        ],
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
            {"articles": ["2", "3", "5", "14", "7110", "4063"], "label": " Blandet Deponi"},
            {"articles": ["6", "16", "7", "36", "46"], "label": "Mineralsk Deponi "},
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
        "sheet_name": "Producentansvar",
        "title": "Indsamlingsmængder Storeskrald - hele året",
        "row_label_mode": "generic",
        "ton_label_mode": "generic",
        "sort_mode": "material_then_year",
        "blank_between": "material",
        "customer_names": None,
        "customer_label": "Randers",
        "drop_unmapped_group_rows": True,
        "material_order": [
            "Ren pap fraktion fra genbrugspladserne (vare nr. 4206, 4207)",
            "Fraført Ren Pap (vare nr. 4259)",
            "Indsamlings Papir/pap (vare nr. 3101, 3100)",
            "Genbrugsplads Papir/pap (vare nr. 3105)",
            "Fraført Papir/pap (vare nr. 3110)",
            "Indsamlings Glas/metal (vare nr. 4004)",
            "Genbrugsplads Glas/metal (vare nr. 4006)",
            "Fraført glas/metal (vare nr. 4112)",
            "Indsamlings plast og MDK (vare nr. 4005)",
            "Genbrugsplads Plast og MDK (vare nr. 4007)",
            "Fraført Plast og MDK (vare nr. 4231)",
            "Indsamlings Plastfolie fra Storskrald (vare nr. 4062)",
            "Genbrugsplads Plastfolie (vare nr. 7004)",
            "Genbrugsplads Bigbags (vare nr. 4222)",
            "Genbrugsplads Flamingo (vare nr. 2004)",
            "Genbrugsplads Plastdunke (vare nr. 4212)",
            "Genbrugsplads Hård Plast (vare nr. 4200)",
            "Restaffald (vare nr. 1000)",
        ],
        "material_groups": [
            {
                "articles": ["4206", "4207"],
                "label": "Ren pap fraktion fra genbrugspladserne",
            },
            {
                "articles": ["4259"],
                "label": "Fraført Ren Pap",
            },
            {
                "articles": ["3101", "3100"],
                "label": "Indsamlings Papir/pap",
            },
            {
                "articles": ["3105"],
                "label": "Genbrugsplads Papir/pap",
            },
            {
                "articles": ["3110"],
                "label": "Fraført Papir/pap",
            },
            {
                "articles": ["4004"],
                "label": "Indsamlings Glas/metal",
            },
            {
                "articles": ["4006"],
                "label": "Genbrugsplads Glas/metal",
            },
            {
                "articles": ["4112"],
                "label": "Fraført glas/metal",
            },
            {
                "articles": ["4005"],
                "label": "Indsamlings plast og MDK",
            },
            {
                "articles": ["4007"],
                "label": "Genbrugsplads Plast og MDK",
            },
            {
                "articles": ["4231"],
                "label": "Fraført Plast og MDK",
            },
            {
                "articles": ["4062"],
                "label": "Indsamlings Plastfolie fra Storskrald",
            },
            {
                "articles": ["7004"],
                "label": "Genbrugsplads Plastfolie",
            },
            {
                "articles": ["4222"],
                "label": "Genbrugsplads Bigbags",
            },
            {
                "articles": ["2004"],
                "label": "Genbrugsplads Flamingo",
            },
            {
                "articles": ["4212"],
                "label": "Genbrugsplads Plastdunke",
            },
            {
                "articles": ["4200"],
                "label": "Genbrugsplads Hård Plast",
            },
            {
                "articles": ["1000"],
                "label": "Restaffald",
            },
            {
                "articles": ["4067"],
                "label": "Træ(inde) fra storskrald",
                "customer_names": ["EHJ Energi & Miljø A/S  Træ Storskrald"],
            },
            {
                "articles": ["4019", "4020"],
                "label": "Træ(inde) fra genbrugspladserne",
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
            "Pap - løs fra husstande(Del af 4207 indtil 2026) (vare nr. 4207)",
            "Pap - løs fra husstande(Ny) (vare nr. 4060)",
            "Papir/Pap (vare nr. 3102)",
            "Tekstiler (vare nr. 4220)",
            "Hård plast(Del af 4200 indtil 2026) (vare nr. 4200)",
            "Hård plast(Ny) (vare nr. 4064)",
            "Plastfolie (vare nr. 4062)",
            "Træ til genbrug(Del af 4019 indtil 2026) (vare nr. 4019)",
            "Træ til genbrug(Ny) (vare nr. 4067)",
            "Trykimprægneret træ (vare nr. 4066)",
            "Deponi(Del af 2 indtil 2026) (vare nr. 2)",
            "Deponi(Ny) (vare nr. 4063)",
            "Flamingo(Del af 2004 indtil 2026) (vare nr. 2004)",
            "Flamingo(Ny) (vare nr. 4065)",
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
                "label": "Pap - løs fra husstande(Del af 4207 indtil 2026)",
            },
            {
                "articles": ["4060"],
                "label": "Pap - løs fra husstande(Ny)",
            },
            {
                "articles": ["3102"],
                "label": "Papir/Pap",
            },
            {
                "articles": ["4220"],
                "label": "Tekstiler",
            },
            {
                "articles": ["4200"],
                "label": "Hård plast(Del af 4200 indtil 2026)",
                "customer_names": ["EHJ Energi & Miljø A/S  Hård Plast Storskrald"],
            },
            {
                "articles": ["4064"],
                "label": "Hård plast(Ny)",
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
                "articles": ["4066"],
                "label": "Trykimprægneret træ",
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
        ],
    },

]
