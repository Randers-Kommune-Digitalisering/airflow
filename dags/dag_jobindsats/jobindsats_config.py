JOBINDSATS_CONFIG = [
    {
        "name": "Ledighed: Faktiske antal ledige og fuldtidsledige",
        "years_back": 5,
        "dataset": "y25i01",
        "period_format": "M",
        "data_to_get": {
        }
    },
    {
        "name": "Ydelsesmodtagere med løntimer",
        "years_back": 2,
        "dataset": "otij01",
        "period_format": "M",
        "data_to_get": {
            "_ygrp_j01": [
                "Kontanthjælp",
                "Jobafklaringsforløb",
                "Forrevalidering og revalidering",
                "Ressourceforløb"
            ],
            "_maalgrp": [
                "Jobparate mv."
            ]
        }
    },
    {
        "name": "Offentligt forsørgede",
        "years_back": 2,
        "dataset": "ptv_a02",
        "period_format": "M",
        "data_to_get": {
            "_ygrpa02": [
                "Førtidspension",
                "Efterløn",
                "Seniorpension",
                "Jobafklaringsforløb",
                "Tidlig pension"
            ],
            "_kon": [
                "Kvinder",
                "Mænd"
            ]
        }
    },
    {
        "name": "Tilbud og samtaler",
        "years_back": 2,
        "dataset": "ptvc01",
        "period_format": "M",
        "data_to_get": {
            "_ygrpc02": [
                "A-dagpenge",
                "Kontanthjælp",
                "Sygedagpenge"
            ]
        }
    },
    {
        "name": "A-Dagpenge",
        "years_back": 5,
        "dataset": "y01a02",
        "period_format": "M",
        "data_to_get": {}
    },
    {
        "name": "Revalidering",
        "years_back": 5,
        "dataset": "y04a02",
        "period_format": "M",
        "data_to_get": {}
    },
    {
        "name": "Sygedagpenge",
        "years_back": 5,
        "dataset": "y07a02",
        "period_format": "M",
        "data_to_get": {}
    },
    {
        "name": "Fleksjob",
        "years_back": 5,
        "dataset": "y08a02",
        "period_format": "M",
        "data_to_get": {}
    },
    {
        "name": "Ledighedsydelse",
        "years_back": 5,
        "dataset": "y09a02",
        "period_format": "M",
        "data_to_get": {}
    },
    {
        "name": "Tilbagetrækningsydelser",
        "years_back": 5,
        "dataset": "y10a02",
        "period_format": "M",
        "data_to_get": {}
    },
    {
        "name": "Ressourceforløb",
        "years_back": 5,
        "dataset": "y11a02",
        "period_format": "M",
        "data_to_get": {}
    },
    {
        "name": "Jobafklaringsforløb",
        "years_back": 5,
        "dataset": "y12a02",
        "period_format": "M",
        "data_to_get": {}
    },
    {
        "name": "Kontanthjælp",
        "years_back": 5,
        "dataset": "y60a02",
        "period_format": "M",
        "data_to_get": {}
    },
    {
        "name": "Barselsdagpenge",
        "years_back": 5,
        "dataset": "y40a02",
        "period_format": "M",
        "data_to_get": {}
    },
    {
        "name": "Fra ydelse til job",
        "years_back": 2,
        "dataset": "y14d03",
        "period_format": "QMAT",
        "data_to_get": {
            "_ygrpmm12": [
                "A-dagpenge",
                "Kontanthjælp",
                "Sygedagpenge"
            ],
            "_tilbud_d03": [
                "Privat virksomhedspraktik",
                "Offentlig virksomhedspraktik"
            ],
            "_maalgrp_d03": [
                "Jobparate mv.",
                "Aktivitetsparate mv."
            ]
        }
    },
    {
        "name": "Ydelsesgrupper",
        "years_back": 4,
        "dataset": "y30r21",
        "period_format": "QMAT",
        "data_to_get": {
            "_ygrp_y30r21": [
                "Ydelsesgrupper i alt",
                "A-dagpenge mv.",
                "Sygedagpenge mv.",
                "Kontanthjælp mv."
            ]
        }
    },
    {
        "name": "Sygedagpenge",
        "years_back": 5,
        "dataset": "y07a07",
        "period_format": "M",
        "data_to_get": {
            "area": ["Randers", "Hele landet"]
        }
    }
]
