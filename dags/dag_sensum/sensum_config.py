SENSUM_CONFIG = [
    {
        "name": "aktive_indsatser",
        "dir": "/D:/SFTP-EGDW/sensum_randers",
        "key_col": "IndsatsId",
        "pattern": "Indsatser_*.csv",
        "cols": [
            "IndsatsStartDato",
            "IndsatsStatus",
            "IndsatsSlutDato"
        ]
    },
    {
        "name": "aktive_sager",
        "dir": "/D:/SFTP-EGDW/sensum_randers",
        "key_col": "SagId",
        "pattern": "Sager_*.csv",
        "cols": [
            "SagNavn",
            "SagType",
            "AfdelingNavn",
            "Status"
        ],
        "sec_pattern": "Medarbejder_*.csv",
        "sec_cols": ["Fornavn", "Efternavn"],
        "merge_on": ["MedarbejderId", "PrimærAnsvarligMedarbejderId"],
        "sec_prefix": "Medarbejder",
        "filter": ["Status", "Igangværende"]
    },
    {
        "name": "ydelse",
        "dir": "/D:/SFTP-EGDW/Frem",
        "key_col": "YdelseId",
        "pattern": "Ydelse_*.csv",
        "cols": [
            "YdelseNavn",
            "StartDato",
            "SlutDato"
        ],
        "sec_pattern": "Afdeling_*.csv",
        "sec_cols": ["Navn"],
        "merge_on": ["AfdelingId"],
        "sec_prefix": "Afdeling"
    },
    {
        "name": "indsats_fordeling",
        "dir": "/D:/SFTP-EGDW/sensum_randers",
        "key_col": "IndsatsId",
        "pattern": "Indsatser_*.csv",
        "cols": [
            "IndsatsStatus",
            "Indsats",
            "IndsatsStartDato",
            "IndsatsSlutDato",
            "LeverandørIndsats",
            "LeverandørNavn",
            "IndsatsParagraf"
        ],
        "sec_pattern": "Sager_*.csv",
        "sec_cols": ["AfdelingNavn"],
        "merge_on": ["SagId"]
    }
]
