SENSUM_CONFIG = [
    {
        "name": "aktive_indsatser_alt",
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
        "name": "aktive_sager_alt",
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
        "filter": ["Status", "Igangværende"]
    },
    {
        "name": "ydelse_alt",
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
        "merge_on": ["AfdelingId"]
    },
    {
        "name": "indsats_fordeling_alt",
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
