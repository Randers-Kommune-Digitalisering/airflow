from dags.dag_sensum.sensum_config import SENSUM_CONFIG


def test_sensum_config_structure():
    for config in SENSUM_CONFIG:
        required_keys = {"name", "dir", "key_col", "pattern", "cols"}
        if not required_keys.issubset(config.keys()):
            missing = required_keys - config.keys()
            raise ValueError(f"Config is missing required keys: {missing}")
        for key, value in config.items():
            if "cols" in key:
                if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                    raise ValueError(f'Config key "{key}" must be a list of strings.')
            elif key == "merge_on":
                if not isinstance(value, list) or not all(isinstance(v, str) for v in value) or len(value) not in {1, 2}:
                    raise ValueError(f'Config key "{key}" must be a list of strings, with length 1 or 2.')
            elif key == "filter":
                if not isinstance(value, list) or len(value) != 2 or not all(isinstance(i, str) for i in value):
                    raise ValueError(f'Config key "{key}" must be a list of two strings.')
            else:
                if not isinstance(value, str):
                    raise ValueError(f'Config key "{key}" must be a string.')
