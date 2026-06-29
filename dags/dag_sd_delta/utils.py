
def validate_insts_to_import(insts_to_import: list) -> None:
    """
    Validates the structure of the insts_to_import variable.
    Expected format: List of dicts with 'inst_id' (2-char string) and 'excluded_dept_ids' (list of 4-char strings).
    """
    if not (
        isinstance(insts_to_import, list)
        and all(
            isinstance(item, dict)
            and isinstance(item.get("inst_id"), str)
            and len(item["inst_id"]) == 2
            and isinstance(item.get("excluded_dept_ids"), list)
            and all(
                isinstance(dept_id, str) and len(dept_id) == 4
                for dept_id in item["excluded_dept_ids"]
            )
            for item in insts_to_import
        )
    ):
        raise ValueError(
            "delta_sd_insts_to_import must be a list of {inst_id: <2 chars>, excluded_dept_ids: [<4 chars>, ...]}"
        )
