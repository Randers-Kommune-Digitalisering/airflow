import pandas as pd

from dag_affald.affald_data import _apply_customer_and_material_view, sheet_specs_requires_carrier


def test_sheet_specs_requires_carrier_true_with_current_config() -> None:
    # Current SHEET_SPECS include per-material-group carrier filters.
    assert sheet_specs_requires_carrier() is True


def test_apply_view_normalizes_whitespace_for_matching() -> None:
    df = pd.DataFrame(
        [
            {
                "CustomerName": "Randers",
                "ArticleNumber": "1000",
                "CarrierName": "Marius   Pedersen  A/S   -  Restaffald",
                "year_month": "2026-01",
                "weightnet_sum": 123.0,
            }
        ]
    )

    out = _apply_customer_and_material_view(
        df=df,
        customer_names=["  Randers  "],
        carrier_names=["Marius Pedersen A/S - Restaffald"],
        customer_label=None,
        carrier_label=None,
        material_groups=None,
        drop_unmapped_group_rows=False,
    )

    assert len(out) == 1
    assert out.iloc[0]["CustomerName"] == "Randers"
    assert out.iloc[0]["CarrierName"] == "Marius Pedersen A/S - Restaffald"
