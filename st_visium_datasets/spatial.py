from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd

from st_visium_datasets.utils import get_nested_filepath


def load_scale_factors(spatial_dir: Path | str) -> dict[str, float]:
    filename = "scalefactors_json.json"
    scale_factors_path = get_nested_filepath(spatial_dir, filename)
    with open(scale_factors_path, mode="r") as f:
        return json.load(f)


def get_spot_diameter_px(spatial_dir: Path | str) -> int:
    """Return spot diameter in pixels"""
    scale_factors = load_scale_factors(spatial_dir)
    return math.ceil(scale_factors["spot_diameter_fullres"])


def get_tissue_positions_df(spatial_dir: Path | str) -> pd.DataFrame:
    """Return spots.tsv.gz as dataframe"""
    filenames = ["tissue_positions_list.csv", "tissue_positions.csv"]
    kwargs_list = [
        {
            "names": [
                "barcode",
                "in_tissue",
                "array_row",
                "array_col",
                "pxl_row_in_fullres",
                "pxl_col_in_fullres",
            ],
        },
        {"header": 0},
    ]
    for filename, kwargs in zip(filenames, kwargs_list):
        try:
            filepath = get_nested_filepath(spatial_dir, filename)
            df: pd.DataFrame = pd.read_csv(filepath, **kwargs)
            break
        except FileNotFoundError:
            continue
    else:
        raise FileNotFoundError(f"no {filenames} found in {spatial_dir}")

    df = df.rename(columns={"pxl_row_in_fullres": "y", "pxl_col_in_fullres": "x"})
    df = df[df["in_tissue"] == 1].drop(columns=["in_tissue", "array_row", "array_col"])
    df = df.set_index("barcode")
    return df
