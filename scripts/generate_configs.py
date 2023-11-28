import logging

import pandas as pd

from st_visium_datasets import DatasetConfig, setup_logging
from st_visium_datasets.utils import sanitize_str, scrap_visium_dataset_homepage

setup_logging()
logger = logging.getLogger(__name__)


def main(html: str):
    dfs = pd.read_html(html, extract_links="body")
    header_df, df = dfs[0], dfs[1]
    df.columns = header_df.columns
    other_cols = df.columns.difference(["Datasets"])
    df[other_cols] = df[other_cols].map(lambda x: x[0])  # type: ignore
    df[["title", "homepage"]] = pd.DataFrame(df["Datasets"].tolist(), index=df.index)
    df = df.drop(
        columns=[
            "Datasets",
            "Products",
            "Chemistry Version",
            "Software",
            "Pipeline Version",
            "Subpipeline",
            "10x Instrument(s)",
            "Publish Date",
        ]
    )
    df = (
        df.replace("None", pd.NA)
        .replace("nan", pd.NA)
        .replace("N/A", pd.NA)
        .replace("", pd.NA)
    )
    df.columns = list(map(sanitize_str, df.columns))
    df = df.dropna(axis=1, how="all")
    for record in df.to_dict(orient="records"):
        url = record.pop("homepage")

        for data in scrap_visium_dataset_homepage(url, **record):  # type: ignore
            try:
                config = DatasetConfig(**data)
            except ValueError as err:
                logger.error(err)
                continue

            config.save(overwrite=True)


if __name__ == "__main__":
    main("./data.html")
