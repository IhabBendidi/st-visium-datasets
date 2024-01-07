from __future__ import annotations

import json
import logging
import os
import typing as tp
from pathlib import Path

import datasets
import pandas as pd

from st_visium_datasets.base import (
    VisiumDatasetBuilderConfig,
    gen_builder_configs,
)
from st_visium_datasets.builder import build_spots_datasets

logger = logging.getLogger(__name__)


class VisiumDatasetBuilder(datasets.GeneratorBasedBuilder):
    BUILDER_CONFIG_CLASS = VisiumDatasetBuilderConfig
    BUILDER_CONFIGS = list(gen_builder_configs())
    DEFAULT_CONFIG_NAME = "all"
    DEFAULT_SPLIT = "default"

    def __init__(
        self,
        spot_diameter_px: int | tp.Literal["auto"] = "auto",
        pil_resize_longest: int | None = 3840,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._spot_diameter_px = spot_diameter_px
        self._pil_resize_longest = pil_resize_longest
        self._cache_datasets_dir = os.path.join(self._cache_dir_root, "visium_datasets")
        self._dataset_paths: tp.Optional[dict] = None
        self._spot_dataset_dirs: tp.Optional[list[Path]] = None
        self.config: VisiumDatasetBuilderConfig

    @property
    def dataset_paths(self) -> dict:
        if self._dataset_paths is None:
            raise ValueError(
                "Dataset paths are not available. Please run `download_and_prepare` first."
            )
        return self._dataset_paths

    def _build_description(self) -> str:
        base_desc = (
            "Visium datasets for spatial transcriptomics. This dataset is "
            "a collection of the following datasets from 10x "
            "Genomics: {}"
        )
        config_names = [c.name for c in self.config.visium_configs]
        return base_desc.format(", ".join(config_names))

    def _info(self):
        return datasets.DatasetInfo(
            description=self._build_description(),
            features=datasets.Features(
                {
                    "species": datasets.Value("string"),
                    "anatomical_entity": datasets.Value("string"),
                    "disease_state": datasets.Value("string"),
                    "spot_path": datasets.Value("string"),
                    "spot_bytes": datasets.Value(dtype="binary"),
                    "features_path": datasets.Value("string"),
                    "features": datasets.features.Sequence(
                        {
                            "feature_id": datasets.Value("string"),
                            "feature_type": datasets.Value(dtype="string"),
                            "gene": datasets.Value("string"),
                            "count": datasets.Value(dtype="int64"),
                        }
                    ),
                }
            ),
            license="Creative Commons",
        )

    def _split_generators(self, dl_manager: datasets.DownloadManager):
        datasets_urls = {
            vc.name: {
                "tiff": vc.image_tiff,
                "feature_bc_matrix": vc.feature_barcode_matrix_filtered,
                "spatial": vc.spatial_imaging_data,
            }
            for vc in self.config.visium_configs
        }
        logger.info("Downloading dataset files ...")
        self._dataset_paths = dl_manager.download_and_extract(datasets_urls)  # type: ignore
        logger.info("Building spots datasets ...")
        dataset_dirs = build_spots_datasets(
            configs=self.config.visium_configs,
            data_dir=Path(self._cache_datasets_dir),
            dataset_paths=self.dataset_paths,
            spot_diameter_px=self._spot_diameter_px,  # type:ignore
            pil_resize_longest=self._pil_resize_longest,
            overwrite=dl_manager.download_config.force_download
            or dl_manager.download_config.force_extract,
            num_proc=dl_manager.download_config.num_proc,
        )
        return [
            datasets.SplitGenerator(
                name=self.DEFAULT_SPLIT,
                gen_kwargs={"dataset_dirs": dataset_dirs},
            )
        ]

    def _generate_examples(self, dataset_dirs: list[Path]):
        for dataset_dir in dataset_dirs:
            config_path = dataset_dir / "config.json"
            spots_dir = dataset_dir / "spots"
            config = json.loads(config_path.read_text())
            species = config["species"] or "unknown"
            anatomical_entity = config["anatomical_entity"] or "unknown"
            disease_state = config["disease_state"] or "unknown"

            for spot_path in spots_dir.glob("*.npy"):
                features_path = spot_path.with_suffix(".csv")
                barcode = spot_path.stem
                data = {
                    "species": species,
                    "anatomical_entity": anatomical_entity,
                    "disease_state": disease_state,
                    "spot_path": str(spot_path),
                    "features_path": str(features_path),
                    "spot_bytes": spot_path.read_bytes(),
                    "features": pd.read_csv(features_path).to_dict("records"),
                }
                yield barcode, data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"
