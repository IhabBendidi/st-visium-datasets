import dataclasses as dc
import os
import typing as tp
from pathlib import Path

import datasets

from st_visium_datasets.base import VisiumDatasetBuilderConfig, gen_builder_configs
from st_visium_datasets.builder import build_spots_dataset


@dc.dataclass(kw_only=True)
class VisiumDatasetInfo(datasets.DatasetInfo):
    number_of_spots_under_tissue: int = 0
    number_of_genes_detected: int = 0


class VisiumDatasetBuilder(datasets.GeneratorBasedBuilder):
    BUILDER_CONFIG_CLASS = VisiumDatasetBuilderConfig
    BUILDER_CONFIGS = list(gen_builder_configs())
    DEFAULT_CONFIG_NAME = "all"

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

    def _info(self):
        self.config: VisiumDatasetBuilderConfig
        number_of_spots_under_tissue = sum(
            [vc.number_of_spots_under_tissue for vc in self.config.visium_configs]
        )
        number_of_genes_detected = sum(
            [vc.number_of_genes_detected for vc in self.config.visium_configs]
        )
        return VisiumDatasetInfo(
            license="Creative Commons",
            number_of_spots_under_tissue=number_of_spots_under_tissue,
            number_of_genes_detected=number_of_genes_detected,
        )

    def _split_generators(self, dl_manager: datasets.DownloadManager):
        datasets_urls = {
            vc.name: {
                "tiff_img_path": vc.image_tiff,
                "feature_bc_matrix_dir": vc.feature_barcode_matrix_filtered,
                "spatial_dir": vc.spatial_imaging_data,
            }
            for vc in self.config.visium_configs
        }
        dataset_paths: dict = dl_manager.download_and_extract(datasets_urls)
        dataset_dirs = []
        for name, paths in dataset_paths.items():
            config = self.config.visium_configs[name]
            dataset_dir = build_spots_dataset(
                config,
                Path(self._cache_datasets_dir),
                spot_diameter_px=self._spot_diameter_px,
                pil_resize_longest=self._pil_resize_longest,
                **paths,
            )
            dataset_dirs.append(dataset_dir)
        return [
            datasets.SplitGenerator(
                name="default",
                gen_kwargs={"dataset_dirs": dataset_dirs},
            )
        ]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"
