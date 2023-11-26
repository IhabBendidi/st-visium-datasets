import json
import logging
import math
import typing as tp
from pathlib import Path

import numpy as np
from PIL import Image
from rich.progress import track
from tifffile import TiffFile, imread

from st_atlas_datasets.base import (
    BaseDatasetBuilder,
    BaseDatasetConfig,
    MultiDatasetConfig,
)
from st_atlas_datasets.settings import settings
from st_atlas_datasets.utils import DataFile
from st_atlas_datasets.utils.download_manager import DownloadManager
from st_atlas_datasets.visium.scrapper import scrap_visium_homepage
from st_atlas_datasets.visium.spots_utils import VisiumSpots

logger = logging.getLogger(__name__)


class VisiumDatasetConfig(BaseDatasetConfig):
    # Required
    inputs_image_tiff: DataFile
    outputs_summary_csv: DataFile
    outputs_spatial_imaging_data: DataFile
    outputs_feature_barcode_matrix_hdf5_filtered: DataFile

    visium_dataset_name: str
    liscense: str = "Creative Commons license"
    published_at: str | None = None
    pipeline_version: str | None = None
    product_slug: str | None = None
    software_name: str | None = None

    # Optional for completeness
    inputs_fastqs: DataFile | None = None
    inputs_probe_set: DataFile | None = None
    outputs_genome_aligned_bam: DataFile | None = None
    outputs_genome_aligned_bam_index: DataFile | None = None
    outputs_per_molecule_read_information: DataFile | None = None
    outputs_feature_barcode_matrix_filtered: DataFile | None = None
    outputs_feature_barcode_matrix_hdf5_raw: DataFile | None = None
    outputs_feature_barcode_matrix_raw: DataFile | None = None
    outputs_clustering_analysis: DataFile | None = None
    outputs_spatial_enrichment_moran_s_i: DataFile | None = None
    outputs_summary_html: DataFile | None = None
    outputs_loupe_browser_file: DataFile | None = None

    @classmethod
    def from_dataset_homepage(
        cls, url: str, **overrides
    ) -> "VisiumDatasetConfig | MultiDatasetConfig":
        config_dicts = [{**d, **overrides} for d in scrap_visium_homepage(url)]
        if not config_dicts:
            raise ValueError(f"Could not scrap any dataset from {url}")
        configs = [cls(**d) for d in config_dicts]
        if len(configs) == 1:
            return configs[0]
        visium_dataset_names = [c.visium_dataset_name for c in configs]
        if len(set(visium_dataset_names)) > 1:
            raise NotImplementedError()
        name = visium_dataset_names[0]
        return MultiDatasetConfig(name=name, configs=configs)


class VisiumDatasetBuilder:
    def __init__(
        self,
        config: VisiumDatasetConfig,
        spot_diameter_px: int | tp.Literal["auto"] = "auto",
    ):
        self._config = config
        self._dl_manager = DownloadManager()
        self._spot_diameter_px = spot_diameter_px

        # filled in download()
        self._tiff_path: Path | None = None
        self._summary_path: Path | None = None
        self._feature_barcode_matrix_path: Path | None = None
        self._scalefactors_path: Path | None = None
        self._tissue_positions_path: Path | None = None

        # filled in prepare()
        self._spots: VisiumSpots | None = None
        self._img: np.ndarray | None = None
        self._pil_img: Image.Image | None = None

    @property
    def spot_diameter_px(self) -> int:
        if isinstance(self._spot_diameter_px, (int, float)):
            return int(self._spot_diameter_px)

        if self._spot_diameter_px == "auto":
            if self._scalefactors_path is None:
                raise ValueError(
                    "spot_diameter_px is set to 'auto' but scalefactors file is not "
                    "available, run 'self.download()' first."
                )
            with open(self._scalefactors_path) as f:
                scalefactors = json.load(f)
            self._spot_diameter_px = math.ceil(scalefactors["spot_diameter_fullres"])
            logger.info(
                f"Updating dataset '{self._config.name}' spot diameter from  'auto' "
                f"to {self._spot_diameter_px} px."
            )
            return self._spot_diameter_px

        raise ValueError(self._spot_diameter_px)

    @property
    def spots(self) -> VisiumSpots:
        if self._spots is not None:
            return self._spots

        if not self._tissue_positions_path:
            raise ValueError("Run 'self.download()' first.")

        self._spots = VisiumSpots.from_visium_csv(self._tissue_positions_path)
        return self._spots

    @property
    def img(self) -> np.ndarray:
        if self._img is not None:
            return self._img

        if self._tiff_path is None:
            raise ValueError("Run 'self.download()' first.")

        self._img = imread(self._tiff_path)
        return self._img

    @property
    def pil_image(self) -> Image.Image:
        if self._pil_img is not None:
            return self._pil_img
        self._pil_img = Image.fromarray(self.img, mode="YCbCr").convert("RGB")
        return self._pil_img

    def download(self) -> None:
        urls = (
            self._config.inputs_image_tiff.uri,
            self._config.outputs_summary_csv.uri,
            self._config.outputs_spatial_imaging_data.uri,
            self._config.outputs_feature_barcode_matrix_hdf5_filtered.uri,
        )
        with self._dl_manager:
            data_files: list[DataFile] = self._dl_manager.download_and_extract(urls)  # type: ignore

        paths = [Path(f.uri) for f in data_files]
        (
            self._tiff_path,
            self._summary_path,
            spatial_imaging_metadata_path,
            self._feature_barcode_matrix_path,
        ) = paths

        spatial_imaging_metadata: dict = json.loads(
            spatial_imaging_metadata_path.read_text()
        )
        self._scalefactors_path = Path(
            spatial_imaging_metadata["spatial/scalefactors_json.json"]
        )
        self._tissue_positions_path = Path(
            spatial_imaging_metadata.get(
                "spatial/tissue_positions_list.csv",
                spatial_imaging_metadata.get("spatial/tissue_positions.csv", ""),
            )
        )

    def plot_spots(
        self,
        resize_longest: int | None = 3840,
        only_in_tissue: bool = True,
        show: bool = True,
        save_path: str | Path | None = None,
    ):
        self.spots.plot(
            self.pil_image,
            self.spot_diameter_px,
            resize_longest=resize_longest,
            only_in_tissue=only_in_tissue,
            show=show,
            save_path=save_path,
        )

    def prepare(self) -> None:
        dataset_dir = settings.DATA_DIR / "datasets" / self._config.name
        spots_dir = dataset_dir / "spots"
        spots_dir.mkdir(parents=True, exist_ok=True)
        spots_plot_path = dataset_dir / "spots.png"
        config_path = dataset_dir / "config.json"
        self.plot_spots(show=False, save_path=spots_plot_path)
        self._config.save(config_path, overwrite=True)
        for spot in track(self.spots):
            if not spot.in_tissue:
                continue
            spot_np = spot.get_crop(self.img, self.spot_diameter_px)
            spot_pil = spot.get_pil_crop(self.pil_image, self.spot_diameter_px)
            spot_np_path = spots_dir / f"{spot.barcode}.npy"
            spot_pil_path = spots_dir / f"{spot.barcode}.png"
            np.save(spot_np_path, spot_np)
            spot_pil.save(spot_pil_path)

    def download_and_prepare(self) -> None:
        self.download()
        self.prepare()


if __name__ == "__main__":
    config = VisiumDatasetConfig.load("visium-ffpe-human-breast-cancer-1-3-0")
    builder = VisiumDatasetBuilder(config)
    builder.download_and_prepare()
