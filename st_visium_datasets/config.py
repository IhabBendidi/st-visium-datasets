import datetime as dt
from pathlib import Path

import typing_extensions as tx
from pydantic import BaseModel, ConfigDict, HttpUrl

from st_visium_datasets.data_file import DataFile
from st_visium_datasets.utils import KebabCaseStr

__all__ = ["DatasetConfig", "get_config_path", "get_configs_dir"]


def get_configs_dir() -> Path:
    return Path(__file__).parent / "configs"


def get_config_path(config_name: str | Path) -> Path:
    if (path := Path(config_name)).is_file():
        return path

    if isinstance(config_name, Path):
        raise FileNotFoundError(config_name)

    configs_dir = get_configs_dir()
    if not config_name.endswith(".json"):
        config_name += ".json"

    config_paths = list(configs_dir.glob(config_name))
    if not config_paths:
        raise ValueError(f"Config {config_name} not found in {configs_dir}")

    if len(config_paths) > 1:
        raise ValueError(f"Multiple configs found for {config_name}: {config_paths}")

    return config_paths[0]


class DatasetConfig(BaseModel):
    name: KebabCaseStr
    homepage: HttpUrl
    visium_dataset_name: str
    title: str
    description: str
    liscense: str | None = None
    published_at: dt.datetime | None = None
    species: KebabCaseStr = "undefined"
    anatomical_entity: KebabCaseStr = "undefined"
    disease_state: KebabCaseStr = "undefined"
    preservation_method: str | None = None
    staining_method: str | None = None
    biomaterial_type: str | None = None
    donor_count: int | None = None
    development_stage: str | None = None
    number_of_spots_under_tissue: int | None = None

    image_tiff: DataFile
    feature_barcode_matrix_hdf5_filtered: DataFile
    spatial_imaging_data: DataFile

    model_config = ConfigDict(extra="allow")

    def save(self, *, overwrite: bool = False) -> None:
        config_path = (
            get_configs_dir()
            / self.species
            / self.anatomical_entity
            / f"{self.name}.json"
        )
        if config_path.is_file() and not overwrite:
            raise FileExistsError(config_path)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(self.model_dump_json(indent=2))
        return None

    @classmethod
    def load(cls, name: str) -> tx.Self:
        return cls.model_validate_json(get_config_path(name).read_text())
