import dataclasses as dc
import typing as tp
from pathlib import Path

import datasets
import typing_extensions as tx
from pydantic import BaseModel, ConfigDict, Field, RootModel

from st_visium_datasets.utils import get_configs_dir, sanitize_str


class DataFile(BaseModel):
    url: str
    md5sum: str
    size: int = Field(..., alias="bytes")

    model_config = ConfigDict(populate_by_name=True)

    def __repr__(self) -> str:
        return f"DataFile('{self.url}')"

    def __str__(self) -> str:
        return str(self.url)


class VisiumConfig(BaseModel):
    name: str
    homepage: str
    visium_dataset_name: str
    title: str
    description: str
    species: str
    anatomical_entity: str
    number_of_spots_under_tissue: int
    number_of_genes_detected: int = Field(..., alias="genes_detected")

    image_tiff: DataFile
    spatial_imaging_data: DataFile
    feature_barcode_matrix_filtered: DataFile

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @classmethod
    def load(cls, filepath: Path) -> tx.Self:
        return cls.model_validate_json((filepath).read_text())

    def save(self, filepath: Path) -> None:
        filepath.write_text(self.model_dump_json(indent=2))


class VisiumConfigs(RootModel):
    root: list[VisiumConfig] = Field(..., min_length=1)

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item: int | str) -> VisiumConfig:
        if isinstance(item, int):
            return self.root[item]
        configs = [c for c in self.root if c.name == item]
        if not configs:
            raise KeyError(f"{item!r} not found")
        if len(configs) > 1:
            raise KeyError(f"multiple configs found for {item!r}")
        return configs[0]

    def __len__(self):
        return len(self.root)

    @classmethod
    def load(cls, dirpath: Path) -> tx.Self:
        configs = [VisiumConfig.load(path) for path in dirpath.glob("**/*.json")]
        return cls(root=configs)


@dc.dataclass(kw_only=True)
class VisiumDatasetBuilderConfig(datasets.BuilderConfig):
    name: str
    version: datasets.Version = datasets.Version("1.0.0")
    visium_configs: VisiumConfigs

    @classmethod
    def load(cls, name: str, path: Path, **kwargs) -> tx.Self:
        if path.is_dir():
            visium_configs = VisiumConfigs.load(path)
        elif path.is_file():
            visium_configs = VisiumConfigs(root=[VisiumConfig.load(path)])
        else:
            raise FileNotFoundError(f"{path} is not a file or directory")

        name = sanitize_str(name, sep="-")
        return cls(name=name, visium_configs=visium_configs, **kwargs)

    def __repr__(self) -> str:
        return (
            f"VisiumDatasetBuilderConfig(name={self.name!r}, "
            f"{len(self.visium_configs)} visium sub-datasets)"
        )


def gen_builder_configs(
    configs_dir: Path | None = None
) -> tp.Generator[VisiumDatasetBuilderConfig, None, None]:
    base_configs_dir = get_configs_dir()

    if configs_dir is None:
        yield VisiumDatasetBuilderConfig.load("all", base_configs_dir)
        configs_dir = configs_dir or base_configs_dir

    for path in configs_dir.iterdir():
        if path == base_configs_dir or path.name.startswith((".", "_")):
            continue

        if path.is_dir():
            name = str(path).removeprefix(str(base_configs_dir))
            yield VisiumDatasetBuilderConfig.load(name, path)
            yield from gen_builder_configs(path)
