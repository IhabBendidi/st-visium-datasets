import typing as tp
from pathlib import Path

import datasets
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, RootModel

from st_visium_datasets.utils import DataFile, get_configs_dir
from st_visium_datasets.utils.utils import remove_prefix


class VisiumConfig(BaseModel):
    name: str
    homepage: str
    visium_dataset_name: str
    title: str
    description: str
    species: str
    anatomical_entity: str
    number_of_spots_under_tissue: int
    number_of_genes_detected: int = Field(
        ...,
        validation_alias=AliasChoices("genes_detected", "total_genes_detected"),
        serialization_alias="number_of_genes_detected",
    )

    image_tiff: DataFile
    spatial_imaging_data: DataFile
    feature_barcode_matrix_filtered: DataFile

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @classmethod
    def load(cls, filepath: Path):
        return cls.model_validate_json((filepath).read_text())

    def save(self, filepath: Path) -> None:
        filepath.write_text(self.model_dump_json(indent=2))


class VisiumConfigs(RootModel):
    root: tp.List[VisiumConfig] = Field(..., min_length=1)

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item: tp.Union[int, str]) -> VisiumConfig:
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
    def load(cls, dirpath: Path):
        configs = [VisiumConfig.load(path) for path in dirpath.glob("**/*.json")]
        return cls(root=configs)


class VisiumDatasetBuilderConfig(datasets.BuilderConfig):
    def __init__(self, visium_configs: VisiumConfigs, **kwargs):
        super().__init__(**kwargs)
        self.visium_configs = visium_configs

    @classmethod
    def load(cls, name: str, path: Path, **kwargs):
        if path.is_dir():
            visium_configs = VisiumConfigs.load(path)
        elif path.is_file():
            visium_configs = VisiumConfigs(root=[VisiumConfig.load(path)])
        else:
            raise FileNotFoundError(f"{path} is not a file or directory")

        return cls(
            name=name.strip("/").replace("/", "_"),
            visium_configs=visium_configs,
            **kwargs,
        )

    def __repr__(self) -> str:
        return (
            f"VisiumDatasetBuilderConfig(name={self.name!r}, "
            f"{len(self.visium_configs)} visium sub-datasets)"
        )


def gen_builder_configs(
    configs_dir: tp.Optional[Path] = None,
) -> tp.Generator[VisiumDatasetBuilderConfig, None, None]:
    base_configs_dir = get_configs_dir()

    if configs_dir is None:
        yield VisiumDatasetBuilderConfig.load("all", base_configs_dir)
        configs_dir = configs_dir or base_configs_dir

    for path in configs_dir.iterdir():
        if path == base_configs_dir or path.name.startswith((".", "_")):
            continue

        if path.is_dir():
            name = remove_prefix(str(path), str(base_configs_dir))
            yield VisiumDatasetBuilderConfig.load(name, path)
            yield from gen_builder_configs(path)
