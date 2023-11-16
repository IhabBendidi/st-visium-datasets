import dataclasses as dc
import json
import typing as tp

import datasets

from st_atlas_datasets.utils import to_snake_case


class _ConfigCompatibilityMixin:
    @property
    def data_dir(self) -> None:
        """For compatibility with datasets"""
        return None

    @property
    def data_files(self) -> None:
        """For compatibility with datasets"""
        return None

    def create_config_id(self, *args, **kwargs) -> str:
        name = getattr(self, "name")
        return f"st-atlas-datasets/{name}"


@dc.dataclass(kw_only=True, frozen=True)
class BaseBuilderConfig(_ConfigCompatibilityMixin):
    full_name: str = ""
    short_name: str = ""
    description: str = ""
    version: datasets.Version = datasets.Version("0.0.0")
    homepage: str = ""
    license: str = ""
    citation: str = ""
    metadata: dict[str, str] = dc.field(default_factory=dict)

    @property
    def name(self) -> str:
        if self.short_name:
            return to_snake_case(self.short_name)
        if self.full_name:
            return to_snake_case(self.full_name)
        raise ValueError("Either full_name or short_name must be set")


T = tp.TypeVar("T", bound=BaseBuilderConfig)


@dc.dataclass(kw_only=True, frozen=True)
class MultiBuilderConfig(tp.Generic[T], _ConfigCompatibilityMixin):
    name: str
    configs: list[T]
    version: datasets.Version | None = None

    def __post_init__(self) -> None:
        if not self.configs:
            raise ValueError("configs must not be empty")

    def __iter__(self) -> tp.Iterator[T]:
        return iter(self.configs)

    def __getitem__(self, key: int) -> T:
        return self.configs[key]

    def get_config_by_name(self, name: str) -> T:
        for config in self.configs:
            if config.name == name:
                return config
        raise ValueError(f"Config with name {name} not found")

    @property
    def license(self) -> str:
        return json.dumps({c.name: c.license for c in self.configs}, indent=2)

    @property
    def homepage(self) -> str:
        return json.dumps({c.name: c.homepage for c in self.configs}, indent=2)

    @property
    def description(self) -> str:
        return json.dumps({c.name: c.description for c in self.configs}, indent=2)

    @property
    def citation(self) -> str:
        return json.dumps({c.name: c.citation for c in self.configs}, indent=2)


@dc.dataclass(kw_only=True, frozen=True)
class TenXGenomicsBuilderConfig(BaseBuilderConfig):
    tiff_url: str
    spatial_url: str
    filtered_feature_bc_matrix_url: str
    license: str = "CC BY 4.0"

    @property
    def urls(self) -> list[str]:
        return [self.tiff_url, self.spatial_url, self.filtered_feature_bc_matrix_url]


class TenXGenomicsBaseBuilder(datasets.GeneratorBasedBuilder):
    BUILDER_CONFIG_CLASS = TenXGenomicsBuilderConfig

    def _info(self):
        self.config: TenXGenomicsBuilderConfig | MultiBuilderConfig
        return datasets.DatasetInfo(
            description=self.config.description,
            homepage=self.config.homepage,
            citation=self.config.citation,
            license=self.config.license,
        )

    def _generate_spots(self, config_name: str, tiff_path: str, spatial_dir: str):
        raise NotImplementedError

    def _split_generators(self, dl_manager: datasets.DownloadManager):
        if isinstance(self.config, MultiBuilderConfig):
            assert all(isinstance(c, TenXGenomicsBuilderConfig) for c in self.config)
            urls: dict[str, list[str]] = {c.name: c.urls for c in self.config}
        elif isinstance(self.config, TenXGenomicsBuilderConfig):
            urls = {self.config.name: self.config.urls}
        else:
            raise TypeError(
                f"Unexpected config type: {type(self.config)}, expected one "
                f"of {TenXGenomicsBuilderConfig}, {MultiBuilderConfig}"
            )

        local_files_map: dict[str, list[str]] = dl_manager.download_and_extract(urls)  # type: ignore
        for config_name, local_files in local_files_map.items():
            tiff_path, spatial_dir, *_ = local_files
            self._generate_spots(config_name, tiff_path, spatial_dir)

        yield (
            datasets.SplitGenerator(
                name=datasets.Split.TRAIN,
                gen_kwargs={
                    "tiff_path": tiff_path,
                    "spatial_path": spatial_path,
                },
            ),
        )
