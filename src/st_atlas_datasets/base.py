import json
from abc import ABC, abstractmethod
from pathlib import Path

import typing_extensions as tp
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from st_atlas_datasets.settings import settings
from st_atlas_datasets.utils import sanitize_as_snake_case
from st_atlas_datasets.utils.download_manager import DownloadManager


def _str_sanitizer(s: str | None) -> str | None:
    if not s:
        return None
    if s.lower() in ["null", "none", "nan", "n/a"]:
        return None
    return sanitize_as_snake_case(s)


SanitizedStr = tp.Annotated[str, AfterValidator(_str_sanitizer)]


class BaseDatasetConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    title: str | None = None
    homepage: str | None = None
    description: str | None = None
    liscense: str | None = None
    species: SanitizedStr = "undefined"
    anatomical_entity: SanitizedStr = "undefined"
    disease_state: SanitizedStr | None = "undefined"

    @classmethod
    def get_configs_dir(cls) -> Path:
        return (Path(__file__).parent / "configs").resolve()

    @classmethod
    def config_builder_name(cls) -> str:
        return sanitize_as_snake_case(cls.__name__.replace("DatasetConfig", ""))

    @classmethod
    def load(cls: tp.Type[tp.Self], config_name_or_path: str | Path) -> tp.Self:
        if (path := Path(config_name_or_path)).is_file():
            return cls.model_validate_json(path.read_text())

        if isinstance(config_name_or_path, Path):
            raise FileNotFoundError(config_name_or_path)

        configs_dir = cls.get_configs_dir()
        config_name_or_path = str(config_name_or_path)

        if not config_name_or_path.endswith(".json"):
            config_name_or_path += ".json"

        for config_path in configs_dir.glob(config_name_or_path):
            return cls.model_validate_json(config_path.read_text())

        for config_path in configs_dir.glob(f"**/{config_name_or_path}"):
            return cls.model_validate_json(config_path.read_text())

        raise FileNotFoundError(config_name_or_path)

    def save(
        self, config_path: str | Path | None = None, overwrite: bool = False
    ) -> None:
        config_path = config_path or (
            self.get_configs_dir()
            / self.config_builder_name()
            / self.species
            / self.anatomical_entity
            / f"{self.name}.json"
        )

        config_path = Path(config_path)

        if config_path.is_file() and not overwrite:
            raise FileExistsError(
                f"Config file already exists at {config_path.resolve()}"
            )

        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(self.model_dump_json(indent=2))


class MultiDatasetConfig(BaseDatasetConfig):
    configs: tp.Sequence[BaseDatasetConfig] = Field(..., min_length=1)

    def __iter__(self) -> tp.Iterator[BaseDatasetConfig]:
        return iter(self.configs)

    def __getitem__(self, item: int) -> BaseDatasetConfig:
        return self.configs[item]

    def __len__(self) -> int:
        return len(self.configs)

    def __repr__(self) -> str:
        return f"MultiDatasetConfig(name={self.name}, nb_configs={len(self)})"

    def __str__(self) -> str:
        return self.__repr__()

    def __add__(
        self, other: "BaseDatasetConfig | MultiDatasetConfig"
    ) -> "MultiDatasetConfig":
        if isinstance(other, MultiDatasetConfig):
            name = "+".join([self.name, other.name])
            return self.model_validate(
                {"name": name, "configs": [*self.configs, *other.configs]}
            )
        if isinstance(other, BaseDatasetConfig):
            return self.model_copy(update={"configs": [*self.configs, other]})
        raise TypeError(other)

    @model_validator(mode="after")
    def _fill_defaults(self):
        def _get_value_from_configs(field_name: str) -> tp.Any:
            values = {}
            for c in self.configs:
                v = getattr(c, field_name, None)
                if v:
                    values[c.name] = v
            if not values:
                return None
            if len(set(values.values())) == 1:
                return values.popitem()[1]
            return json.dumps(values)

        for field_name in self.model_fields:
            if v := getattr(self, field_name, None):
                continue
            if v := _get_value_from_configs(field_name):
                setattr(self, field_name, v)

        return self

    def save(self, overwrite: bool = False) -> None:
        for config in self:
            config.save(overwrite=overwrite)

    @classmethod
    def load(cls, config_name_or_path: str | Path) -> None:
        raise NotImplementedError("MultiDatasetConfig cannot be loaded from a config")


class BaseDatasetBuilder(ABC):
    def __init__(
        self,
        config: BaseDatasetConfig,
        dl_manager: DownloadManager | None = None,
    ):
        self.config = config
        self.dl_manager = dl_manager or DownloadManager()

        @abstractmethod
        def download(self):
            pass

        @abstractmethod
        def prepare(self):
            pass
