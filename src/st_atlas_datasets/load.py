import inspect
import typing as tp

import pandas as pd

import st_atlas_datasets
from st_atlas_datasets.base import BaseDatasetConfig, MultiDatasetConfig


def _load_config_builder(config_builder_name: str) -> tp.Type[BaseDatasetConfig]:
    def _predicate(obj) -> bool:
        return (
            inspect.isclass(obj)
            and issubclass(obj, BaseDatasetConfig)
            and obj.config_builder_name() == config_builder_name
        )

    members = inspect.getmembers(st_atlas_datasets, predicate=_predicate)
    if not members:
        raise ValueError(f"Could not find config builder: '{config_builder_name}'")

    if len(members) > 1:
        raise ValueError(
            f"Found multiple config builders for '{config_builder_name}': {members}"
        )

    _, cls = members[0]
    return cls


def _load_dataset_config(
    datasets_registry: pd.DataFrame,
    config_name: str,
) -> BaseDatasetConfig:
    rows = datasets_registry.loc[datasets_registry["config_name"] == config_name]
    assert len(rows) == 1
    row = rows.iloc[0]
    config_builder_name, species, anatomical_entity = row.name  # type: ignore
    cls = _load_config_builder(config_builder_name)
    full_config_name = (
        f"{config_builder_name}/{species}/{anatomical_entity}/{config_name}"
    )
    return cls.load(full_config_name)


def _build_datasets_registry() -> pd.DataFrame:
    configs_dir = BaseDatasetConfig.get_configs_dir()
    configs_metadata = []
    for config_path in configs_dir.glob("**/*.json"):
        (
            config_builder_name,
            species,
            anatomical_entity,
            filename,
        ) = config_path.parts[-4:]
        metadata = {
            "config_builder_name": config_builder_name,
            "species": species,
            "anatomical_entity": anatomical_entity,
            "config_name": filename.rstrip(".json"),
        }
        configs_metadata.append(metadata)

    df = pd.DataFrame(configs_metadata)
    df = df.set_index(["config_builder_name", "species", "anatomical_entity"])
    if not df["config_name"].is_unique:
        raise ValueError("Config names must be unique")
    return df


def _list_grouped_dataset_names(
    datasets_registry: pd.DataFrame
) -> dict[str, dict[str, str]]:
    dataset_groups = {}

    # Dataset hiearchy is split into 3 levels:

    # Level 1: config_builder_name dataset_name, e.g. visium and species, eg: human
    config_builder_name = (
        datasets_registry.index.get_level_values("config_builder_name")
        .unique()
        .tolist()
    )
    dataset_groups.update(
        {name: {"config_builder_name": name} for name in config_builder_name}
    )
    species = datasets_registry.index.get_level_values("species").unique().tolist()
    dataset_groups.update({name: {"species": name} for name in species})

    # Level 2: '{config_builder_name}/{species}'
    for index, _ in datasets_registry.groupby(level=(0, 1)):
        config_builder_name, species = index
        dataset_groups[f"{config_builder_name}/{species}"] = {
            "config_builder_name": config_builder_name,
            "species": species,
        }

    # Level 3: '{species}/{anatomical_entity}'
    for index, _ in datasets_registry.groupby(level=(1, 2)):
        species, anatomical_entity = index
        dataset_groups[f"{species}/{anatomical_entity}"] = {
            "anatomical_entity": anatomical_entity,
            "species": species,
        }

    # Level 4: '{species}/{anatomical_entity}'
    for index, _ in datasets_registry.groupby(level=(0, 1, 2)):
        config_builder_name, species, anatomical_entity = index
        dataset_groups[f"{config_builder_name}/{species}/{anatomical_entity}"] = {
            "config_builder_name": config_builder_name,
            "anatomical_entity": anatomical_entity,
            "species": species,
        }

    return dataset_groups


def _list_individual_dataset_names(
    datasets_registry: pd.DataFrame
) -> dict[str, dict[str, str]]:
    dr = datasets_registry.reset_index().set_index("config_name")
    return {c: {"config_name": c} for c in dr.index.tolist()}


_DATASETS_REGISTRY = _build_datasets_registry()
_GROUPED_DATASET_NAMES = _list_grouped_dataset_names(_DATASETS_REGISTRY)
_INDIVIDUAL_DATASET_NAMES = _list_individual_dataset_names(_DATASETS_REGISTRY)
_DATASET_NAMES = {**_GROUPED_DATASET_NAMES, **_INDIVIDUAL_DATASET_NAMES}


def list_datasets(
    filter_: tp.Literal["all", "grouped", "individual"] = "all"
) -> list[str]:
    if filter_ == "all":
        return list(_DATASET_NAMES.keys())
    if filter_ == "grouped":
        return list(_GROUPED_DATASET_NAMES.keys())
    if filter_ == "individual":
        return list(_INDIVIDUAL_DATASET_NAMES.keys())
    raise ValueError(filter_)


def get_datasets_registry() -> pd.DataFrame:
    return _DATASETS_REGISTRY.copy()


def load_dataset_config(dataset_name: str) -> BaseDatasetConfig:
    if dataset_name not in _DATASET_NAMES:
        raise ValueError(
            f"Unknown dataset name: '{dataset_name}'. "
            f"Available dataset names: {list_datasets()}"
        )
    dsr = _DATASETS_REGISTRY.sort_index(level=(0, 1, 2), ascending=(True, True, True))
    params = _DATASET_NAMES[dataset_name]
    config_name = params.get("config_name")
    if config_name:
        return _load_dataset_config(dsr, config_name)

    config_builder_name = params.get("config_builder_name") or slice(None)
    species = params.get("species") or slice(None)
    anatomical_entity = params.get("anatomical_entity") or slice(None)

    dsr = dsr.loc[pd.IndexSlice[config_builder_name, species, anatomical_entity], :]
    if dsr.empty:
        raise ValueError(f"Could not find dataset: '{dataset_name}'")

    configs = [
        _load_dataset_config(dsr, config_name) for config_name in dsr["config_name"]
    ]
    return MultiDatasetConfig(name=dataset_name, configs=configs)


if __name__ == "__main__":
    config = load_dataset_config("human")
    print(config)
