from datasets import get_dataset_config_names, load_dataset, load_dataset_builder

from st_visium_datasets import builder


def list_visium_datasets() -> list[str]:
    return [c.name for c in builder.VisiumDatasetBuilder.BUILDER_CONFIGS]


def load_visium_dataset_builder(name: str, **kwargs) -> builder.VisiumDatasetBuilder:
    name = name.replace("/", "-").replace("_", "-")
    return load_dataset_builder(builder.__file__, name=name, **kwargs)  # type: ignore


if __name__ == "__main__":
    dsb = load_visium_dataset_builder("human/cerebellum")
    print(dsb)
    print(dsb.info)
    dsb.download_and_prepare()
