import logging

import datasets

from st_visium_datasets.utils import sanitize_str, setup_logging
from st_visium_datasets.visium import visium

setup_logging(level="DEBUG")

logger = logging.getLogger(__name__)


def get_visium_dataset_path() -> str:
    """Load visium dataset path for usage with `datasets.load_dataset` from huggingface

    Example:
    >>> from st_visium_datasets import get_visium_dataset_path
    >>> import datasets
    >>> dataset = datasets.load_dataset(get_visium_dataset_path(), name="human-cerebellum")
    >>> builder = datasets.load_dataset_builder(get_visium_dataset_path(), name="human-cerebellum")
    """
    return visium.__file__


def load_visium_dataset_builder(name: str = "all", **kwargs):
    return datasets.load_dataset_builder(
        get_visium_dataset_path(),
        name=sanitize_str(name, sep="-"),
        **kwargs,
    )


def load_visium_dataset(name: str = "all", **kwargs):
    if split := kwargs.get("split"):
        if not split.startswith(visium.VisiumDatasetBuilder.DEFAULT_SPLIT):
            logger.warning(
                f"split '{split}' does not start with '{visium.VisiumDatasetBuilder.DEFAULT_SPLIT}'. "
                f"Only one split is provided: {visium.VisiumDatasetBuilder.DEFAULT_SPLIT}"
            )
            split = visium.VisiumDatasetBuilder.DEFAULT_SPLIT
    else:
        split = visium.VisiumDatasetBuilder.DEFAULT_SPLIT

    kwargs.update({"split": split})
    return datasets.load_dataset(
        get_visium_dataset_path(),
        name=sanitize_str(name, sep="-"),
        **kwargs,
    )


def list_visium_datasets(**kwargs):
    return datasets.get_dataset_config_names(get_visium_dataset_path(), **kwargs)


if __name__ == "__main__":
    b = load_visium_dataset("human-cerebellum")
