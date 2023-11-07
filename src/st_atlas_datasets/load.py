import importlib
import inspect
import os
import typing as tp
from pathlib import Path

from datasets import load_dataset as _load_dataset

from st_atlas_datasets import st_atlas
from st_atlas_datasets.base import BaseDatasetBuilder


def load_dataset(name: str):
    path = os.path.join(os.path.dirname(st_atlas.__file__), f"{name}.py")
    return _load_dataset(path)


if __name__ == "__main__":
    load_dataset("visium_human_breast_cancer")
