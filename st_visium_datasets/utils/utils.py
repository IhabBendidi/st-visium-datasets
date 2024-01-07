from __future__ import annotations

import logging
import logging.config
import re
from functools import lru_cache
from pathlib import Path

from datasets.utils.logging import get_verbosity

import st_visium_datasets


def setup_logging(level: str | int | None = None) -> None:
    level = level or get_verbosity()
    logging.getLogger("st_visium_datasets").setLevel(level)


def remove_prefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix) :]
    return s


def remove_suffix(s: str, suffix: str) -> str:
    if s.endswith(suffix):
        return s[: -len(suffix)]
    return s


def sanitize_str(s: str, sep: str = "_") -> str:
    # Replace camelCase with sep
    s = re.sub("(.)([A-Z][a-z]+)", r"\1{}\2".format(sep), s)
    s = re.sub("([a-z0-9])([A-Z])", r"\1{}\2".format(sep), s)
    s = s.lower()
    # Replace ' ', '.', '-', '_', '/', '(', ')', ''', ',' with sep
    s = re.sub(r"[ -._/,;:()\']", sep, s)
    # Remove leading _, -, sep
    s = s.strip("_").strip("-")
    # Deduplicate consecutive '_' and '-'
    s = re.sub(r"_{2,}", "_", s)
    s = re.sub(r"-{2,}", "-", s)
    return s


def get_nested_filepath(dirname: Path | str, filename: str) -> Path:
    """Return path to file in directory"""
    paths = list(Path(dirname).glob(f"**/{filename}"))
    if len(paths) == 0:
        raise FileNotFoundError(f"no {filename} found in {dirname}")
    if len(paths) > 1:
        raise ValueError(f"multiple {filename} found in {dirname}: {paths}")
    return paths[0]


@lru_cache
def get_configs_dir() -> Path:
    return Path(st_visium_datasets.__file__).parent / "configs"
