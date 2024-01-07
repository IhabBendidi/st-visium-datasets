from __future__ import annotations

import logging
import logging.config
import re
import typing as tp
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from datasets.utils.logging import get_verbosity
from rich.console import ConsoleRenderable
from rich.logging import RichHandler as _RichHandler
from rich.traceback import Traceback

import st_visium_datasets


class RichHandler(_RichHandler):
    """Custom rich logging handler that prints logger name instead of path in the
    most right column.
    """

    def __init__(self, **kwargs) -> None:
        kwargs.update({"rich_tracebacks": True, "enable_link_path": True})
        super().__init__(**kwargs)

    def render(
        self,
        *,
        record: logging.LogRecord,
        traceback: tp.Optional[Traceback],
        message_renderable: ConsoleRenderable,
    ) -> ConsoleRenderable:
        path = record.name
        level = self.get_level_text(record)
        time_format = None if self.formatter is None else self.formatter.datefmt
        log_time = datetime.fromtimestamp(record.created)

        log_renderable = self._log_render(
            self.console,
            [message_renderable] if not traceback else [message_renderable, traceback],
            log_time=log_time,
            time_format=time_format,
            level=level,
            path=path,
            link_path=record.pathname if self.enable_link_path else None,
        )
        return log_renderable


def setup_logging(level: int | str | None = None) -> None:
    logging.basicConfig(
        level=level if level is not None else get_verbosity(),
        format="%(message)s",
        datefmt="[%x %X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )


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


def get_nested_filepath(dirname: Path, filename: str) -> Path:
    """Return path to file in directory"""
    paths = list(dirname.glob(f"**/{filename}"))
    if len(paths) == 0:
        raise FileNotFoundError(f"no {filename} found in {dirname}")
    if len(paths) > 1:
        raise ValueError(f"multiple {filename} found in {dirname}: {paths}")
    return paths[0]


@lru_cache
def get_configs_dir() -> Path:
    return Path(st_visium_datasets.__file__).parent / "configs"
