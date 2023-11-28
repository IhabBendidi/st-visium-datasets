import io
import json
import logging
import re
import typing as tp

import pandas as pd
import requests
from parsel import Selector
from pydantic import BeforeValidator
from rich.logging import RichHandler

__all__ = [
    "sanitize_str",
    "SnakeCaseStr",
    "KebabCaseStr",
    "scrap_visium_dataset_homepage",
    "setup_logging",
]

_COMMON_SANITIZATIONS = {"FFPE": "ffpe", "CytAssist": "cytassist", "FASTQ": "fastq"}


def setup_logging() -> None:
    logging.basicConfig(
        level="NOTSET",
        format="%(message)s",
        datefmt="[%x %X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )


def sanitize_str(s: str, sep: str = "_") -> str:
    # TODO: can be done in fewer lines
    # Replace common patterns
    for old, new in _COMMON_SANITIZATIONS.items():
        s = s.replace(old, new)
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


def _sanitize(s: str | None, sep: str) -> str:
    if s is None:
        return "undefined"
    if s.lower() in ["null", "none", "nan", "n/a"]:
        return "undefined"
    return sanitize_str(s, sep=sep)


SnakeCaseStr = tp.Annotated[str, BeforeValidator(lambda s: _sanitize(s, "_"))]
KebabCaseStr = tp.Annotated[str, BeforeValidator(lambda s: _sanitize(s, "-"))]


def scrap_visium_dataset_homepage(
    url: str, **overrides
) -> tp.Generator[dict, None, None]:
    props = _parse_json_props(url)
    dataset = props.get("props", {}).get("pageProps", {}).get("dataset", {})
    filesets: list[dict[str, list[dict[str, str]]]] = (
        props.get("props", {}).get("pageProps", {}).get("filesets", [])
    )
    visium_dataset_name = props.get("query", {}).get("slug")
    if not dataset:
        raise ValueError(f"Could not find dataset props in {url}")

    info = _get_dataset_info(dataset)
    info.update({"homepage": url, "visium_dataset_name": visium_dataset_name})
    if overrides:
        info.update(overrides)

    version = info.pop("pipeline_version", None) or ""

    ressource_ids = _get_dataset_ressource_ids(dataset)
    data_files_array = _get_data_files(filesets)

    if len(ressource_ids) != len(data_files_array):
        raise ValueError(
            f"Found {len(ressource_ids)} ressource ids but {len(data_files_array)} data files"
        )
    for ressource_id, data_files in zip(ressource_ids, data_files_array):
        name = sanitize_str(f"{ressource_id}-{version}", sep="-")
        summary = {}
        if "summary_csv" in data_files:
            summary = _parse_summary_csv(data_files["summary_csv"]["url"])
        yield {"name": name, **info, **data_files, **summary}


def _parse_json_props(uri: str) -> dict[str, tp.Any]:
    response = requests.get(uri)
    response.raise_for_status()
    selector = Selector(response.text)
    text = selector.xpath("/html/body/script[1]/text()").get()
    if text is None:
        raise ValueError(
            f"[{response.status_code}] Could not scrap dataset props in {uri}"
        )
    body = json.loads(text)
    return body


def _get_dataset_info(dataset: dict[str, tp.Any]) -> dict[str, tp.Any]:
    info = {
        "title": dataset.get("title"),
        "published_at": dataset.get("publishedAt"),
        "description": dataset.get("body"),
        "pipeline_version": dataset.get("pipeline", {}).get("version"),
    }
    return info


def _get_dataset_ressource_ids(dataset: dict[str, tp.Any]):
    files = dataset.get("files", []) or []
    if not files:
        raise ValueError("Could not find any file in dataset")

    def _parse(file: dict) -> str:
        s = file.get("resourceId") or file.get("title") or ""
        return s

    return list(map(_parse, files))


def _get_data_files(
    filesets: list[dict[str, list[dict[str, str]]]]
) -> list[dict[str, dict[str, str]]]:
    data_files_array: list[dict[str, dict[str, str]]] = []
    fileset_keys = ["inputs", "outputs"]
    for fileset in filesets:
        data_files: dict[str, dict[str, str]] = {}
        for key in fileset_keys:
            if not fileset.get(key):
                continue
            files = fileset[key]
            for file in files:
                if not isinstance(file, dict):
                    continue
                data_files[sanitize_str(file["title"])] = file
        data_files_array.append(data_files)
    return data_files_array


def _parse_summary_csv(url: str) -> dict[str, tp.Any]:
    content = io.BytesIO(requests.get(url).content)
    data = pd.read_csv(content).to_dict(orient="records")
    assert len(data) == 1
    summary = data[0]
    summary = {sanitize_str(k): v for k, v in summary.items()}  # type: ignore
    return summary
