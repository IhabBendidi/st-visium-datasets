import json
import logging
import typing as tp

import requests
from parsel import Selector

from st_atlas_datasets.utils import DataFile, sanitize_as_snake_case, sanitize_with_sep

logger = logging.getLogger(__name__)


def scrap_visium_homepage(homepage_url: str) -> list[dict[str, tp.Any]]:
    props = _parse_json_props(homepage_url)
    dataset = props.get("props", {}).get("pageProps", {}).get("dataset", {})
    filesets: list[dict[str, list[dict[str, str]]]] = (
        props.get("props", {}).get("pageProps", {}).get("filesets", [])
    )
    if not dataset:
        raise ValueError(f"Could not find dataset props in {homepage_url}")

    ressource_ids = _get_dataset_ressource_ids(dataset)
    data_files_array = _get_data_files(filesets)

    if len(ressource_ids) != len(data_files_array):
        raise ValueError(
            f"Found {len(ressource_ids)} ressource ids but {len(data_files_array)} data files"
        )

    visium_dataset_name = props.get("query", {}).get("slug")

    base_info = {
        "homepage": homepage_url,
        "visium_dataset_name": visium_dataset_name,
        **_get_dataset_info(dataset),
    }
    version = base_info.get("pipeline_version") or ""
    configs = []
    for ressource_id, data_files in zip(ressource_ids, data_files_array):
        name = sanitize_with_sep(f"{ressource_id}-{version}", sep="-")
        config = {"name": name, **base_info, **data_files}
        configs.append(config)
    return configs


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
        "product_slug": dataset.get("product", {}).get("slug"),
        "software_name": dataset.get("software", {}).get("name"),
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
) -> list[dict[str, DataFile]]:
    data_files_array: list[dict[str, DataFile]] = []
    fileset_keys = ["inputs", "outputs"]
    for fileset in filesets:
        data_files: dict[str, DataFile] = {}
        for key in fileset_keys:
            if not fileset.get(key):
                raise ValueError(f"Could not find '{key}' in fileset: {fileset}")
            files = fileset[key]
            for file in files:
                if not isinstance(file, dict):
                    continue
                file_key = sanitize_as_snake_case(f"{key}_{file['title']}")
                data_file = DataFile(uri=file["url"], md5sum=file["md5sum"])
                data_files[file_key] = data_file
        data_files_array.append(data_files)
    return data_files_array


if __name__ == "__main__":
    uris = [
        "https://www.10xgenomics.com/resources/datasets/visium-cytassist-gene-expression-libraries-of-post-xenium-human-colon-cancer-ffpe-using-the-human-whole-transcriptome-probe-set-2-standard",
        "https://www.10xgenomics.com/resources/datasets/human-breast-cancer-ductal-carcinoma-in-situ-invasive-carcinoma-ffpe-1-standard-1-3-0",
    ]
    for url in uris:
        print(f"\n\nHOMEPAGE: {url}")
        scrap_visium_homepage(url)
