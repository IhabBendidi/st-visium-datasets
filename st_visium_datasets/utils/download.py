from __future__ import annotations

import hashlib
import logging
import tarfile
import typing as tp
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import fsspec
from rich.console import Group
from rich.live import Live
from rich.progress import (
    BarColumn,
    DownloadColumn,
    MofNCompleteColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)

from st_visium_datasets.utils.data_file import DataFile
from st_visium_datasets.utils.utils import remove_suffix

logger = logging.getLogger(__name__)

DatasetsDict = tp.Dict[str, tp.Dict[str, DataFile]]
LocalDatatsetDict = tp.Dict[str, tp.Dict[str, Path]]


def _create_dl_progress(**kwargs) -> Progress:
    columns = (
        TextColumn("[bold blue][{task.fields[task_name]}][/bold blue]"),
        TextColumn("[bold green]{task.fields[dataset_name]}[/bold green]"),
        "•",
        TextColumn("{task.fields[name]}"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeElapsedColumn(),
    )
    kwargs.setdefault("expand", True)
    return Progress(*columns, **kwargs)


def _create_global_progress(**kwargs) -> Progress:
    columns = (
        TextColumn("[bold blue]Downloading datasets[/bold blue]"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        "•",
        MofNCompleteColumn(),
        "files",
        "•",
        TimeElapsedColumn(),
    )
    kwargs.setdefault("expand", True)
    return Progress(*columns, **kwargs)


def _split_extensions(path: str | Path) -> tuple[str, str]:
    ext = "".join(Path(path).suffixes)
    return remove_suffix(str(path), ext), ext


def _validate_md5sum(
    dataset_name: str,
    name: str,
    file: DataFile,
    filepath: Path,
    dl_progress: Progress,
    buffer_size: int = 8192,
):
    if not filepath.is_file():
        raise FileNotFoundError(filepath)

    md5sum_path = filepath.with_suffix(".md5sum")
    if md5sum_path.is_file():
        computed_md5sum = md5sum_path.read_text().strip()
    else:
        md5 = hashlib.md5()
        task_id = dl_progress.add_task(
            "",
            total=file.size,
            dataset_name=dataset_name,
            task_name="Validate MD5",
            name=name,
        )
        with dl_progress.open(
            filepath, mode="rb", buffering=buffer_size, task_id=task_id
        ) as f:
            while chunk := f.read(buffer_size):
                md5.update(chunk)

        dl_progress.update(task_id, visible=False)
        computed_md5sum = md5.hexdigest()
        md5sum_path.write_text(computed_md5sum)

    return computed_md5sum == file.md5sum


def _download_file(
    dataset_name: str,
    name: str,
    file: DataFile,
    download_dir: Path,
    dl_progress: Progress,
    force_download: bool = False,
    buffer_size: int = 8192,
):
    _, ext = _split_extensions(file.url)
    save_path = download_dir / f"{file.md5sum}{ext}"
    if force_download or not save_path.is_file():
        task_id = dl_progress.add_task(
            "",
            total=file.size,
            dataset_name=dataset_name,
            task_name="Download",
            name=name,
        )

        with fsspec.open(file.url, "rb") as src:
            with open(save_path, "wb") as fout:
                while chunk := src.read(buffer_size):  # type: ignore
                    fout.write(chunk)
                    dl_progress.update(task_id, advance=len(chunk))

        dl_progress.update(task_id, visible=False)

    is_valid = _validate_md5sum(
        dataset_name,
        name,
        file,
        save_path,
        dl_progress,
        buffer_size=buffer_size,
    )
    if is_valid:
        return save_path
    return None


def _extract_file(
    dataset_name: str,
    name: str,
    filepath: Path,
    dl_progress: Progress,
    force_extract: bool = False,
    buffer_size: int = 8192,
) -> Path:
    if not filepath.is_file():
        raise FileNotFoundError(filepath)

    if not str(filepath).endswith((".tar.gz")):
        return filepath

    extract_dir = Path(remove_suffix(str(filepath), ".tar.gz"))
    if extract_dir.is_dir() and list(extract_dir.iterdir()) and not force_extract:
        return extract_dir

    extract_dir.mkdir(exist_ok=True, parents=True)

    task_id = dl_progress.add_task(
        "",
        dataset_name=dataset_name,
        task_name="Extract",
        name=name,
    )
    fileobj = dl_progress.open(filepath, "rb", task_id=task_id, buffering=buffer_size)
    with tarfile.open(fileobj=fileobj) as tar:
        tar.extractall(extract_dir)

    dl_progress.update(task_id, visible=False)
    return extract_dir


def _download_and_extract_file(
    dataset_name: str,
    name: str,
    file: DataFile,
    download_dir: Path,
    dl_progress: Progress,
    force_download: bool = False,
    force_extract: bool = False,
    buffer_size: int = 8192,
    max_retries: int = 3,
) -> Path:
    i = 0
    save_path = None
    while i < max_retries:
        _force_download = force_download or i > 0
        save_path = _download_file(
            dataset_name,
            name,
            file,
            download_dir,
            dl_progress,
            force_download=_force_download,
            buffer_size=buffer_size,
        )
        if save_path is not None:
            break
        logger.warning(f"Failed to download {file.url} (attempt {i + 1}/{max_retries})")
        i += 1

    if save_path is None:
        raise RuntimeError(f"Failed to download {file}")

    return _extract_file(
        dataset_name,
        name,
        save_path,
        dl_progress,
        force_extract=force_extract,
        buffer_size=buffer_size,
    )


def download_visium_datasets(
    datasets: DatasetsDict,
    *,
    download_dir: str | Path,
    force_download: bool = False,
    force_extract: bool = False,
    disable_pbar: bool = False,
    max_retries: int = 3,
) -> LocalDatatsetDict:
    download_dir = Path(download_dir)
    download_dir.mkdir(exist_ok=True, parents=True)

    total_nb_files = sum(len(d) for d in datasets.values())
    global_progress = _create_global_progress(disable=disable_pbar)
    dl_progress = _create_dl_progress(disable=disable_pbar)

    progress_group = Group(global_progress, dl_progress)

    def _worker(dataset_name: str, name: str, file: DataFile):
        save_path = _download_and_extract_file(
            dataset_name,
            name,
            file,
            download_dir,
            dl_progress,
            force_download=force_download,
            force_extract=force_extract,
            max_retries=max_retries,
        )
        return (dataset_name, name, save_path)

    futures = []
    local_datasets = defaultdict(dict)
    task_id = global_progress.add_task("", total=total_nb_files)
    with Live(progress_group):
        with ThreadPoolExecutor() as executor:
            for dataset_name, files in datasets.items():
                for name, file in files.items():
                    assert isinstance(file, DataFile)
                    f = executor.submit(_worker, dataset_name, name, file)
                    futures.append(f)

            for future in as_completed(futures):
                dataset_name, name, save_path = future.result()
                local_datasets[dataset_name][name] = save_path
                global_progress.update(task_id, advance=1)

    return local_datasets
