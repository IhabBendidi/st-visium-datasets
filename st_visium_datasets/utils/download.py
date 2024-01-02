from __future__ import annotations

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
    if save_path.is_file() and not force_download:
        return save_path

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
    return save_path


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
) -> Path:
    save_path = _download_file(
        dataset_name,
        name,
        file,
        download_dir,
        dl_progress,
        force_download=force_download,
        buffer_size=buffer_size,
    )
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
