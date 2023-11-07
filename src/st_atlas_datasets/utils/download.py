import functools
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)


def _create_pbar(**kwargs) -> Progress:
    return Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
        **kwargs,
    )


def _download_url(
    url: str,
    save_dir: Path,
    overwrite: bool = False,
    progress: Progress | None = None,
    client: httpx.Client | None = None,
) -> Path:
    filename = Path(url).name
    save_path = save_dir / filename
    if save_path.is_file() and not overwrite:
        return save_path
    progress = progress or _create_pbar(disable=True)
    _streamer = client.stream if client else httpx.stream
    with open(save_path, "wb") as f:
        with _streamer("GET", url) as resp:
            total = int(resp.headers["Content-Length"])
            dl_task = progress.add_task(f"Download '{filename}'", total=total)
            with progress:
                for chunk in resp.iter_bytes():
                    f.write(chunk)
                    progress.update(dl_task, advance=len(chunk))
    return save_path


def download_urls(
    urls: list[str],
    save_dir: Path | str,
    overwrite: bool = False,
    max_workers: int | None = None,
    disable_pbar: bool = False,
) -> list[Path]:
    save_dir = Path(save_dir)
    progress = _create_pbar(disable=disable_pbar)
    client = httpx.Client()
    _worker = functools.partial(
        _download_url,
        save_dir=save_dir,
        overwrite=overwrite,
        progress=progress,
        client=client,
    )
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_worker, url) for url in urls]
        save_paths = list(f.result() for f in as_completed(futures))

    client.close()
    return save_paths
