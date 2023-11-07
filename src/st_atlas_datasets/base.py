from pathlib import Path

from st_atlas_datasets.utils.download import download_urls
from st_atlas_datasets.utils.utils import camelcase_to_snakecase


class BaseDatasetBuilder:
    _NAME: str | None = None
    _DATA_URLS: list[str] = []

    def __init__(
        self,
        base_data_dir: str | Path = "./data",
        overwrite: bool = False,
        disable_pbar: bool = False,
        max_worker_threads: int | None = None,
    ):
        self._overwrite = overwrite
        self._disable_pbar = disable_pbar
        self._max_worker_threads = max_worker_threads
        self._data_dir = Path(base_data_dir) / self.name()
        self._raw_data_dir = self._data_dir / "raw"
        self._raw_data_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def name(cls):
        if cls._NAME:
            return cls._NAME
        return camelcase_to_snakecase(cls.__name__)

    @classmethod
    def data_urls(cls) -> list[str]:
        if cls._DATA_URLS:
            return cls._DATA_URLS
        raise NotImplementedError(
            "Please either fill _DATA_URLS or override the 'data_urls' property"
        )

    def download_raw(self) -> list[Path]:
        return download_urls(
            self.data_urls(),
            save_dir=self._raw_data_dir,
            overwrite=self._overwrite,
            disable_pbar=self._disable_pbar,
            max_workers=self._max_worker_threads,
        )
