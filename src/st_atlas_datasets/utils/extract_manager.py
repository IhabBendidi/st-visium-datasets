import os
import tarfile
import zipfile
from abc import ABC, abstractmethod
from pathlib import Path

import rich.progress
import typing_extensions as tp

from st_atlas_datasets.utils.data_file import DataFile, UriType
from st_atlas_datasets.utils.utils import create_download_progress


class BaseExtractor(ABC):
    VALID_EXTENSIONS: list[str] = []

    @classmethod
    @abstractmethod
    def is_extractable(cls, fileobj: tp.IO[bytes]) -> bool:
        pass

    @classmethod
    @abstractmethod
    def _extract(cls, fileobj: tp.IO[bytes], extract_dir: Path) -> list[str]:
        pass

    @classmethod
    def extract(cls, fileobj: tp.IO[bytes], extract_dir: str | Path) -> list[str]:
        extract_dir = Path(extract_dir)
        extract_dir.mkdir(parents=True, exist_ok=True)
        return cls._extract(fileobj, extract_dir)


class TarExtractor(BaseExtractor):
    VALID_EXTENSIONS = [
        ".tar",
        ".tar.gz",
        ".tgz",
        ".tar.bz2",
        ".tbz2",
        ".tar.xz",
        ".txz",
    ]

    @classmethod
    def is_extractable(cls, fileobj: tp.IO[bytes]) -> bool:
        return tarfile.is_tarfile(fileobj)

    @classmethod
    def _extract(cls, fileobj: tp.IO[bytes], extract_dir: Path) -> list[str]:
        with tarfile.open(fileobj=fileobj) as tar:
            members = tar.getmembers()
            tar.extractall(extract_dir)
            return [m.name for m in members if m.isfile()]


class ZipExtractor(BaseExtractor):
    VALID_EXTENSIONS = [".zip"]

    @classmethod
    def is_extractable(cls, fileobj: tp.IO[bytes]) -> bool:
        return zipfile.is_zipfile(fileobj)

    @classmethod
    def _extract(cls, fileobj: tp.IO[bytes], extract_dir: Path) -> list[str]:
        with zipfile.ZipFile(fileobj) as zip:
            zip.extractall(extract_dir)
            infos = zip.infolist()
            return [info.filename for info in infos if not info.is_dir()]


class NotExtractable(BaseExtractor):
    VALID_EXTENSIONS = []

    @classmethod
    def is_extractable(cls, fileobj: tp.IO[bytes]) -> bool:
        return False

    @classmethod
    def _extract(cls, fileobj: tp.IO[bytes], extract_dir: Path):
        raise NotImplementedError("File is not extractable.")


class ExtractorManager:
    _EXTRACTORS: list[tp.Type[BaseExtractor]] = [TarExtractor, ZipExtractor]

    def __init__(
        self,
        data_file: UriType,
        progress: rich.progress.Progress | None = None,
    ):
        self._data_file = DataFile.parse(data_file)

        if not self._data_file.is_local_file():
            raise ValueError(
                f"Cannot extract '{self._data_file}', only local files are extractable."
            )

        if not self._data_file.md5sum:
            raise ValueError(f"Missing md5sum for '{self._data_file}'")

        self._progress = progress or create_download_progress(disable=True)
        self._fileobj: tp.IO[bytes] | None = None
        self._extractor: tp.Type[BaseExtractor] | None = None

    def __enter__(self) -> tp.Self:
        task_id = self._progress.add_task(
            "",
            task_name="Extract",
            filename=self._data_file.name,
        )
        self._fileobj = self._progress.open(self._data_file.uri, "rb", task_id=task_id)
        self._extractor = self.get_extractor()
        return self

    def __exit__(self, *args, **kwargs) -> None:
        if self._fileobj is not None:
            self._fileobj.close()

    @property
    def fileobj(self) -> tp.IO[bytes]:
        if self._fileobj is None:
            raise ValueError("ExtractorManager not in context.")
        return self._fileobj

    @property
    def extractor(self) -> tp.Type[BaseExtractor]:
        if self._extractor is None:
            raise ValueError("ExtractorManager not in context.")
        return self._extractor

    def get_extractor(self) -> tp.Type[BaseExtractor]:
        # check for extensions first
        for extractor in self._EXTRACTORS:
            if self._data_file.uri.lower().endswith(tuple(extractor.VALID_EXTENSIONS)):
                return extractor

        # check for magic bytes
        for extractor in self._EXTRACTORS:
            if extractor.is_extractable(self.fileobj):
                return extractor

        return NotExtractable

    def is_extractable(self) -> bool:
        return self.extractor is not NotExtractable

    def extract(self, extract_dir: str | Path) -> dict[str, str]:
        members = self.extractor.extract(self.fileobj, extract_dir)
        return {m: os.path.join(extract_dir, m) for m in members}
