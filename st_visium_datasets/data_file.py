import hashlib
import logging
from functools import cached_property
from pathlib import Path

import fsspec
import typing_extensions as tx
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, PrivateAttr

from st_visium_datasets.settings import Settings, get_settings

logger = logging.getLogger(__name__)


class DataFile(BaseModel):
    url: HttpUrl
    md5sum: str
    size: int = Field(..., gt=0, alias="bytes")

    settings: Settings = Field(default_factory=get_settings, repr=False, exclude=True)

    _local_path: Path | None = PrivateAttr(None)
    _fs: fsspec.AbstractFileSystem | None = PrivateAttr(None)

    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )

    @property
    def local_path(self) -> Path:
        if self._local_path is None:
            raise ValueError("DataFile is not downloaded yet.")
        return self._local_path

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        if self._fs is None:
            self._fs, _ = fsspec.core.url_to_fs(self.url, mode="rb")
        return self._fs

    @cached_property
    def extention(self) -> str:
        if not self.url.path:
            return ""
        return "".join(Path(self.url.path).suffixes)

    @cached_property
    def name(self) -> str:
        file_id = self.md5sum or hashlib.md5(str(self.url).encode()).hexdigest()
        if not self.url.path:
            return file_id
        return f"{self.url.path.strip('/')}/{file_id}"

    @cached_property
    def filename(self) -> str:
        return f"{self.name}{self.extention}"

    def download(self, *, buffer_size: int = 8192) -> tx.Self:
        local_filepath = self.settings.DOWNLOAD_DIR / self.filename
        if self.settings.OVERWRITE_CACHE or not local_filepath.is_file():
            local_filepath.parent.mkdir(parents=True, exist_ok=True)
            # download the file using fsspec
            md5 = hashlib.md5()
            with self.fs.open(self.url, "rb") as fin:
                with open(local_filepath, "wb") as fout:
                    while chunk := fin.read(buffer_size):
                        fout.write(chunk)  # type: ignore
                        md5.update(chunk)  # type: ignore
            # validate checksum
            md5sum = md5.hexdigest()
            if self.md5sum and md5sum != self.md5sum:
                raise ValueError(
                    f"MD5Sum of downloaded file {md5sum} does not match "
                    f"expected {self.md5sum}"
                )

        self._local_path = local_filepath
        return self

    def __repr__(self) -> str:
        return f"DataFile('{self.url}')"

    def __str__(self) -> str:
        return str(self.url)
