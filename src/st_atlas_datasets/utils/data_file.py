import dataclasses
import hashlib
from functools import cached_property
from pathlib import Path

import fsspec
import typing_extensions as tp
from pydantic import ConfigDict
from pydantic.dataclasses import dataclass as pydantic_dataclass


@pydantic_dataclass(kw_only=True, config=ConfigDict(arbitrary_types_allowed=True))
class DataFile:
    uri: str
    md5sum: str | None = None
    name: str = dataclasses.field(default_factory=str)

    _fs: fsspec.AbstractFileSystem = dataclasses.field(init=False, repr=False)
    _uri_path: Path = dataclasses.field(init=False, repr=False)

    def __post_init__(self):
        self._fs, _ = fsspec.core.url_to_fs(self.uri, mode="rb")
        self._uri_path = Path(self.uri)
        self.name = self.name or self._uri_path.name

    @cached_property
    def extention(self) -> str:
        return "".join(self._uri_path.suffixes)

    @cached_property
    def stem(self) -> str:
        return self.name.rstrip(self.extention)

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        return self._fs

    def build_local_filepath(self, local_dir: str | Path) -> Path:
        if self.is_local_file():
            return self._uri_path

        file_id = hashlib.md5(self.uri.encode()).hexdigest()
        filename = f"{self.stem}/{file_id}{self.extention}"
        return Path(local_dir) / filename

    def is_local_file(self) -> bool:
        if self._fs.protocol != ("file", "local"):
            return False
        if not Path(self.uri).is_file():
            raise FileNotFoundError(f"Local file '{self.uri}' does not exist.")
        return True

    def copy_with(self, **changes) -> "DataFile":
        return dataclasses.replace(self, **changes)

    @classmethod
    def parse(cls, item: "UriType") -> "DataFile":
        if isinstance(item, cls):
            return item
        if isinstance(item, (str, Path)):
            return cls(uri=str(item))
        if isinstance(item, dict):
            return cls(**item)
        raise ValueError(f"Cannot parse {item} into a DataFile")

    def __repr__(self) -> str:
        return f"DataFile('{self.uri}')"

    def __str__(self) -> str:
        return self.uri


UriType: tp.TypeAlias = tp.Union[str, Path, dict[str, str], DataFile]
