import dataclasses
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import rich.progress
import typing_extensions as tp
from pydantic.dataclasses import dataclass as pydantic_dataclass

from st_atlas_datasets.settings import settings
from st_atlas_datasets.utils.data_file import DataFile, UriType
from st_atlas_datasets.utils.extract_manager import ExtractorManager
from st_atlas_datasets.utils.utils import create_download_progress


@pydantic_dataclass(kw_only=True)
class DownloadManager:
    download_policy: tp.Literal["always", "never", "missing"] = settings.DOWNLOAD_POLICY
    extract_policy: tp.Literal["always", "never", "missing"] = settings.EXTRCAT_POLICY
    validate_checksums: bool = settings.VALIDATE_CHECKSUMS
    data_dir: Path = settings.DATA_DIR
    disable_pbar: bool = settings.DISABLE_PROGRESS_BAR
    max_workers: int | None = settings.MAX_THREADS
    io_buffer_size: int = 8192

    _download_dir: Path = dataclasses.field(init=False)
    _progress: rich.progress.Progress = dataclasses.field(init=False)
    _executor: ThreadPoolExecutor = dataclasses.field(init=False)

    def __post_init__(self):
        self.data_dir = self.data_dir.expanduser().resolve()
        self._download_dir = self.data_dir / "downloads"
        self._extract_dir = self.data_dir / "extracted"
        self._progress = create_download_progress(disable=self.disable_pbar)
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="st_atlas_datasets.download_manager",
        )

    def __enter__(self) -> tp.Self:
        self._progress.__enter__()
        self._executor.__enter__()
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self._progress.__exit__(*args, **kwargs)
        self._executor.__exit__(*args, **kwargs)

    def download(self, uri: UriType) -> DataFile:
        file = DataFile.parse(uri)
        if file.is_local_file():
            return file

        if self.download_policy == "never":
            raise ValueError(
                f"Download policy is set to 'never', and {file} is not a local file."
            )

        save_path = file.build_local_filepath(self._download_dir)
        if save_path.is_file() and self.download_policy == "missing":
            return file.copy_with(uri=str(save_path))

        save_path.parent.mkdir(parents=True, exist_ok=True)
        with file.fs.open(file.uri, "rb") as fin:
            info: dict = fin.info()  # type: ignore
            total = info.get("size")

            task_id = self._progress.add_task(
                "",
                total=total,
                task_name="Download",
                filename=file.name,
            )
            with open(save_path, "wb") as fout:
                while chunk := fin.read(self.io_buffer_size):
                    fout.write(chunk)  # type: ignore
                    self._progress.update(task_id, advance=len(chunk))

        return file.copy_with(uri=str(save_path))

    def compute_md5sum(self, uri: UriType) -> DataFile:
        file = DataFile.parse(uri)
        if not file.is_local_file():
            raise ValueError(f"Cannot compute md5sum of non-local file {file}")

        md5sum_filepath = Path(f"{file.uri.rstrip(file.extention)}.md5sum")
        if md5sum_filepath.is_file():
            computed_md5sum = md5sum_filepath.read_text()
        else:
            task_id = self._progress.add_task(
                "",
                task_name="Compute MD5Sum",
                filename=file.name,
            )
            md5 = hashlib.md5()
            with self._progress.open(
                file.uri,
                mode="rb",
                buffering=self.io_buffer_size,
                task_id=task_id,
            ) as f:
                while chunk := f.read(self.io_buffer_size):
                    md5.update(chunk)
            computed_md5sum = md5.hexdigest()
            md5sum_filepath.write_text(computed_md5sum)

        if (
            file.md5sum is not None
            and self.validate_checksums
            and file.md5sum != computed_md5sum
        ):
            raise ValueError(
                f"MD5Sum of {uri} does not match expected value. "
                f"Expected: {file.md5sum}, Computed: {computed_md5sum}"
            )
        return file.copy_with(md5sum=computed_md5sum)

    def extract(self, uri: UriType) -> DataFile:
        file = DataFile.parse(uri)

        if self.extract_policy == "never":
            return file

        if not ExtractorManager.is_extractable_filepath(file.uri):
            return file

        head, tail = os.path.split(
            os.path.normpath(f"{file.uri.rstrip(file.extention)}")
        )

        extract_dir = Path(head) / "extracted" / tail
        # metadata file used for tracking extracted files
        metadata_filepath = extract_dir / "extracted_files.json"

        if self.extract_policy == "always" or not metadata_filepath.is_file():
            with ExtractorManager(file, progress=self._progress) as em:
                extracted_files = em.extract(extract_dir)

            # write metadata file
            metadata_filepath.parent.mkdir(parents=True, exist_ok=True)
            metadata_filepath.write_text(json.dumps(extracted_files, indent=2))

        return file.copy_with(uri=str(metadata_filepath), md5sum=None)

    def download_and_extract(
        self, uris: UriType | tp.Sequence[UriType]
    ) -> DataFile | list[DataFile]:
        def _worker(uri: UriType) -> DataFile:
            return self.extract(self.compute_md5sum(self.download(uri)))

        if isinstance(uris, (str, Path, DataFile)):
            return _worker(uris)

        return list(self._executor.map(_worker, uris))


if __name__ == "__main__":
    urls = [
        {
            "uri": "https://cf.10xgenomics.com/samples/spatial-exp/1.3.0/Visium_FFPE_Human_Breast_Cancer/Visium_FFPE_Human_Breast_Cancer_fastqs.tar",
            "md5sum": "a0a33519d17367a4488f946d412e2f73",
        },
        {
            "uri": "https://cf.10xgenomics.com/samples/spatial-exp/1.3.0/Visium_FFPE_Human_Breast_Cancer/Visium_FFPE_Human_Breast_Cancer_spatial.tar.gz",
            "md5sum": "9a6bc80f4c1e288db5e9bcd368d473c3",
        },
    ]
    with DownloadManager() as manager:
        manager.download_and_extract(urls)
