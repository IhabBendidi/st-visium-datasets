import dataclasses as dc
from pathlib import Path

import datasets

datasets.DatasetBuilder
datasets.DownloadManager


@dc.dataclass(frozen=True, kw_only=True, slots=True)
class FastqSeq:
    seq_id: str
    seq: str
    qual: str


def read_fastq(filepath: str | Path):
    pass
