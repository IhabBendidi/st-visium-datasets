import re

import rich.progress

_COMMON_SANITIZATIONS = {"FFPE": "ffpe", "CytAssist": "cytassist", "FASTQ": "fastq"}


def sanitize_with_sep(s: str, sep: str = "") -> str:
    # Replace common patterns
    for old, new in _COMMON_SANITIZATIONS.items():
        s = s.replace(old, new)
    # Replace camelCase with sep
    s = re.sub("(.)([A-Z][a-z]+)", r"\1{}\2".format(sep), s)
    s = re.sub("([a-z0-9])([A-Z])", r"\1{}\2".format(sep), s)
    s = s.lower()
    # Replace ' ', '.', '-', '_', '/', '(', ')', ''', ',' with sep
    s = re.sub(r"[ -._/,()\']", sep, s)
    # Remove leading _, -, sep
    s = s.strip("_").strip("-")
    # Deduplicate consecutive _, -
    s = re.sub(r"_{2,}", "_", s)
    s = re.sub(r"-{2,}", "-", s)
    return s


def sanitize_as_snake_case(s: str) -> str:
    return sanitize_with_sep(s, sep="_")


def create_download_progress(**kwargs) -> rich.progress.Progress:
    columns = (
        rich.progress.TextColumn("[bold blue][{task.fields[task_name]}][/bold blue]"),
        rich.progress.TextColumn("[bold green]{task.fields[filename]}[/bold green]"),
        rich.progress.BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        rich.progress.DownloadColumn(),
        "•",
        rich.progress.TransferSpeedColumn(),
        "•",
        rich.progress.TimeElapsedColumn(),
    )
    kwargs.setdefault("expand", True)
    return rich.progress.Progress(*columns, **kwargs)
