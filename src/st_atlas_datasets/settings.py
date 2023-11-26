from pathlib import Path

import typing_extensions as tp
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATA_DIR: Path = Path("./data").expanduser().resolve()
    DOWNLOAD_POLICY: tp.Literal["always", "never", "missing"] = "missing"
    EXTRCAT_POLICY: tp.Literal["always", "never", "missing"] = "missing"
    VALIDATE_CHECKSUMS: bool = True
    DISABLE_PROGRESS_BAR: bool = False
    MAX_THREADS: int | None = None

    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_prefix="ST_ATLAS_DATASETS_",
    )


settings = Settings()
