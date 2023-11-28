from pathlib import Path

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["Settings", "get_settings"]


class Settings(BaseSettings):
    CACHE_DIR: Path = Path("~/.cache/st-visium-datasets").expanduser()
    OVERWRITE_CACHE: bool = False

    @computed_field
    @property
    def DOWNLOAD_DIR(self) -> Path:
        return self.CACHE_DIR / "downloads"

    @computed_field
    @property
    def DATASETS_DIR(self) -> Path:
        return self.CACHE_DIR / "datasets"

    model_config = SettingsConfigDict(
        case_sensitive=True, env_prefix="ST_VISIUM_DATASETS_"
    )


def get_settings() -> Settings:
    return Settings()
