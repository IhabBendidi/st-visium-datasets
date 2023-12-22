from pydantic import BaseModel, ConfigDict, Field


class DataFile(BaseModel, frozen=True):
    url: str
    md5sum: str
    size: int = Field(..., alias="bytes")

    model_config = ConfigDict(populate_by_name=True)

    def __repr__(self) -> str:
        return f"DataFile('{self.url}')"

    def __str__(self) -> str:
        return str(self.url)
