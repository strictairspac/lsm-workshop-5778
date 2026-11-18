from typing import Literal

from pydantic import BaseModel, Field


class MigrationInput(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    dataset_id: int = Field(gt=0)
    shard_count: int = Field(ge=2, le=4)
    range_start: str = Field(min_length=1, max_length=64)
    range_end: str = Field(min_length=1, max_length=64)


class ShardWriteInput(BaseModel):
    key: str = Field(min_length=1, max_length=64)
    op: Literal['put', 'delete']
    value: str | None = Field(default=None, max_length=200)
