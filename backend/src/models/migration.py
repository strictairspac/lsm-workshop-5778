from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, JSON, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from ..config.database import Base
from ..constants.enums import MIGRATION_STATUSES

# MySQL 默认排序规则不区分大小写；键区间与唯一键必须按字节序比较。
KEY_TYPE = String(64).with_variant(String(64, collation='utf8mb4_bin'), 'mysql')


class Migration(Base):
    """一次在线区间搬迁演练：初始分片、目标分片、路由与复制检查点。"""

    __tablename__ = 'migrations'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey('datasets.id'))
    status: Mapped[str] = mapped_column(String(20), default=MIGRATION_STATUSES[0])
    shard_count: Mapped[int] = mapped_column(Integer)
    range_start: Mapped[str] = mapped_column(KEY_TYPE)
    range_end: Mapped[str] = mapped_column(KEY_TYPE)
    source_shard_id: Mapped[int] = mapped_column(Integer)
    target_shard_id: Mapped[int] = mapped_column(Integer)
    routes: Mapped[list] = mapped_column(JSON)
    next_seq: Mapped[int] = mapped_column(Integer, default=0)
    copy_cursor: Mapped[str | None] = mapped_column(KEY_TYPE, nullable=True)
    copy_done: Mapped[bool] = mapped_column(default=False)
    copy_batches: Mapped[int] = mapped_column(Integer, default=0)
    copied_rows: Mapped[int] = mapped_column(Integer, default=0)
    history: Mapped[list] = mapped_column(JSON, default=list)
    switched_seq: Mapped[int | None] = mapped_column(Integer, nullable=True)
    switched_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class Shard(Base):
    __tablename__ = 'shards'
    id: Mapped[int] = mapped_column(primary_key=True)
    migration_id: Mapped[int] = mapped_column(ForeignKey('migrations.id'), index=True)
    name: Mapped[str] = mapped_column(String(80))
    kind: Mapped[str] = mapped_column(String(20))  # source / target
    ordinal: Mapped[int] = mapped_column(Integer)
    range_start: Mapped[str | None] = mapped_column(KEY_TYPE, nullable=True)
    range_end: Mapped[str | None] = mapped_column(KEY_TYPE, nullable=True)


class ShardVersion(Base):
    """分片内每个键的版本；墓碑也是版本，复制和查询都不能丢。"""

    __tablename__ = 'shard_versions'
    __table_args__ = (UniqueConstraint('shard_id', 'key', 'seq', name='uq_shard_version'),)
    id: Mapped[int] = mapped_column(primary_key=True)
    shard_id: Mapped[int] = mapped_column(ForeignKey('shards.id'), index=True)
    key: Mapped[str] = mapped_column(KEY_TYPE, index=True)
    seq: Mapped[int] = mapped_column(Integer)
    deleted: Mapped[bool] = mapped_column(default=False)
    value: Mapped[str | None] = mapped_column(String(200), nullable=True)


class MigrationWrite(Base):
    """按确认顺序记录每次写入，查询水位与“切换前后写入都保留”的证据。"""

    __tablename__ = 'migration_writes'
    __table_args__ = (UniqueConstraint('migration_id', 'seq', name='uq_migration_write_seq'),)
    id: Mapped[int] = mapped_column(primary_key=True)
    migration_id: Mapped[int] = mapped_column(ForeignKey('migrations.id'), index=True)
    seq: Mapped[int] = mapped_column(Integer)
    key: Mapped[str] = mapped_column(KEY_TYPE, index=True)
    op: Mapped[str] = mapped_column(String(10))
    value: Mapped[str | None] = mapped_column(String(200), nullable=True)
    shard_id: Mapped[int] = mapped_column(Integer)
    phase: Mapped[str] = mapped_column(String(20))  # seed / before_switch / after_switch
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
