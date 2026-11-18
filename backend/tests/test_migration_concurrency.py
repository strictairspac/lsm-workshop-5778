"""并发写入与切换相撞：线程内直接调用服务层，写入与切换共用迁移行锁。"""
import threading

from sqlalchemy import func, select

from src.config.database import Session
from src.models.dataset import Dataset
from src.models.migration import Migration, ShardVersion
from src.schemas.migration import MigrationInput, ShardWriteInput
from src.services import migration as service


def _setup(name, start='k07', end='k10'):
    with Session() as db:
        dataset = db.scalar(select(Dataset).where(Dataset.name == '迁移演练集'))
        created = service.create(db, MigrationInput(
            name=name, dataset_id=dataset.id, shard_count=3, range_start=start, range_end=end))
        return created['id']


def _drain(ident):
    while True:
        with Session() as db:
            payload = service.advance(db, ident)
        if payload['advance']['done']:
            return


def test_parallel_writes_and_switch_keep_every_acknowledged_write():
    ident = _setup('并发切换相撞')
    _drain(ident)

    confirmed = []
    errors = []
    barrier = threading.Barrier(6)

    def writer(index):
        barrier.wait()
        try:
            key = f'k{7 + index % 4:02d}'
            with Session() as db:
                result = service.write(db, ident, ShardWriteInput(
                    key=key, op='put', value=f'writer-{index}'))
            confirmed.append((result['seq'], key, f'writer-{index}'))
        except Exception as exc:  # pragma: no cover - 断言输出更直观
            errors.append(exc)

    def switcher():
        barrier.wait()
        with Session() as db:
            service.switch(db, ident)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(5)]
    threads.append(threading.Thread(target=switcher))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert len(confirmed) == 5
    assert len({seq for seq, _, _ in confirmed}) == 5

    # 每条已确认写入都必须出现在切换后的快照里；同一键按序号取最后一条。
    keys_written = sorted({key for _, key, _ in confirmed})
    with Session() as db:
        migration = db.get(Migration, ident)
        assert migration.status == 'switched'
        snapshot = service.query(db, ident, 'k07', 'k10')
        target_versions = db.scalar(select(func.count(ShardVersion.id)).where(
            ShardVersion.shard_id == migration.target_shard_id))
        target_written = dict(db.execute(select(ShardVersion.key, func.count(ShardVersion.id)).where(
            ShardVersion.shard_id == migration.target_shard_id,
            ShardVersion.key.in_(keys_written)).group_by(ShardVersion.key)).all())
    values = {row['key']: (row['value'], row['seq']) for row in snapshot['rows']}
    expected = {key: (value, seq) for seq, key, value in confirmed}
    for key, (value, seq) in expected.items():
        assert values[key][0] == value
        assert target_written.get(key, 0) >= 1, f'{key} 未搬到目标分片'
    assert snapshot['verified'] is True
    assert snapshot['count'] == len(values)
    assert [row['key'] for row in snapshot['rows']] == sorted(values)
    # 切换后目标分片持有区间内全部版本，没有丢写。
    assert target_versions >= 5


def test_query_while_copying_returns_consistent_snapshot():
    ident = _setup('复制中并发查询')
    stop = threading.Event()

    def writer():
        index = 0
        while not stop.is_set() and index < 40:
            index += 1
            with Session() as db:
                service.write(db, ident, ShardWriteInput(
                    key='k08', op='put', value=f'v{index}'))

    thread = threading.Thread(target=writer)
    thread.start()
    try:
        for _ in range(6):
            with Session() as db:
                snapshot = service.query(db, ident, 'k07', 'k10')
                assert snapshot['verified'] is True
                assert snapshot['watermark'] >= 10
                assert snapshot['ordered_keys'] == sorted(snapshot['ordered_keys'])
            with Session() as db:
                service.advance(db, ident)
    finally:
        stop.set()
        thread.join()

    with Session() as db:
        while not service.advance(db, ident)['advance']['done']:
            pass
        switched = service.switch(db, ident)
        assert switched['status'] == 'switched'
        final = service.query(db, ident, 'k07', 'k10')
    assert final['verified'] is True
    assert all(row['replicated_copies'] >= 1 for row in final['rows'])
    with Session() as db:
        writes = db.scalar(select(func.count(ShardVersion.id)).where(ShardVersion.key == 'k08'))
    assert writes >= 2
