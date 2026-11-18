"""在线键区间迁移演练。

分片与版本都是教学模型：MySQL 保存分片、每键版本与写入日志。路由切换和
目标分片补齐在同一个事务里提交；查询按发起时的已确认水位读取全部分片、
按版本号合并，因此旧副本不会让删键复活，复制中的重复版本也只计一次。

路由区间统一带显式的开闭标记（例如 [start, end] 或 (end, +∞)），切换时只把
闭区间交给目标分片，避免为 64 字符的键构造不存在的后继键。
"""
import hashlib
import json
import threading
from datetime import datetime, timezone

from fastapi import HTTPException
from sqlalchemy import func, select

from ..models.dataset import Dataset
from ..models.migration import Migration, MigrationWrite, Shard, ShardVersion

COPY_BATCH_KEYS = 3
STATUS_LABELS = {'pending': '待搬迁', 'copying': '复制中', 'switched': '已切换'}
STAGE_LABELS = {**STATUS_LABELS, 'ready': '复制完成 · 待切换'}

# MySQL 使用行锁串行化切换与写入；SQLite/单进程内再补一把锁，测试也走同一路径。
_LOCK = threading.RLock()


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _check_key(key, label='键'):
    if not isinstance(key, str) or not 1 <= len(key) <= 64:
        raise HTTPException(422, f'{label}长度必须在1到64之间')
    if '\x00' in key:
        raise HTTPException(422, f'{label}不能包含 NUL 字符')


def _contains(route, key):
    if key < route['start'] or (key == route['start'] and not route['start_inclusive']):
        return False
    if route['end'] is None:
        return True
    if key < route['end']:
        return True
    if key == route['end']:
        return route['end_inclusive']
    return False


def _router(routes):
    return sorted(routes, key=lambda r: (r['start'], not r['start_inclusive']))


def route_for_key(routes, key):
    for route in _router(routes):
        if _contains(route, key):
            return route
    raise HTTPException(500, '键没有可归属的分片区间')


def _covers(route, start, end):
    return _contains(route, start) and _contains(route, end)


def _boundaries(keys, count):
    """把有序键切成连续分片，返回每片起点（首片为空串覆盖更小的键）。"""
    total = len(keys)
    starts = ['']
    for i in range(1, count):
        starts.append(keys[(i * total) // count])
    return starts


def _build_routes(keys, shard_count):
    starts = _boundaries(keys, shard_count)
    routes = []
    for i, start in enumerate(starts):
        routes.append({
            'start': start, 'start_inclusive': True,
            'end': starts[i + 1] if i + 1 < len(starts) else None, 'end_inclusive': False,
            'shard_id': None,
        })
    return routes


def _carve(routes, start, end, target_id):
    """从旧路由切出闭区间 [start, end] 交给目标分片，其余区间原样保留。"""
    carved = []
    for route in _router(routes):
        if not _covers(route, start, end):
            carved.append(route)
            continue
        if route['start'] < start:
            carved.append({'start': route['start'], 'start_inclusive': route['start_inclusive'],
                           'end': start, 'end_inclusive': False, 'shard_id': route['shard_id']})
        if route['end'] is None or end < route['end'] or (end == route['end'] and route['end_inclusive']):
            carved.append({'start': end, 'start_inclusive': False,
                           'end': route['end'], 'end_inclusive': route['end_inclusive'],
                           'shard_id': route['shard_id']})
    carved.append({'start': start, 'start_inclusive': True, 'end': end, 'end_inclusive': True,
                   'shard_id': target_id})
    return _router(carved)


def _load_locked(db, ident):
    migration = db.scalar(select(Migration).where(Migration.id == ident).with_for_update())
    if not migration:
        raise HTTPException(404, '迁移不存在')
    return migration


def _shard(db, ident):
    shard = db.get(Shard, ident)
    if not shard:
        raise HTTPException(500, '迁移分片丢失')
    return shard


def _history(migration, event, **extra):
    migration.history = (migration.history or []) + [{
        'step': len(migration.history or []) + 1, 'event': event,
        'status': migration.status, **extra,
    }]


def create(db, data):
    if db.scalar(select(Migration).where(Migration.name == data.name)):
        raise HTTPException(409, '迁移名称已存在')
    dataset = db.get(Dataset, data.dataset_id)
    if not dataset:
        raise HTTPException(404, '写入批次不存在')
    entries = dataset.entries or []
    keys = sorted({e['key'] for e in entries})
    if not 2 <= data.shard_count <= 4:
        raise HTTPException(422, '分片数必须在2到4之间')
    if len(keys) < data.shard_count:
        raise HTTPException(422, '不同键数少于分片数，无法建立可写分片')
    for key in keys:
        _check_key(key, '批次键')
    _check_key(data.range_start, '区间起点')
    _check_key(data.range_end, '区间终点')
    if data.range_start > data.range_end:
        raise HTTPException(422, '区间起点必须不大于终点')
    if not any(data.range_start <= key <= data.range_end for key in keys):
        raise HTTPException(422, '区间内没有可搬迁的键')

    routes = _build_routes(keys, data.shard_count)
    source_route = route_for_key(routes, data.range_start)
    if not _covers(source_route, data.range_start, data.range_end):
        raise HTTPException(422, '键区间必须完整落在一个来源分片内')

    migration = Migration(
        name=data.name, dataset_id=data.dataset_id, status='pending',
        shard_count=len(routes), range_start=data.range_start, range_end=data.range_end,
        source_shard_id=0, target_shard_id=0, routes=[], next_seq=0,
        copy_cursor=None, copy_done=False, copy_batches=0, copied_rows=0,
        history=[], switched_seq=None, switched_at=None,
    )
    db.add(migration)
    db.flush()

    shard_ids = []
    for ordinal, route in enumerate(_router(routes)):
        shard = Shard(migration_id=migration.id, name=f'S{ordinal + 1}', kind='source', ordinal=ordinal,
                      range_start=route['start'], range_end=route['end'])
        db.add(shard)
        db.flush()
        route['shard_id'] = shard.id
        shard_ids.append(shard.id)
    target = Shard(migration_id=migration.id, name=f'S{len(shard_ids) + 1}', kind='target',
                   ordinal=len(shard_ids), range_start=data.range_start, range_end=data.range_end)
    db.add(target)
    db.flush()

    migration.source_shard_id = source_route['shard_id']
    migration.target_shard_id = target.id
    migration.routes = routes
    migration.history = [{
        'step': 1, 'event': 'migration_created', 'status': 'pending',
        'shards': len(shard_ids), 'source_shard_id': source_route['shard_id'],
        'target_shard_id': target.id, 'range': [data.range_start, data.range_end],
    }]

    seq = 0
    for entry in entries:
        seq += 1
        shard_id = route_for_key(routes, entry['key'])['shard_id']
        deleted = entry['op'] == 'delete'
        db.add(ShardVersion(shard_id=shard_id, key=entry['key'], seq=seq, deleted=deleted,
                            value=None if deleted else entry['value']))
        db.add(MigrationWrite(migration_id=migration.id, seq=seq, key=entry['key'], op=entry['op'],
                              value=None if deleted else entry['value'], shard_id=shard_id, phase='seed'))
    migration.next_seq = seq
    db.commit()
    return detail(db, migration.id)


def _brief(migration):
    stage = migration.status
    if stage == 'copying' and migration.copy_done:
        stage = 'ready'
    return {
        'id': migration.id, 'name': migration.name, 'dataset_id': migration.dataset_id,
        'status': migration.status, 'status_label': STATUS_LABELS.get(migration.status, migration.status),
        'stage': stage, 'stage_label': STAGE_LABELS.get(stage, stage),
        'shard_count': migration.shard_count, 'source_shard_id': migration.source_shard_id,
        'target_shard_id': migration.target_shard_id,
        'range_start': migration.range_start, 'range_end': migration.range_end,
        'watermark': migration.next_seq, 'copy_done': migration.copy_done,
        'copy_batches': migration.copy_batches, 'copied_rows': migration.copied_rows,
        'copy_cursor': migration.copy_cursor,
        'switched_seq': migration.switched_seq, 'created_at': str(migration.created_at),
    }


def listing(db):
    return [_brief(m) for m in db.scalars(select(Migration).order_by(Migration.id.desc()))]


def _route_view(routes, names, kinds=None):
    view = []
    for route in _router(routes):
        row = {'start': route['start'], 'start_inclusive': route['start_inclusive'],
               'end': route['end'], 'end_inclusive': route['end_inclusive'],
               'shard_id': route['shard_id'], 'shard_name': names.get(route['shard_id'])}
        if kinds is not None:
            row['shard_kind'] = kinds.get(route['shard_id'])
        view.append(row)
    return view


def detail(db, ident):
    migration = db.get(Migration, ident)
    if not migration:
        raise HTTPException(404, '迁移不存在')
    shards = db.scalars(select(Shard).where(Shard.migration_id == migration.id).order_by(Shard.ordinal)).all()
    names = {shard.id: shard.name for shard in shards}
    shard_ids = [shard.id for shard in shards]
    counts = dict(db.execute(
        select(ShardVersion.shard_id, func.count(ShardVersion.id)).where(
            ShardVersion.shard_id.in_(shard_ids)).group_by(ShardVersion.shard_id)).all())
    key_counts = dict(db.execute(
        select(ShardVersion.shard_id, func.count(func.distinct(ShardVersion.key))).where(
            ShardVersion.shard_id.in_(shard_ids)).group_by(ShardVersion.shard_id)).all())
    payload = _brief(migration)
    phases = dict(db.execute(
        select(MigrationWrite.phase, func.count(MigrationWrite.id)).where(
            MigrationWrite.migration_id == migration.id).group_by(MigrationWrite.phase)).all())
    payload.update({
        'routes': _route_view(migration.routes or [], names,
                              {shard.id: shard.kind for shard in shards}),
        'shards': [{
            'id': shard.id, 'name': shard.name, 'kind': shard.kind, 'ordinal': shard.ordinal,
            'range_start': shard.range_start, 'range_end': shard.range_end,
            'versions': counts.get(shard.id, 0), 'keys': key_counts.get(shard.id, 0),
        } for shard in shards],
        'switched_at': str(migration.switched_at) if migration.switched_at else None,
        'write_phases': phases,
        'history': migration.history or [],
    })
    return payload


def _copy_versions(db, migration, source_id, target_id, keys):
    if not keys:
        return 0
    versions = db.scalars(select(ShardVersion).where(
        ShardVersion.shard_id == source_id, ShardVersion.key.in_(keys)
    ).order_by(ShardVersion.key, ShardVersion.seq)).all()
    existing = set(db.execute(select(ShardVersion.key, ShardVersion.seq).where(
        ShardVersion.shard_id == target_id, ShardVersion.key.in_(keys))).all())
    inserted = 0
    for version in versions:
        if (version.key, version.seq) in existing:
            continue
        db.add(ShardVersion(shard_id=target_id, key=version.key, seq=version.seq,
                            deleted=version.deleted, value=version.value))
        existing.add((version.key, version.seq))
        inserted += 1
    return inserted


def _range_key_query(source_id, start, end):
    return select(ShardVersion.key).where(
        ShardVersion.shard_id == source_id,
        ShardVersion.key >= start, ShardVersion.key <= end,
    )


def _copy_batch(db, migration, source_id, target_id):
    statement = _range_key_query(source_id, migration.range_start, migration.range_end)
    if migration.copy_cursor is not None:
        statement = statement.where(ShardVersion.key > migration.copy_cursor)
    keys = list(db.scalars(statement.distinct().order_by(ShardVersion.key).limit(COPY_BATCH_KEYS)))
    if not keys:
        migration.copy_done = True
        _history(migration, 'copy_complete', cursor=migration.copy_cursor,
                 copied_rows=migration.copied_rows)
        return {'copied_keys': [], 'inserted': 0, 'done': True}
    inserted = _copy_versions(db, migration, source_id, target_id, keys)
    migration.copy_cursor = keys[-1]
    migration.copied_rows += inserted
    migration.copy_batches += 1
    _history(migration, 'copy_batch', cursor=keys[-1], keys=len(keys), inserted=inserted,
             copied_rows=migration.copied_rows)
    return {'copied_keys': keys, 'inserted': inserted, 'done': False}


def _start_copying(migration):
    if migration.status == 'pending':
        migration.status = 'copying'
        _history(migration, 'copy_started')


def advance(db, ident):
    """推进一个复制批次：待搬迁 → 复制中；到达末尾只标记完成，不自动切换。"""
    with _LOCK:
        migration = _load_locked(db, ident)
        if migration.status == 'switched':
            db.rollback()
            return {**detail(db, ident), 'advance': {'copied_keys': [], 'inserted': 0, 'done': True,
                                                     'noop': True}}
        _start_copying(migration)
        if migration.copy_done:
            db.commit()
            return {**detail(db, ident), 'advance': {'copied_keys': [], 'inserted': 0, 'done': True,
                                                     'noop': True}}
        result = _copy_batch(db, migration, migration.source_shard_id, migration.target_shard_id)
        db.commit()
        return {**detail(db, ident), 'advance': result}


def resume(db, ident, max_batches=1000):
    """中断后续迁：从持久化游标继续；已复制的键/版本不重复写入。"""
    with _LOCK:
        migration = _load_locked(db, ident)
        if migration.status == 'switched':
            db.rollback()
            return {**detail(db, ident), 'resume': {'batches': 0, 'copy_done': True,
                                                    'cursor': migration.copy_cursor, 'noop': True}}
        _start_copying(migration)
        batches = 0
        while not migration.copy_done and batches < max_batches:
            _copy_batch(db, migration, migration.source_shard_id, migration.target_shard_id)
            batches += 1
        _history(migration, 'resume_finished', batches=batches, cursor=migration.copy_cursor,
                 copy_done=migration.copy_done)
        db.commit()
        return {**detail(db, ident), 'resume': {'batches': batches, 'copy_done': migration.copy_done,
                                                'cursor': migration.copy_cursor}}


def switch(db, ident):
    """复制完成后在同一事务里补齐目标并切换路由，返回最新归属。"""
    with _LOCK:
        migration = _load_locked(db, ident)
        if migration.status == 'switched':
            db.rollback()
            return {**detail(db, ident), 'switch': {'noop': True, 'catch_up_versions': 0}}
        if not migration.copy_done:
            raise HTTPException(409, '复制尚未完成，不能切换路由')
        keys = list(db.scalars(_range_key_query(
            migration.source_shard_id, migration.range_start, migration.range_end).distinct()))
        # 复制完成后、切换提交前落到来源分片的新版本，必须在同一事务里补齐。
        catch_up = _copy_versions(db, migration, migration.source_shard_id,
                                  migration.target_shard_id, keys)
        migration.routes = _carve(migration.routes, migration.range_start, migration.range_end,
                                  migration.target_shard_id)
        migration.status = 'switched'
        migration.switched_seq = migration.next_seq
        migration.switched_at = _now()
        _history(migration, 'switch_committed', catch_up_versions=catch_up,
                 switched_seq=migration.switched_seq)
        db.commit()
        return {**detail(db, ident), 'switch': {'noop': False, 'catch_up_versions': catch_up,
                                                'switched_seq': migration.switched_seq}}


def write(db, ident, data):
    """按当前路由写入分片；与切换共用迁移行锁，切换前后写入都保留。"""
    _check_key(data.key)
    if data.op == 'put' and (not isinstance(data.value, str) or len(data.value) > 200):
        raise HTTPException(422, 'put 必须包含不超过200字的字符串 value')
    with _LOCK:
        migration = _load_locked(db, ident)
        shard = _shard(db, route_for_key(migration.routes, data.key)['shard_id'])
        seq = migration.next_seq + 1
        deleted = data.op == 'delete'
        phase = 'after_switch' if migration.status == 'switched' else 'before_switch'
        db.add(ShardVersion(shard_id=shard.id, key=data.key, seq=seq, deleted=deleted,
                            value=None if deleted else data.value))
        db.add(MigrationWrite(migration_id=migration.id, seq=seq, key=data.key, op=data.op,
                              value=None if deleted else data.value, shard_id=shard.id, phase=phase))
        migration.next_seq = seq
        db.commit()
        return {
            'migration_id': migration.id, 'seq': seq, 'key': data.key, 'op': data.op,
            'value': None if deleted else data.value, 'shard_id': shard.id, 'shard_name': shard.name,
            'phase': phase, 'status': migration.status, 'watermark': seq,
        }


def _oracle(db, migration, start, end, watermark):
    rows = db.scalars(select(MigrationWrite).where(
        MigrationWrite.migration_id == migration.id,
        MigrationWrite.key >= start, MigrationWrite.key <= end,
        MigrationWrite.seq <= watermark,
    ).order_by(MigrationWrite.seq)).all()
    latest = {}
    for row in rows:
        latest[row.key] = row
    return {key: row.value for key, row in sorted(latest.items()) if row.op != 'delete'}


def query(db, ident, start=None, end=None):
    """按发起时已确认水位返回跨分片、去重、有序的完整快照。"""
    migration = db.get(Migration, ident)
    if not migration:
        raise HTTPException(404, '迁移不存在')
    start = migration.range_start if start is None else start
    end = migration.range_end if end is None else end
    _check_key(start, '查询起点')
    _check_key(end, '查询终点')
    if start > end:
        raise HTTPException(422, '查询起点必须不大于终点')
    # 先固定水位，再读取版本；并发写入即使提交也因 seq 越界而不进入本次结果。
    watermark = migration.next_seq

    rows = db.execute(select(ShardVersion).join(
        Shard, Shard.id == ShardVersion.shard_id
    ).where(
        Shard.migration_id == migration.id,
        ShardVersion.key >= start, ShardVersion.key <= end,
        ShardVersion.seq <= watermark,
    ).order_by(ShardVersion.key, ShardVersion.seq, ShardVersion.shard_id)).scalars().all()

    shards = db.scalars(select(Shard).where(Shard.migration_id == migration.id)).all()
    names = {shard.id: shard.name for shard in shards}
    kinds = {shard.id: shard.kind for shard in shards}

    latest = {}
    locations = {}
    for version in rows:
        current = latest.get(version.key)
        if current is None or version.seq > current.seq:
            latest[version.key] = version
        locations.setdefault(version.key, []).append(
            {'shard_id': version.shard_id, 'shard_name': names.get(version.shard_id),
             'seq': version.seq, 'deleted': version.deleted})

    result = []
    tombstones = 0
    for key in sorted(latest):
        version = latest[key]
        if version.deleted:
            tombstones += 1
            continue
        owner = route_for_key(migration.routes, key)
        result.append({
            'key': key, 'value': version.value, 'seq': version.seq,
            'owner_shard_id': owner['shard_id'], 'owner_shard_name': names.get(owner['shard_id']),
            'owner_shard_kind': kinds.get(owner['shard_id']),
            'version_shard_id': version.shard_id, 'version_shard_name': names.get(version.shard_id),
            'version_shard_kind': kinds.get(version.shard_id),
            'replicated_copies': len(locations[key]),
        })
    actual = {row['key']: row['value'] for row in result}
    oracle = _oracle(db, migration, start, end, watermark)
    if actual != oracle:
        raise HTTPException(500, '查询快照与写入日志 Oracle 不一致')
    checksum = hashlib.sha256(json.dumps(actual, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    return {
        'migration_id': migration.id, 'status': migration.status,
        'status_label': STATUS_LABELS.get(migration.status, migration.status),
        'watermark': watermark, 'start': start, 'end': end,
        'rows': result, 'count': len(result), 'ordered': True,
        'ordered_keys': [row['key'] for row in result],
        'scanned_versions': len(rows), 'duplicates_merged': len(rows) - len(latest),
        'tombstones_applied': tombstones, 'verified': True, 'checksum': checksum,
        'shards_read': sorted({version.shard_id for version in rows}),
        'shards_read_names': sorted({names.get(version.shard_id) for version in rows}),
        'ownership': _route_view(migration.routes or [], names, kinds),
    }


def ownership(db, ident):
    migration = db.get(Migration, ident)
    if not migration:
        raise HTTPException(404, '迁移不存在')
    shards = db.scalars(select(Shard).where(Shard.migration_id == migration.id).order_by(Shard.ordinal)).all()
    names = {shard.id: shard.name for shard in shards}
    kinds = {shard.id: shard.kind for shard in shards}
    counts = dict(db.execute(
        select(ShardVersion.shard_id, func.count(ShardVersion.id)).where(
            ShardVersion.shard_id.in_([shard.id for shard in shards])
        ).group_by(ShardVersion.shard_id)).all())
    range_owner = route_for_key(migration.routes, migration.range_start)
    return {
        'migration_id': migration.id, 'status': migration.status,
        'status_label': STATUS_LABELS.get(migration.status, migration.status),
        'range': {'start': migration.range_start, 'end': migration.range_end,
                  'owner_shard_id': range_owner['shard_id'],
                  'owner_shard_name': names.get(range_owner['shard_id']),
                  'owner_shard_kind': kinds.get(range_owner['shard_id'])},
        'routes': _route_view(migration.routes or [], names, kinds),
        'shards': [{
            'id': shard.id, 'name': shard.name, 'kind': shard.kind,
            'range_start': shard.range_start, 'range_end': shard.range_end,
            'versions': counts.get(shard.id, 0),
            'ownership': ('已切换归属' if shard.kind == 'target' and migration.status == 'switched'
                          else '复制副本 · 不接管路由' if shard.kind == 'target' else '当前持有'),
        } for shard in shards],
        'target_staged_versions': counts.get(migration.target_shard_id, 0),
        'watermark': migration.next_seq,
        'copy_done': migration.copy_done,
    }
