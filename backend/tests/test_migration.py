"""在线键区间迁移的验收测试：复制删键、跨分片查询、切换相撞、重启续迁。"""
from tests.conftest import advance_until_done, auth, login, make_migration, query, write

# 默认演练：10 个键分 3 片，来源分片 S3 覆盖 [k07, +∞)，搬迁 k07..k10 四键。


def test_create_builds_writable_shards(client, token):
    migration = make_migration(client, token, '建立分片')
    assert migration['status'] == 'pending'
    assert migration['status_label'] == '待搬迁'
    assert [s['kind'] for s in migration['shards']].count('source') == 3
    assert migration['shards'][-1]['kind'] == 'target'
    assert migration['range_start'] == 'k07' and migration['range_end'] == 'k10'
    assert migration['watermark'] == 10
    assert migration['copy_batches'] == 0 and migration['copy_done'] is False

    source = next(s for s in migration['shards'] if s['id'] == migration['source_shard_id'])
    assert source['range_start'] <= 'k07' and source['range_end'] is None

    # 待搬迁时也能覆盖、删键和范围查询；所有来源分片都可写。
    write(client, token, migration['id'], 'k08', 'put', 'seed-cover')
    write(client, token, migration['id'], 'k02', 'put', 'other-shard')
    snapshot = query(client, token, migration['id'], 'k01', 'k09')
    values = {row['key']: row['value'] for row in snapshot['rows']}
    assert list(values) == ['k01', 'k02', 'k03', 'k04', 'k05', 'k06', 'k07', 'k08', 'k09']
    assert values['k08'] == 'seed-cover' and values['k02'] == 'other-shard'
    assert len({row['owner_shard_id'] for row in snapshot['rows']}) == 3
    assert snapshot['verified'] is True


def test_replication_keeps_tombstone_and_old_copy_cannot_resurrect(client, token):
    migration = make_migration(client, token, '复制删键')
    ident = migration['id']
    write(client, token, ident, 'k08', 'delete')

    advanced = client.post(f'/migrations/{ident}/advance', headers=auth(token)).json()
    assert advanced['status'] == 'copying'
    advance_until_done(client, token, ident)

    # 切换前查询：目标副本已带墓碑，但归属仍在来源分片。
    prefixes = query(client, token, ident, 'k07', 'k08')
    assert [row['key'] for row in prefixes['rows']] == ['k07']
    assert prefixes['tombstones_applied'] == 1
    assert next(r for r in prefixes['rows'] if r['key'] == 'k07')['owner_shard_kind'] == 'source'

    switched = client.post(f'/migrations/{ident}/switch', headers=auth(token))
    assert switched.status_code == 200, switched.text
    assert switched.json()['status'] == 'switched'

    snapshot = query(client, token, ident, 'k07', 'k10')
    assert snapshot['ordered_keys'] == ['k07', 'k09', 'k10']
    assert 'k08' not in snapshot['ordered_keys']
    assert snapshot['rows'][0]['owner_shard_kind'] == 'target'
    # 旧副本仍留着 put 版本（供切换竞态合并），但墓碑按最高序号胜出。
    detail = client.get(f'/migrations/{ident}', headers=auth(token)).json()
    source = next(s for s in detail['shards'] if s['id'] == detail['source_shard_id'])
    target = next(s for s in detail['shards'] if s['id'] == detail['target_shard_id'])
    assert source['versions'] == target['versions'] == 5  # 4 个种子版本 + 1 个墓碑
    assert snapshot['verified'] is True


def test_query_spans_shards_ordered_without_duplicates(client, token):
    migration = make_migration(client, token, '跨分片查询', start='k07', end='k09')
    ident = migration['id']
    write(client, token, ident, 'k01', 'put', 'outside')
    write(client, token, ident, 'k03', 'delete')
    advance_until_done(client, token, ident)
    client.post(f'/migrations/{ident}/switch', headers=auth(token))

    full = query(client, token, ident, 'k01', 'k10')
    assert full['ordered_keys'] == ['k01', 'k02', 'k04', 'k05', 'k06', 'k07', 'k08', 'k09', 'k10']
    assert full['verified'] is True
    assert len(full['ordered_keys']) == len(set(full['ordered_keys']))
    assert full['tombstones_applied'] == 1
    assert len(full['ownership']) >= 4
    assert any(row['shard_kind'] == 'target' for row in full['ownership'])
    target_name = next(row['shard_name'] for row in full['ownership'] if row['shard_kind'] == 'target')
    assert target_name in full['shards_read_names']

    narrow = query(client, token, ident, 'k06', 'k08')
    assert narrow['ordered_keys'] == ['k06', 'k07', 'k08']
    assert narrow['ordered_keys'] == sorted(narrow['ordered_keys'])
    assert target_name in narrow['shards_read_names']


def test_query_uses_launch_watermark(client, token):
    migration = make_migration(client, token, '查询水位')
    ident = migration['id']
    first = query(client, token, ident, 'k07', 'k10')
    mark = first['watermark']
    write(client, token, ident, 'k09', 'put', 'later')
    second = query(client, token, ident, 'k07', 'k10')
    assert mark < second['watermark']
    assert {row['key']: row['value'] for row in first['rows']}['k09'] == 'v9'
    assert {row['key']: row['value'] for row in second['rows']}['k09'] == 'later'


def test_concurrent_writes_around_switch_are_all_kept(client, token):
    migration = make_migration(client, token, '切换相撞')
    ident = migration['id']
    before = write(client, token, ident, 'k08', 'put', 'before-switch')
    assert before['phase'] == 'before_switch'
    assert before['status'] == 'pending'
    advance_until_done(client, token, ident)

    # 复制完成、路由未切换时写入：仍进来源分片，切换事务必须把它补齐。
    late = write(client, token, ident, 'k07', 'put', 'late-before-switch')
    assert late['phase'] == 'before_switch'

    switched = client.post(f'/migrations/{ident}/switch', headers=auth(token)).json()
    assert switched['status'] == 'switched'
    assert switched['switch']['catch_up_versions'] >= 1

    after = write(client, token, ident, 'k08', 'put', 'after-switch')
    assert after['phase'] == 'after_switch'
    assert after['shard_id'] == switched['target_shard_id']
    removed = write(client, token, ident, 'k09', 'delete')
    assert removed['shard_id'] == switched['target_shard_id']

    snapshot = query(client, token, ident, 'k07', 'k10')
    values = {row['key']: row['value'] for row in snapshot['rows']}
    assert values == {'k07': 'late-before-switch', 'k08': 'after-switch', 'k10': 'v10'}
    assert snapshot['verified'] is True

    # 切换前的写入在切换时补齐到目标，不丢失也不重复。
    detail = client.get(f'/migrations/{ident}', headers=auth(token)).json()
    source = next(s for s in detail['shards'] if s['id'] == detail['source_shard_id'])
    target = next(s for s in detail['shards'] if s['id'] == detail['target_shard_id'])
    assert target['versions'] >= source['versions']
    events = [h['event'] for h in detail['history']]
    assert 'copy_batch' in events and 'switch_committed' in events


def test_switch_before_copy_done_is_rejected(client, token):
    migration = make_migration(client, token, '提前切换')
    response = client.post(f"/migrations/{migration['id']}/switch", headers=auth(token))
    assert response.status_code == 409
    assert '复制尚未完成' in response.json()['detail']

    advance_until_done(client, token, migration['id'])
    # 复制完成但未切换时，归属仍属于来源分片。
    detail = client.get(f"/migrations/{migration['id']}", headers=auth(token)).json()
    assert detail['status'] == 'copying'
    assert detail['stage'] == 'ready' and detail['stage_label'] == '复制完成 · 待切换'
    ownership = client.get(f"/migrations/{migration['id']}/ownership", headers=auth(token)).json()
    assert ownership['status'] == 'copying'
    assert ownership['range']['owner_shard_kind'] == 'source'
    assert ownership['target_staged_versions'] > 0


def test_resume_after_interruption_does_not_duplicate(client, token):
    migration = make_migration(client, token, '中断续迁')
    ident = migration['id']
    partial = client.post(f'/migrations/{ident}/advance', headers=auth(token)).json()
    assert partial['status'] == 'copying'
    assert partial['copied_rows'] == 3
    assert partial['copy_cursor'] == 'k09'
    assert partial['copy_done'] is False

    # 模拟进程重启：源分片在游标之后又有新写入，resume 必须补齐。
    write(client, token, ident, 'k10', 'delete')
    resumed = client.post(f'/migrations/{ident}/resume', headers=auth(token)).json()
    assert resumed['resume']['copy_done'] is True
    assert resumed['resume']['cursor'] == 'k10'
    client.post(f'/migrations/{ident}/switch', headers=auth(token))
    snapshot = query(client, token, ident, 'k07', 'k10')
    assert snapshot['ordered_keys'] == ['k07', 'k08', 'k09']
    # 来源分片保留旧副本，但查询按键取最高版本，每个键只输出一次。
    assert snapshot['scanned_versions'] == 10
    assert snapshot['duplicates_merged'] == snapshot['scanned_versions'] - 4  # k07,k08,k09,k10 四个键
    assert all(row['replicated_copies'] >= 2 for row in snapshot['rows'])

    # 再次 resume/switch 幂等，不会重复搬迁。
    again = client.post(f'/migrations/{ident}/resume', headers=auth(token)).json()
    assert again['resume']['noop'] is True
    switch_again = client.post(f'/migrations/{ident}/switch', headers=auth(token)).json()
    assert switch_again['switch']['noop'] is True
    detail = client.get(f'/migrations/{ident}', headers=auth(token)).json()
    target = next(s for s in detail['shards'] if s['id'] == detail['target_shard_id'])
    assert target['versions'] == 5  # 4 个种子版本 + k10 墓碑，无重复
    history = [h['event'] for h in detail['history']]
    assert history.count('copy_complete') == 1


def test_advance_is_idempotent_after_switch(client, token):
    migration = make_migration(client, token, '切换后推进')
    ident = migration['id']
    advance_until_done(client, token, ident)
    client.post(f'/migrations/{ident}/switch', headers=auth(token))
    response = client.post(f'/migrations/{ident}/advance', headers=auth(token))
    assert response.status_code == 200
    assert response.json()['advance']['noop'] is True


def test_ownership_and_export(client, token):
    migration = make_migration(client, token, '归属查看')
    ident = migration['id']
    before = client.get(f'/migrations/{ident}/ownership', headers=auth(token)).json()
    assert before['status'] == 'pending'
    assert before['range']['owner_shard_kind'] == 'source'
    assert before['target_staged_versions'] == 0

    write(client, token, ident, 'k08', 'delete')
    advance_until_done(client, token, ident)
    client.post(f'/migrations/{ident}/switch', headers=auth(token))
    after = client.get(f'/migrations/{ident}/ownership', headers=auth(token)).json()
    assert after['range']['owner_shard_kind'] == 'target'
    assert after['status_label'] == '已切换'
    assert any(shard['ownership'] == '已切换归属' for shard in after['shards'])

    export = client.get(f'/migrations/{ident}/export', headers=auth(token))
    assert export.status_code == 200
    body = export.json()
    assert body['migration']['status'] == 'switched'
    assert body['snapshot']['verified'] is True
    assert body['ownership']['status'] == 'switched'


def test_viewer_can_read_but_not_write_or_switch(client, token):
    migration = make_migration(client, token, '只读校验')
    ident = migration['id']
    viewer = login(client, 'viewer@beiling.test')
    assert client.post(f'/migrations/{ident}/writes', headers=auth(viewer),
                       json={'key': 'k08', 'op': 'put', 'value': 'x'}).status_code == 403
    assert client.post(f'/migrations/{ident}/switch', headers=auth(viewer)).status_code == 403
    assert client.post(f'/migrations/{ident}/advance', headers=auth(viewer)).status_code == 403
    response = client.get(f'/migrations/{ident}/query', headers=auth(viewer),
                          params={'start': 'k07', 'end': 'k08'})
    assert response.status_code == 200
    assert response.json()['ordered_keys'] == ['k07', 'k08']


def test_invalid_range_and_keys_rejected(client, token):
    base = {'dataset_id': 1, 'shard_count': 3}
    response = client.post('/migrations', headers=auth(token), json={
        **base, 'name': '非法区间', 'range_start': 'k09', 'range_end': 'k07'})
    assert response.status_code == 422
    response = client.post('/migrations', headers=auth(token), json={
        **base, 'name': '越过分片', 'range_start': 'k03', 'range_end': 'k09'})
    assert response.status_code == 422
    response = client.post('/migrations', headers=auth(token), json={
        **base, 'name': '区间无键', 'range_start': 'a1', 'range_end': 'a2'})
    assert response.status_code == 422
    response = client.post('/migrations', headers=auth(token), json={
        **base, 'name': '分片过多', 'shard_count': 6, 'range_start': 'k07', 'range_end': 'k10'})
    assert response.status_code == 422

    migration = make_migration(client, token, '非法写入检查')
    bad = client.post(f"/migrations/{migration['id']}/writes", headers=auth(token),
                      json={'key': 'k08', 'op': 'put'})
    assert bad.status_code == 422
    bad_key = client.post(f"/migrations/{migration['id']}/writes", headers=auth(token),
                          json={'key': 'x' * 65, 'op': 'put', 'value': 'v'})
    assert bad_key.status_code == 422
    bad_query = client.get(f"/migrations/{migration['id']}/query", headers=auth(token),
                           params={'start': 'k09', 'end': 'k07'})
    assert bad_query.status_code == 422
    assert client.post('/migrations', headers=auth(token), json={
        **base, 'name': '建立分片', 'range_start': 'k07', 'range_end': 'k10'}).status_code == 409
