"""原有批次 / 配置 / 试验流程在新增迁移后仍可用。"""
from tests.conftest import auth, dataset_id, login

from src.services.lsm_engine import simulate


class _Queue:
    def __init__(self):
        self.jobs = []

    def rpush(self, name, value):
        self.jobs.append((name, value))

    def blpop(self, *args, **kwargs):
        return None


def test_existing_trial_flow_still_works(client, token, monkeypatch):
    from src.services import trial as trial_service

    queue = _Queue()
    monkeypatch.setattr(trial_service, 'queue', queue)

    config = client.post('/configs', headers=auth(token), json={
        'name': '迁移回归配置', 'memtable_limit': 2, 'l0_trigger': 2,
        'max_levels': 3, 'final_compaction': True})
    assert config.status_code == 201, config.text
    trial = client.post('/trials', headers=auth(token), json={
        'name': '迁移回归试验', 'dataset_id': dataset_id(), 'config_id': config.json()['id']})
    assert trial.status_code == 202, trial.text
    assert trial.json()['status'] == 'queued'
    assert queue.jobs and queue.jobs[0][0] == 'lsm:jobs'

    # 页面侧读取队列与批次仍返回原有形状。
    listed = client.get('/trials', headers=auth(token)).json()
    assert any(item['name'] == '迁移回归试验' for item in listed)
    datasets = client.get('/datasets', headers=auth(token)).json()
    assert any(item['name'] == '迁移演练集' for item in datasets)

    # worker 的演算入口没有被迁移模型改动。
    snapshot = trial.json()['input_snapshot']
    result = simulate(snapshot['entries'], snapshot['config'])
    assert result['verified'] is True and result['live_keys'] == 10


def test_migration_listing_does_not_disturb_other_entities(client, token):
    response = client.get('/migrations', headers=auth(token))
    assert response.status_code == 200
    for item in response.json():
        assert set(item) >= {'id', 'name', 'status', 'status_label', 'watermark'}

    viewer = login(client, 'viewer@beiling.test')
    assert client.get('/migrations', headers=auth(viewer)).status_code == 200
    assert client.get('/datasets', headers=auth(viewer)).status_code == 200
