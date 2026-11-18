import os
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DB_PATH = pathlib.Path('/tmp/lsm_migration_test.db')
if DB_PATH.exists():
    DB_PATH.unlink()
os.environ['DATABASE_URL'] = f'sqlite:///{DB_PATH}'
os.environ['JWT_SECRET'] = 'test-secret'
os.environ['REDIS_URL'] = 'redis://127.0.0.1:6399/15'
os.environ['SEED_DEMO'] = 'false'

from fastapi.testclient import TestClient  # noqa: E402

from src.config.database import Base, Session, engine  # noqa: E402
from src.main import app  # noqa: E402
from src.models.dataset import Dataset  # noqa: E402
from src.models.user import User  # noqa: E402
from src.utils.password import hash_password  # noqa: E402

ENTRIES = [
    {'op': 'put', 'key': 'k01', 'value': 'v1'},
    {'op': 'put', 'key': 'k02', 'value': 'v2'},
    {'op': 'put', 'key': 'k03', 'value': 'v3'},
    {'op': 'put', 'key': 'k04', 'value': 'v4'},
    {'op': 'put', 'key': 'k05', 'value': 'v5'},
    {'op': 'put', 'key': 'k06', 'value': 'v6'},
    {'op': 'put', 'key': 'k07', 'value': 'v7'},
    {'op': 'put', 'key': 'k08', 'value': 'v8'},
    {'op': 'put', 'key': 'k09', 'value': 'v9'},
    {'op': 'put', 'key': 'k10', 'value': 'v10'},
]


@pytest.fixture(scope='session', autouse=True)
def database():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin', 'operator', 'viewer'):
            db.add(User(email=f'{role}@beiling.test', role=role, password_hash=hash_password('EdgeStore!17')))
        db.add(Dataset(name='迁移演练集', description='十个键，覆盖搬迁区间', entries=ENTRIES))
        db.commit()
    yield


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def token(client):
    return login(client, 'operator@beiling.test')


def login(client, email):
    response = client.post('/auth/login', json={'email': email, 'password': 'EdgeStore!17'})
    assert response.status_code == 200, response.text
    return response.json()['token']


def auth(token):
    return {'Authorization': f'Bearer {token}'}


def dataset_id():
    with Session() as db:
        return db.query(Dataset).filter(Dataset.name == '迁移演练集').one().id


def make_migration(client, token, name, start='k07', end='k10', shard_count=3):
    response = client.post('/migrations', headers=auth(token), json={
        'name': name, 'dataset_id': dataset_id(), 'shard_count': shard_count,
        'range_start': start, 'range_end': end,
    })
    assert response.status_code == 201, response.text
    return response.json()


def advance_until_done(client, token, ident, limit=30):
    payload = None
    for _ in range(limit):
        payload = client.post(f'/migrations/{ident}/advance', headers=auth(token))
        assert payload.status_code == 200, payload.text
        payload = payload.json()
        if payload['advance']['done']:
            return payload
    raise AssertionError('复制未在限定批次内完成')


def write(client, token, ident, key, op='put', value=None):
    body = {'key': key, 'op': op}
    if value is not None:
        body['value'] = value
    response = client.post(f'/migrations/{ident}/writes', headers=auth(token), json=body)
    assert response.status_code == 201, response.text
    return response.json()


def query(client, token, ident, start=None, end=None):
    params = {}
    if start is not None:
        params['start'] = start
    if end is not None:
        params['end'] = end
    response = client.get(f'/migrations/{ident}/query', headers=auth(token), params=params)
    assert response.status_code == 200, response.text
    return response.json()
