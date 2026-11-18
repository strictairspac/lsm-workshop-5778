"""路由区间与切换切分的纯函数测试：覆盖、不重叠、64 字符键无后继构造。"""
import pytest

from src.models.migration import KEY_TYPE
from src.services.migration import (_build_routes, _carve, _contains, _router,
                                    route_for_key)


def _owners(routes, key):
    return [route for route in _router(routes) if _contains(route, key)]


def _assign(routes):
    for index, route in enumerate(_router(routes)):
        route['shard_id'] = index + 1
    return routes


@pytest.mark.parametrize('count', [2, 3, 4])
def test_routes_partition_keyspace_exactly_once(count):
    keys = [f'k{i:02d}' for i in range(10)]
    routes = _assign(_build_routes(keys, count))
    probes = ['', 'k00', 'k00\x00', 'k05', 'k09', 'k10', 'zzz', '￿']
    for key in keys + probes:
        assert len(_owners(routes, key)) == 1, (count, key)
    for key in keys:
        assert _contains(route_for_key(routes, key), key)


def test_carve_keeps_outside_keys_on_source_and_inside_on_target():
    keys = [f'k{i:02d}' for i in range(10)]
    routes = _assign(_build_routes(keys, 3))
    carved = _carve([dict(route) for route in routes], 'k07', 'k09', 99)
    target = next(route for route in _router(carved) if route['shard_id'] == 99)
    for key in ['k07', 'k08', 'k09']:
        assert _contains(target, key)
    for key in ['k00', 'k06', 'k10', 'zzz']:
        assert not _contains(target, key)
    probes = ['', 'k00', 'k06', 'k07', 'k08', 'k09', 'k10', 'zzz']
    for key in probes:
        assert len(_owners(carved, key)) == 1, key
    # 原路由仍是完整的，二分切分不修改输入。
    assert len(_owners(routes, 'k07')) == 1


def test_carve_supports_64_char_keys_without_successor():
    long_a = 'a' * 64
    long_b = 'b' * 64
    keys = [long_a, 'm' * 64, long_b]
    routes = _assign(_build_routes(keys, 2))
    carved = _carve([dict(route) for route in routes], long_b, long_b, 99)
    target = next(route for route in _router(carved) if route['shard_id'] == 99)
    assert _contains(target, long_b)
    assert not _contains(target, long_a)
    for key in ['', 'a' * 63, long_a, 'm' * 64, long_b, 'z' * 64]:
        assert len(_owners(carved, key)) == 1, key


def test_single_key_range_and_full_shard_range():
    keys = ['a', 'b', 'c', 'd', 'e', 'f']
    routes = _assign(_build_routes(keys, 2))
    for key in keys:
        carved = _carve([dict(route) for route in routes], key, key, 99)
        target = next(route for route in _router(carved) if route['shard_id'] == 99)
        assert _contains(target, key)
        for other in keys:
            if other != key:
                assert not _contains(target, other)

    first = _router(routes)[0]
    carved = _carve([dict(route) for route in routes], 'a', 'c', 99)
    target = next(route for route in _router(carved) if route['shard_id'] == 99)
    assert [_contains(target, key) for key in ['a', 'b', 'c']] == [True, True, True]
    assert not _contains(target, 'd')
    # 原路由对象不被原地修改，其余区间仍属来源分片。
    assert _contains(first, 'a')
    remaining = next(route for route in _router(carved) if route['shard_id'] == first['shard_id'])
    assert _contains(remaining, '') and not _contains(remaining, 'a')


def test_mysql_key_columns_are_bytewise():
    from sqlalchemy.dialects import mysql

    variant = KEY_TYPE.dialect_impl(mysql.dialect())
    assert variant.collation == 'utf8mb4_bin'
