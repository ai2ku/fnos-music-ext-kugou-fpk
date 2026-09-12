"""search_track 全量模式验证。

打桩 fetch_kugou_search（不发起真实网络请求），走完整的 _fetch_online_pages
与 merge_online_tracks 逻辑，验证：
  1. 手机端不分页 -> 循环取到酷狗 total（含跨页重复），不再第 3 页误判末页
  2. PC 分页请求 -> 参数精确透传，只取一页
  3. total 语义 -> 不分页时 total = 实际返回条数，不再用酷狗声明总数
  4. 空结果 / fetcher 异常 / 返回 None -> 不崩
"""
import asyncio
import importlib.util
import os
import sys
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient

HERE = Path(__file__).resolve().parent
APP_PY = HERE.parent / "fnmusic-ext-kugou.fpk" / "app" / "proxy" / "app.py"

os.environ.setdefault("FNMUSIC_MUSIC_DB", "/tmp/nonexistent_music.db")
os.environ.setdefault("FNMUSIC_UPSTREAM_SOCK", "/tmp/nonexistent.socket")
os.environ.setdefault("FNMUSIC_CACHE_DIR", "/tmp/fnmusic_test_cache")

sys.path.insert(0, str(APP_PY.parent))  # app.py 内 `import kugou_source` 需同目录可导入

spec = importlib.util.spec_from_file_location("fnmusic_app", APP_PY)
app_mod = importlib.util.module_from_spec(spec)
sys.modules["fnmusic_app"] = app_mod
spec.loader.exec_module(app_mod)

FASTAPI_APP: FastAPI = app_mod.app


# ---------------------------------------------------------------- helpers
def _official(keyword: str, n: int) -> dict:
    return {
        "code": 0,
        "data": {
            "total": n,
            "list": [
                {
                    "guid": f"local:{keyword}:{i}",
                    "title": f"本地歌{i} {keyword}",
                    "artist": "本地歌手",
                    "coverId": f"local:{keyword}:{i}",
                }
                for i in range(n)
            ],
        },
    }


def _kugou_page(keyword: str, page: int, size: int, total: int, overlap_from: int = 0) -> dict:
    """造一页酷狗结果；overlap_from>0 时本页含前面页的重复条目。"""
    start = (page - 1) * size
    items = []
    for i in range(start, start + size):
        if i >= total:
            break
        # 每 3 页重复一次前面 2 条，模拟酷狗跨页重复
        if overlap_from and page > 1 and i % 3 == 0 and page * 2 < total:
            items.append({
                "title": f"酷狗歌{overlap_from} {keyword}",
                "artist": "酷狗歌手",
                "id": f"kugou:{overlap_from}",
            })
        items.append({
            "title": f"酷狗歌{i} {keyword}",
            "artist": "酷狗歌手",
            "id": f"kugou:{i}",
        })
    return {"items": items, "total": total, "page": page, "pagesize": size}


class FakeUpstream(httpx.MockTransport):
    def __init__(self, payload):
        self.payload = payload
        super().__init__(handler=self.handler)

    def handler(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=self.payload)


def build_client(official: dict, kugou_responder, *, upstream_client):
    app_mod._SEARCH_CACHE.clear()
    app_mod.get_upstream_client = lambda _app: upstream_client
    app_mod.fetch_kugou_search = kugou_responder
    app_mod.CONF.update({
        "kugou_enabled": True,
        "search_timeout": 8.0,
        "search_cache_ttl": 300,
    })
    return TestClient(FASTAPI_APP)


# ---------------------------------------------------------------- cases
def case_full_mode_fetches_all_pages():
    """手机端不分页：循环取到酷狗 total，跨页重复不导致提前收工。"""
    kw, total, official_n = "本兮", 480, 22
    calls = []

    async def kugou_responder(keyword, limit, page=1):
        calls.append((page, limit))
        await asyncio.sleep(0.001)
        return _kugou_page(keyword, page, limit, total, overlap_from=1)

    up = httpx.AsyncClient(base_url="http://localhost", transport=FakeUpstream(_official(kw, official_n)))
    with build_client(_official(kw, official_n), kugou_responder, upstream_client=up) as c:
        r = c.get(f"/music/api/v1/search/track?q={kw}&lan=zh-CN")
    assert r.status_code == 200, r.text[:400]
    body = r.json()
    got = body["data"]["list"]
    total_field = body["data"]["total"]

    assert len(calls) >= 10, f"只翻了 {len(calls)} 页，未取全量: {calls}"
    assert all(sz == 50 for _, sz in calls), f"页大小不是 50: {calls}"
    assert len(got) > 147, f"合并后仅 {len(got)} 条（旧 bug 停在 147）"
    # total 必须等于实际返回条数，不能是酷狗声明的 480 + 官方 22
    assert total_field == len(got), f"total={total_field} 与实际条数 {len(got)} 不符"
    print(f"OK full: pages={len(calls)} items={len(got)} total={total_field}")


def case_paged_mode_total_honest():
    """PC 分页请求：路由从不按 page 切片，total 必须恒等于实际条数。

    客户端传 page/size 但路由既不转给酷狗也不转给上游，拿到的仍是合并全量列表。
    因此 total 不能是声明总数，否则前端会算出空页。
    """
    kw = "本兮"
    calls = []

    async def kugou_responder(keyword, limit, page=1):
        calls.append((page, limit))
        return _kugou_page(keyword, page, limit, 300)

    up = httpx.AsyncClient(base_url="http://localhost", transport=FakeUpstream(_official(kw, 5)))
    with build_client(_official(kw, 5), kugou_responder, upstream_client=up) as c:
        r = c.get(f"/music/api/v1/search/track?q={kw}&page=2&size=24")
    assert r.status_code == 200, r.text[:400]
    body = r.json()
    # 路由不切片：酷狗侧全量轮询，页号从 1 递增
    assert calls[0] == (1, 50), f"首轮应从第 1 页开始: {calls}"
    assert body["data"]["total"] == len(body["data"]["list"]), \
        f"total={body['data']['total']} 与实际条数 {len(body['data']['list'])} 不符"
    print(f"OK paged_honest: pages={len(calls)} items={len(body['data']['list'])} total={body['data']['total']}")


def case_empty_result():
    """酷狗全空：不崩，返回纯官方结果。"""
    kw = "空词"

    async def kugou_responder(keyword, limit, page=1):
        return {"items": [], "total": 0, "page": page, "pagesize": limit}

    up = httpx.AsyncClient(base_url="http://localhost", transport=FakeUpstream(_official(kw, 3)))
    with build_client(_official(kw, 3), kugou_responder, upstream_client=up) as c:
        r = c.get(f"/music/api/v1/search/track?q={kw}")
    assert r.status_code == 200, r.text[:400]
    assert r.json()["data"]["total"] == 3
    print("OK empty: total=3")


def case_fetcher_none():
    """fetcher 返回 None：不崩，返回纯官方结果。"""
    kw = "故障词"

    async def kugou_responder(keyword, limit, page=1):
        return None

    up = httpx.AsyncClient(base_url="http://localhost", transport=FakeUpstream(_official(kw, 2)))
    with build_client(_official(kw, 2), kugou_responder, upstream_client=up) as c:
        r = c.get(f"/music/api/v1/search/track?q={kw}")
    assert r.status_code == 200, r.text[:400]
    print("OK fetcher_none: total=", r.json()["data"]["total"])


def case_fetcher_raises_midway():
    """中途抛异常：保留已拿到的部分结果，不崩。"""
    kw = "半截词"
    calls = []

    async def kugou_responder(keyword, limit, page=1):
        calls.append(page)
        if page >= 3:
            raise RuntimeError("boom on page 3")
        return _kugou_page(keyword, page, limit, 99999)

    up = httpx.AsyncClient(base_url="http://localhost", transport=FakeUpstream(_official(kw, 1)))
    with build_client(_official(kw, 1), kugou_responder, upstream_client=up) as c:
        r = c.get(f"/music/api/v1/search/track?q={kw}")
    assert r.status_code == 200, r.text[:400]
    body = r.json()
    assert calls[:3] == [1, 2, 3], f"翻页序列异常: {calls}"
    assert len(body["data"]["list"]) > 1, "失败前拿到的结果丢失"
    assert body["data"]["total"] == len(body["data"]["list"])
    print(f"OK midway_raise: calls={calls} items={len(body['data']['list'])}")


def case_cross_page_duplicates_deduped():
    """跨页重复条目必须去重，不能靠'不足页大小'提前停。"""
    kw = "重复词"
    # total 很小但强制 100% 重复率，考验 added==0 判据
    async def kugou_responder(keyword, limit, page=1):
        if page == 1:
            return {"items": [
                {"title": "唯一歌A", "artist": "歌手A", "id": "kugou:1"},
                {"title": "唯一歌B", "artist": "歌手B", "id": "kugou:2"},
            ], "total": 2, "page": 1, "pagesize": limit}
        return {"items": [
            {"title": "唯一歌A", "artist": "歌手A", "id": "kugou:1"},
            {"title": "唯一歌B", "artist": "歌手B", "id": "kugou:2"},
        ], "total": 2, "page": page, "pagesize": limit}

    up = httpx.AsyncClient(base_url="http://localhost", transport=FakeUpstream(_official(kw, 1)))
    with build_client(_official(kw, 1), kugou_responder, upstream_client=up) as c:
        r = c.get(f"/music/api/v1/search/track?q={kw}")
    assert r.status_code == 200, r.text[:400]
    titles = [x["title"] for x in r.json()["data"]["list"]]
    assert titles.count("唯一歌A") == 1 and titles.count("唯一歌B") == 1, f"未去重: {titles}"
    print(f"OK dedup: titles={titles}")


CASES = [
    case_full_mode_fetches_all_pages,
    case_paged_mode_total_honest,
    case_empty_result,
    case_fetcher_none,
    case_fetcher_raises_midway,
    case_cross_page_duplicates_deduped,
]

if __name__ == "__main__":
    failed = 0
    for case in CASES:
        try:
            case()
        except Exception as exc:
            failed += 1
            print(f"FAIL {case.__name__}: {exc!r}")
    print(f"\n{len(CASES) - failed}/{len(CASES)} passed")
    sys.exit(1 if failed else 0)
