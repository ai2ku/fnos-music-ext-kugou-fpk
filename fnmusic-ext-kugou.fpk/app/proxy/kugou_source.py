"""KuGouMusicApi 源适配模块。

把 KuGouMusicApi 的核心接口封装为 fnos_music_ext 期望的契约：
- search(keyword, limit) → list[dict]
- resolve_url(song_id) → str | None
- get_info(guid) → dict
- fetch_lyric(song_id) → str
- qr_login() → QR 登录相关

所有配置项通过 CONF 字典注入，由 app.py 在启动时填充。
"""
from __future__ import annotations

import asyncio
import logging
import json
from typing import Any

import httpx

logger = logging.getLogger("kugou_source")

# 默认值，app.py 启动时通过 set_config() 覆盖
CFG: dict[str, Any] = {
    "kugou_url": "http://127.0.0.1:8899",
    "kugou_quality": "320",
    "kugou_search_timeout": 15.0,
    "kugou_enabled": True,
    # QR 登录凭证
    "kugou_token": "",
    "kugou_userid": "",
    "kugou_dfid": "",
    "kugou_t1": "",
    "kugou_mid": "",
    "kugou_guid": "",
    "kugou_dev": "",
    "kugou_mac": "",
}


def set_config(cfg: dict[str, Any]) -> None:
    """供 app.py 调用，注入运行时配置。"""
    CFG.update(cfg)


def _auth_header() -> str | None:
    """构造 Authorization 头。"""
    parts = []
    if CFG.get("kugou_token"):
        parts.append(f"token={CFG['kugou_token']}")
    if CFG.get("kugou_userid"):
        parts.append(f"userid={CFG['kugou_userid']}")
    if CFG.get("kugou_dfid"):
        parts.append(f"dfid={CFG['kugou_dfid']}")
    if CFG.get("kugou_t1"):
        parts.append(f"t1={CFG['kugou_t1']}")
    if CFG.get("kugou_mid"):
        parts.append(f"KUGOU_API_MID={CFG['kugou_mid']}")
    if CFG.get("kugou_guid"):
        parts.append(f"KUGOU_API_GUID={CFG['kugou_guid']}")
    if CFG.get("kugou_dev"):
        parts.append(f"KUGOU_API_DEV={CFG['kugou_dev']}")
    if CFG.get("kugou_mac"):
        parts.append(f"KUGOU_API_MAC={CFG['kugou_mac']}")
    if not parts:
        return None
    return ";".join(parts)


def _client() -> httpx.AsyncClient:
    """每次调用新建客户端，避免连接池状态问题。"""
    headers = {}
    auth = _auth_header()
    if auth:
        headers["Authorization"] = auth
    return httpx.AsyncClient(
        base_url=CFG["kugou_url"],
        timeout=float(CFG["kugou_search_timeout"]),
        follow_redirects=True,
        headers=headers,
    )


# ============================================================
# 内存索引：搜索时缓存，get_info 命中时直接返回元数据
# ============================================================

_search_index: dict[str, dict] = {}


def _remember_song(item: dict) -> None:
    sid = str(item.get("id") or "").split(":", 1)[-1]
    if sid:
        _search_index[sid] = dict(item)


def clear_index() -> None:
    _search_index.clear()


# ============================================================
# 搜索
# ============================================================

async def search(keyword: str, limit: int = 30, page: int = 1) -> dict:
    """调 KuGouMusicApi /search，返回统一结果对象，包含 items 和 total。"""
    if not keyword:
        return {"items": [], "total": 0, "page": page, "pagesize": limit}
    auth = _auth_header()
    logger.warning("[KUGOU] keyword=%r limit=%d auth_len=%d",
                   keyword, limit, len(auth or ""))
    try:
        async with _client() as c:
            lists = []
            raw = {}
            last_body = ""
            for attempt in range(5):  # 5 次重试，应对 data:null
                r = await c.get(
                    "/search",
                    params={"keywords": keyword, "page": page, "pagesize": limit, "type": "song"},
                )
                last_body = r.text[:200]
                if r.status_code != 200:
                    logger.warning("[KUGOU] http %s attempt=%d", r.status_code, attempt + 1)
                    await asyncio.sleep(0.6 * (attempt + 1))
                    continue
                body = r.json()
                if body.get("status") != 1:
                    logger.warning("[KUGOU] status=%s attempt=%d", body.get("status"), attempt + 1)
                    await asyncio.sleep(0.6 * (attempt + 1))
                    continue
                d = body.get("data") or {}
                lists = d.get("lists") or []
                raw = d
                if lists:
                    break
                logger.warning("[KUGOU] empty lists attempt=%d body=%r", attempt + 1, last_body[:120])
                await asyncio.sleep(0.6 * (attempt + 1))
            logger.warning("[KUGOU] final lists=%d", len(lists))
    except Exception as e:
        logger.warning("[KUGOU] error: %s", e)
        return {"items": [], "total": 0, "page": page, "pagesize": limit}

    items: list[dict] = []
    for it in lists:
        if not isinstance(it, dict):
            continue
        # 兼容多种字段名(旧小写 + 官方 FileName/SingerName 等驼峰)
        sid = str(
            it.get("hash") or it.get("songhash") or it.get("songHash")
            or it.get("FileHash") or it.get("fileHash") or ""
        ).strip()
        title = str(
            it.get("songname") or it.get("songName") or it.get("FileName")
            or it.get("fileName") or it.get("OriSongName") or it.get("OriSongName")
            or ""
        ).strip()
        artist = str(
            it.get("singername") or it.get("singerName") or it.get("SingerName")
            or it.get("singer") or it.get("singName") or ""
        ).strip()
        album = str(
            it.get("albumname") or it.get("albumName") or it.get("AlbumName")
            or it.get("album") or it.get("albumName") or ""
        ).strip()
        duration_raw = (
            it.get("duration") or it.get("playTime") or it.get("Duration")
            or it.get("PlayTime") or 0
        )
        try:
            duration_s = float(duration_raw)
        except (TypeError, ValueError):
            duration_s = 0.0
        ext = "mp3"
        # songType 字段在 KugouMusicApi 中通常是 ExtName 或 songType
        if it.get("ExtName") in ("flac", "ape", "wv", "ogg", "m4a"):
            ext = str(it.get("ExtName")).lower()
        elif it.get("songType") in ("flac", "ape", "wv", "ogg", "m4a"):
            ext = str(it.get("songType")).lower()
        cover = str(
            it.get("Image") or it.get("Img") or it.get("img")
            or it.get("AlbumImg") or it.get("albumImg")
            or it.get("albumimg") or it.get("album_img")
            or it.get("AlbumImage") or it.get("albumImage")
            or it.get("PicUrl") or it.get("picUrl")
            or it.get("coverImg") or it.get("coverImgUrl")
            or it.get("trans_param", {}).get("union_cover")
            or ""
        )
        # Kugou 封面 URL 带 {size} 占位符，保留占位符供 static_cover 按请求 size 替换
        cover = str(cover or "")
        # FileSize 字节数
        file_size = 0
        try:
            file_size = int(it.get("FileSize") or it.get("fileSize") or 0)
        except (TypeError, ValueError):
            file_size = 0
        #  bitrate 用于 audioSpec 的 bitrate 字段
        bitrate = 0
        try:
            bitrate = int(it.get("Bitrate") or it.get("bitrate") or 0)
        except (TypeError, ValueError):
            bitrate = 0
        if not sid:
            continue
        item = {
            "id": f"kugou:{sid}",
            "source": "kugou",
            "title": title,
            "artist": artist,
            "album": album,
            "duration_s": duration_s,
            "ext": ext,
            "cover_url": cover,
            "union_cover": cover,
            "file_size": file_size,
            "bitrate": bitrate,
            "lyric": "",
        }
        items.append(item)
        _remember_song(item)

    try:
        kugou_total = int(raw.get("total") or 0)
    except (TypeError, ValueError):
        kugou_total = 0

    return {
        "items": items,
        "total": max(kugou_total, len(items)),
        "page": page,
        "pagesize": limit,
    }


# ============================================================
# 音频流 URL
# ============================================================

QUALITY_FALLBACK = ["high", "320", "128", "64"]


async def resolve_url(song_id: str) -> tuple[str | None, str | None]:
    """按音质档位尝试解析播放直链。

    返回 (url, ext)：解析失败返回 (None, None)。
    song_id 为 "hash" 部分，不含 "kugou:" 前缀。
    """
    if not song_id:
        return None, None
    pref = str(CFG.get("kugou_quality") or "high")
    order = [pref] + [q for q in QUALITY_FALLBACK if q != pref]
    logger.warning("[KUGOU] resolve_url hash=%s order=%s", song_id, order)

    async with _client() as c:
        for q in order:
            try:
                r = await c.get(
                    "/song/url",
                    params={"hash": song_id, "quality": q},
                )
                body_preview = r.text[:500]
                if r.status_code != 200:
                    logger.warning("[KUGOU] resolve_url hash=%s q=%s http=%s body=%r",
                                   song_id, q, r.status_code, body_preview)
                    continue
                data = r.json()
                # 兼容 API 返回字符串的情况
                if isinstance(data, str):
                    logger.warning("[KUGOU] resolve_url hash=%s q=%s response is str: %r",
                                   song_id, q, data[:200])
                    continue
                if not isinstance(data, dict):
                    logger.warning("[KUGOU] resolve_url hash=%s q=%s response type=%s body=%r",
                                   song_id, q, type(data).__name__, body_preview)
                    continue
                status = data.get("status")
                if status != 1:
                    logger.warning("[KUGOU] resolve_url hash=%s q=%s status=%s body=%r",
                                   song_id, q, status, body_preview)
                    continue
                # 兼容多种返回结构：优先顶层 url，其次 backupUrl，最后嵌套 data.url / data.backupUrl
                url = None
                ext = "mp3"
                if isinstance(data.get("url"), list) and data["url"]:
                    url = data["url"][0]
                    ext = str(data.get("extName") or "mp3").lower()
                elif isinstance(data.get("url"), str):
                    url = data["url"]
                    ext = str(data.get("extName") or "mp3").lower()
                elif isinstance(data.get("backupUrl"), list) and data["backupUrl"]:
                    url = data["backupUrl"][0]
                    ext = str(data.get("extName") or "mp3").lower()
                elif isinstance(data.get("backupUrl"), str):
                    url = data["backupUrl"]
                    ext = str(data.get("extName") or "mp3").lower()
                elif isinstance(data.get("data"), dict):
                    d2 = data["data"]
                    if isinstance(d2.get("url"), list) and d2["url"]:
                        url = d2["url"][0]
                        ext = str(d2.get("extName") or "mp3").lower()
                    elif isinstance(d2.get("url"), str):
                        url = d2["url"]
                        ext = str(d2.get("extName") or "mp3").lower()
                    elif isinstance(d2.get("backupUrl"), list) and d2["backupUrl"]:
                        url = d2["backupUrl"][0]
                        ext = str(d2.get("extName") or "mp3").lower()
                    elif isinstance(d2.get("backupUrl"), str):
                        url = d2["backupUrl"]
                        ext = str(d2.get("extName") or "mp3").lower()
                if not url:
                    logger.warning("[KUGOU] resolve_url hash=%s q=%s no url body=%r",
                                   song_id, q, body_preview)
                    continue
                logger.warning("[KUGOU] resolve_url OK hash=%s q=%s ext=%s url=%s...",
                               song_id, q, ext, url[:100])
                return url, ext
            except Exception as e:
                logger.warning("[KUGOU] resolve_url hash=%s q=%s err=%s", song_id, q, e)
    logger.warning("[KUGOU] resolve_url ALL FAILED hash=%s", song_id)
    return None, None


# ============================================================
# 歌曲信息（用于 track/metadata）
# ============================================================

async def get_user_playlists(page: int = 1, pagesize: int = 500) -> dict:
    """拉取 KuGouMusicApi 当前登录用户的歌单列表。

    统一返回 {"items": [...], "total": n, "page": p, "pagesize": s}。
    字段名尽量同时兼容 data.list/data.list[]/data.items[]/lists[]。
    """
    try:
        async with _client() as c:
            r = await c.get(
                "/user/playlist",
                params={"page": page, "pagesize": pagesize},
            )
            if r.status_code != 200:
                logger.warning("[KUGOU] user_playlist http=%s body=%r", r.status_code, r.text[:200])
                return {"items": [], "total": 0, "page": page, "pagesize": pagesize}
            data = r.json()
            st = data.get("status", data.get("code"))
            if st not in (1, 200, 0):
                logger.warning("[KUGOU] user_playlist status=%s body=%r", st, data)
                return {"items": [], "total": 0, "page": page, "pagesize": pagesize}

            raw = data.get("data") or {}
            raw_list = None
            if isinstance(raw, list):
                raw_list = raw
            elif isinstance(raw, dict):
                # KuGouMusicApi 返回格式: data.info / data.list / data.items / data.lists / data.records
                for key in ("info", "list", "items", "lists", "data", "records"):
                    v = raw.get(key)
                    if isinstance(v, list):
                        raw_list = v
                        break
            items = [x for x in (raw_list or []) if isinstance(x, dict)]
            total = int(raw.get("total") or raw.get("count") or len(items)) if isinstance(raw, dict) else len(items)
            logger.warning("[KUGOU] user_playlist page=%d pagesize=%d items=%d total=%d", page, pagesize, len(items), total)
            return {"items": items, "total": total, "page": page, "pagesize": pagesize}
    except Exception as e:
        logger.warning("[KUGOU] user_playlist error: %s", e)
        return {"items": [], "total": 0, "page": page, "pagesize": pagesize}


async def get_info(song_id: str) -> dict | None:
    """从内存索引拿元数据。搜过的歌直接命中；未搜过则用 hash 搜一次回源。"""
    if not song_id:
        return None
    cached = _search_index.get(song_id)
    if cached:
        return dict(cached)
    # 未搜过（直接进入详情页），用 hash 反向搜一次拿封面/标题等
    try:
        async with _client() as c:
            r = await c.get(
                "/search",
                params={"hash": song_id, "page": 1, "pagesize": 1, "type": "song"},
            )
            if r.status_code == 200:
                data = r.json()
                lists = ((data.get("data") or {}).get("lists") or [])
                if lists and isinstance(lists[0], dict):
                    # 复用 search 的字段解析逻辑，直接走 search 函数
                    return await search_by_hash(song_id)
    except Exception as e:
        logger.warning("get_info search-by-hash %s err=%s", song_id, e)
    return {
        "id": f"kugou:{song_id}",
        "source": "kugou",
        "title": "",
        "artist": "",
        "album": "",
        "duration_s": 0,
        "ext": "mp3",
        "cover_url": "",
        "file_size": 0,
        "bitrate": 0,
        "lyric": "",
    }


async def search_by_hash(song_id: str) -> dict | None:
    """用 hash 搜一首歌并返回统一格式（用于详情页首次进入）。"""
    if not song_id:
        return None
    try:
        async with _client() as c:
            r = await c.get(
                "/search",
                params={"hash": song_id, "page": 1, "pagesize": 1, "type": "song"},
            )
            if r.status_code != 200:
                return None
            data = r.json()
            lists = ((data.get("data") or {}).get("lists") or [])
            if not lists or not isinstance(lists[0], dict):
                return None
            it = lists[0]
            # 复用 search 内部的字段映射
            sid = str(
                it.get("hash") or it.get("songhash") or it.get("songHash")
                or it.get("FileHash") or it.get("fileHash") or song_id
            ).strip()
            title = str(
                it.get("songname") or it.get("songName") or it.get("FileName")
                or it.get("fileName") or it.get("OriSongName") or ""
            ).strip()
            artist = str(
                it.get("singername") or it.get("singerName") or it.get("SingerName")
                or it.get("singer") or ""
            ).strip()
            album = str(
                it.get("albumname") or it.get("albumName") or it.get("AlbumName")
                or it.get("album") or ""
            ).strip()
            duration_raw = (
                it.get("duration") or it.get("playTime") or it.get("Duration")
                or it.get("PlayTime") or 0
            )
            try:
                duration_s = float(duration_raw)
            except (TypeError, ValueError):
                duration_s = 0.0
            ext = "mp3"
            if it.get("ExtName") in ("flac", "ape", "wv", "ogg", "m4a"):
                ext = str(it.get("ExtName")).lower()
            cover = str(
                it.get("Image") or it.get("Img") or it.get("img")
                or it.get("AlbumImg") or it.get("albumImg")
                or it.get("albumimg") or it.get("album_img")
                or it.get("AlbumImage") or it.get("albumImage")
                or it.get("PicUrl") or it.get("picUrl")
                or ""
            )
            # 保留 {size} 占位符，供 static_cover 按请求 size 动态替换
            file_size = 0
            try:
                file_size = int(it.get("FileSize") or 0)
            except (TypeError, ValueError):
                file_size = 0
            bitrate = 0
            try:
                bitrate = int(it.get("Bitrate") or 0)
            except (TypeError, ValueError):
                bitrate = 0
            item = {
                "id": f"kugou:{sid}",
                "source": "kugou",
                "title": title,
                "artist": artist,
                "album": album,
                "duration_s": duration_s,
                "ext": ext,
                "cover_url": cover,
                "file_size": file_size,
                "bitrate": bitrate,
                "lyric": "",
            }
            _remember_song(item)
            return item
    except Exception as e:
        logger.warning("search_by_hash %s err=%s", song_id, e)
    return None


# ============================================================
# 歌词
# ============================================================

async def fetch_lyric(song_id: str) -> str:
    """从 KuGouMusicApi 拉在线歌曲歌词：先用 hash 找 id/accesskey，再取 LRC。"""
    if not song_id:
        return ""
    try:
        async with _client() as c:
            r1 = await c.get("/search/lyric", params={"hash": song_id})
            if r1.status_code != 200:
                logger.warning("kugou fetch_lyric search/lyric hash=%s http=%s body=%r", song_id, r1.status_code, r1.text[:300])
                return ""
            j1 = r1.json()
            cands = j1.get("candidates") or []
            if not cands:
                logger.warning("kugou fetch_lyric search/lyric empty hash=%s status=%s body=%r", song_id, j1.get("status"), j1)
                return ""
            first = cands[0]
            r2 = await c.get(
                "/lyric",
                params={
                    "id": first.get("id"),
                    "accesskey": first.get("accesskey"),
                    "fmt": "lrc",
                    "decode": "true",
                },
            )
            if r2.status_code != 200:
                logger.warning("kugou fetch_lyric lyric hash=%s http=%s body=%r", song_id, r2.status_code, r2.text[:300])
                return ""
            j2 = r2.json()
            text = str(j2.get("decodeContent") or j2.get("lyric") or "").strip()
            logger.warning("kugou fetch_lyric hash=%s lyrics_len=%d status=%s", song_id, len(text), j2.get("status"))
            return text
    except Exception as e:
        logger.warning("kugou fetch_lyric song_id=%s err=%s", song_id, e)
        return ""


# ============================================================
# QR 登录
# ============================================================

async def qr_key() -> str | None:
    """获取二维码 key。"""
    try:
        async with httpx.AsyncClient(
            base_url=CFG["kugou_url"],
            timeout=10.0,
        ) as c:
            r = await c.get("/login/qr/key")
            if r.status_code != 200:
                logger.warning("kugou qr_key http %s: %s", r.status_code, r.text[:300])
                return None
            data = r.json()
            ok = (data.get("code") == 200) or (data.get("status") == 1) or (data.get("status") == 0)
            if ok and isinstance(data.get("data"), dict):
                return data["data"].get("qrcode")
            logger.warning("kugou qr_key bad response: %s", json.dumps(data, ensure_ascii=False)[:400])
    except Exception as e:
        logger.warning("kugou qr_key error: %s", e)
    return None


async def qr_create(key: str) -> str | None:
    """生成二维码图片（base64）。"""
    if not key:
        return None
    try:
        async with httpx.AsyncClient(
            base_url=CFG["kugou_url"],
            timeout=10.0,
        ) as c:
            r = await c.get("/login/qr/create", params={"key": key, "qrimg": "true"})
            if r.status_code != 200:
                logger.warning("kugou qr_create http %s: %s", r.status_code, r.text[:300])
                return None
            data = r.json()
            ok = (data.get("code") == 200) or (data.get("status") == 1) or (data.get("status") == 0)
            if ok and isinstance(data.get("data"), dict):
                return data["data"].get("base64")
            logger.warning("kugou qr_create bad response: %s", json.dumps(data, ensure_ascii=False)[:400])
    except Exception as e:
        logger.warning("kugou qr_create error: %s", e)
    return None


async def qr_check(key: str) -> dict:
    """检查二维码扫描状态。
    
    返回:
    - status=0: 二维码过期
    - status=1: 等待扫码
    - status=2: 已扫码，等待确认
    - status=4: 登录成功，返回用户信息
    """
    import time
    try:
        async with httpx.AsyncClient(
            base_url=CFG["kugou_url"],
            timeout=10.0,
        ) as c:
            r = await c.get(
                "/login/qr/check",
                params={"key": key, "timestamp": int(time.time() * 1000)},
                headers={"Cache-Control": "no-cache"},
            )
            if r.status_code != 200:
                return {"status": -1, "message": f"http {r.status_code}"}
            data = r.json()
            # 兼容 code/status 两种返回
            st = data.get("status", data.get("code"))
            if st == 1 or st == 200:
                return data.get("data", {}) or {"status": int(st)}
            # 扫码确认状态也可能嵌在 data.status
            inner = data.get("data")
            if isinstance(inner, dict) and inner.get("status") is not None:
                inner["message"] = data.get("message", inner.get("message", ""))
                return inner
            return {"status": -1, "message": data.get("message", "unknown")}
    except Exception as e:
        logger.warning("kugou qr_check error: %s", e)
        return {"status": -1, "message": str(e)}


def save_credentials(user_data: dict) -> dict:
    """保存登录凭证到 CFG，返回凭证摘要。"""
    credentials = {
        "token": user_data.get("token", ""),
        "userid": str(user_data.get("userid", "")),
        "dfid": user_data.get("dfid", ""),
        "t1": user_data.get("t1", ""),
        "mid": user_data.get("mid", ""),
        "guid": user_data.get("guid", ""),
        "dev": user_data.get("serverDev", ""),
        "mac": user_data.get("mac", ""),
    }
    # 更新 CFG
    CFG["kugou_token"] = credentials["token"]
    CFG["kugou_userid"] = credentials["userid"]
    CFG["kugou_dfid"] = credentials["dfid"]
    CFG["kugou_t1"] = credentials["t1"]
    CFG["kugou_mid"] = credentials["mid"]
    CFG["kugou_guid"] = credentials["guid"]
    CFG["kugou_dev"] = credentials["dev"]
    CFG["kugou_mac"] = credentials["mac"]
    return credentials
