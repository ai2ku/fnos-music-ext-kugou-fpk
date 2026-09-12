"""fnmusic-ext 拦截代理 (FastAPI + httpx).

功能：
1. 通用透传：所有非拦截路径原样转发到 trim-music unix socket
2. 搜索合并：GET /music/api/v1/search/track* （兼容 q/keyword，并行 musicdl）
3. 在线播放：stream + HLS 兜底 + transcode 空操作 + tee 缓存回放（音频与歌词 sidecar）
4. 在线元数据/歌词/封面
5. GET /_ext/healthz
"""
from __future__ import annotations

import asyncio
import glob
import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Awaitable, Callable, Coroutine
from urllib.parse import quote, urlencode, parse_qs, unquote
from uuid import uuid4
from datetime import datetime
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse, HTMLResponse

logger = logging.getLogger("fnmusic_proxy")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

_HOME = os.environ.get(
    "FNMUSIC_HOME_DIR", "/var/apps/fnmusic_ext_kugou/target/app/home"
)
_APP_MODE = os.environ.get("FNMUSIC_APP_MODE", "").strip().lower()

CONF = {
    "musicdl_url": os.environ.get("FNMUSIC_MUSICDL_URL", "http://127.0.0.1:8768"),
    "musicbox_url": os.environ.get("FNMUSIC_MUSICBOX_URL", "http://127.0.0.1:8770"),
    "musicdl_enabled": os.environ.get("FNMUSIC_MUSICDL_ENABLED", "false").lower() in ("true", "1", "yes"),
    "kugou_enabled": os.environ.get("FNMUSIC_KUGOU_ENABLED", "true").lower() in ("true", "1", "yes"),
    "kugou_url": os.environ.get("FNMUSIC_KUGOU_URL", "http://127.0.0.1:8899"),
    "kugou_quality": str(os.environ.get("FNMUSIC_KUGOU_QUALITY", "high")),
    "kugou_search_timeout": float(os.environ.get("FNMUSIC_KUGOU_SEARCH_TIMEOUT", "15")),
    "kugou_token": os.environ.get("FNMUSIC_KUGOU_TOKEN", ""),
    "kugou_userid": os.environ.get("FNMUSIC_KUGOU_USERID", ""),
    "kugou_dfid": os.environ.get("FNMUSIC_KUGOU_DFID", ""),
    "kugou_t1": os.environ.get("FNMUSIC_KUGOU_/track/", ""),
    "kugou_mid": os.environ.get("FNMUSIC_KUGOU_MID", ""),
    "kugou_guid": os.environ.get("FNMUSIC_KUGOU_GUID", ""),
    "kugou_dev": os.environ.get("FNMUSIC_KUGOU_DEV", ""),
    "kugou_mac": os.environ.get("FNMUSIC_KUGOU_MAC", ""),
    "netease_enabled": os.environ.get("FNMUSIC_NETEASE_ENABLED", "true").lower() in ("true", "1", "yes"),
    "netease_wait_s": float(os.environ.get("FNMUSIC_NETEASE_WAIT_S", "2.5")),
    "netease_quality": os.environ.get("FNMUSIC_NETEASE_QUALITY", "lossless"),
    "netease_search_limit": int(os.environ.get("FNMUSIC_NETEASE_SEARCH_LIMIT", "50")),
    "upstream_sock": os.environ.get("FNMUSIC_UPSTREAM_SOCK", "/var/run/trim_music_upstream.socket"),
    "online_limit": int(os.environ.get("FNMUSIC_ONLINE_LIMIT", "30")),
    "search_list_path": os.environ.get("FNMUSIC_SEARCH_LIST_PATH", "data.list"),
    "cache_dir": os.environ.get("FNMUSIC_CACHE_DIR", os.path.join(_HOME, "cache")),
    # 代理主动上传封面换官方 coverId 并回写 /track/metadata；失败静默，不影响封面出图。
    "cover_upload_enabled": os.environ.get("FNMUSIC_COVER_UPLOAD_ENABLED", "true").lower() in ("true", "1", "yes"),
    "cover_upload_max_bytes": int(os.environ.get("FNMUSIC_COVER_UPLOAD_MAX_BYTES", str(4 * 1024 * 1024)) or 4194304),
    "cover_upload_timeout": float(os.environ.get("FNMUSIC_COVER_UPLOAD_TIMEOUT", "15")),
    # 换官方 coverId 时按该分辨率取图再上传；0=不缩放原样上传。
    # 酷狗 URL 直接把 size 占位符填成该值；本地 sidecar 文件用 ffmpeg 缩到该宽度。
    "cover_upload_size": int(os.environ.get("FNMUSIC_COVER_UPLOAD_SIZE", "1600") or 1600),
    "cover_id_cache_file": os.environ.get(
        "FNMUSIC_COVER_ID_CACHE_FILE", os.path.join(_HOME, "cache", "cover_ids.json")
    ),
    # 空=从飞牛 shared_library.path 自动探测；测试可覆盖到临时目录
    "library_dir": os.environ.get("FNMUSIC_LIBRARY_DIR", ""),
    "music_db": os.environ.get(
        "FNMUSIC_MUSIC_DB", "/usr/local/apps/@appdata/trim.music/db/music.db"
    ),
    "merge_suggest": os.environ.get("FNMUSIC_MERGE_SUGGEST", "true").lower() in ("true", "1", "yes"),
    "merge_search_meta": os.environ.get("FNMUSIC_MERGE_SEARCH_META", "true").lower() in ("true", "1", "yes"),
    "online_sources": os.environ.get("FNMUSIC_ONLINE_SOURCES", "KuwoMusicClient,MiguMusicClient"),
    "lyric_field": os.environ.get("FNMUSIC_LYRIC_FIELD", "data.lyric"),
    "search_timeout": float(os.environ.get("FNMUSIC_SEARCH_TIMEOUT", "15")),
    "search_cache_ttl": float(os.environ.get("FNMUSIC_SEARCH_CACHE_TTL", "300")),
    # 歌手/专辑/歌单全量拉取（手机端不传 page/size 时触发）的分页参数
    # 酷狗 /search 有结果条数上限（歌曲/歌单 480，专辑/歌手 500）且末页必须
    # 满足 from + size <= 上限，否则返回 149 "Out Page Range"（HTTP 502，body
    # 里 error_code=149 / from / size）。步长取 50 即可通吃两类：末页用剩余
    # 条数当 size（余数末页法），页号按本页 size 重算，from + size 恰好等于
    # 上限，不越界，故不需要挑 480 的因子。见 _kugou_page_params。
    "kugou_search_limit": int(os.environ.get("FNMUSIC_KUGOU_SEARCH_LIMIT", "480")),
    "kugou_meta_limit": int(os.environ.get("FNMUSIC_KUGOU_META_LIMIT", "500")),
    "kugou_step": int(os.environ.get("FNMUSIC_KUGOU_STEP", "50")),
    "meta_full_poll_page_timeout": float(os.environ.get("FNMUSIC_META_FULL_POLL_PAGE_TIMEOUT", "8")),
    "late_page_wait_s": float(os.environ.get("FNMUSIC_LATE_PAGE_WAIT_S", "5")),
    "fav_dir": os.environ.get(
        "FNMUSIC_FAV_DIR", os.path.join(_HOME, "online_favorites")
    ),
}

_KUGOU_PLAYLIST_PREFIX = "online:kugou:playlist:"


def kugou_playlist_guid(remote_id: str, name: str = "") -> str:
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", str(remote_id or "").strip()) or "unknown"
    return f"{_KUGOU_PLAYLIST_PREFIX}{safe}"


def is_kugou_playlist_guid(guid: str | None) -> bool:
    return str(guid or "").startswith(_KUGOU_PLAYLIST_PREFIX)


def kugou_playlist_id_from_guid(guid: str) -> str:
    s = str(guid or "")
    if not s.startswith(_KUGOU_PLAYLIST_PREFIX):
        return ""
    rest = s[len(_KUGOU_PLAYLIST_PREFIX):]
    return rest.split(":", 1)[0]


def _kugou_playlist_name_from_guid(guid: str) -> str:
    s = str(guid or "")
    if not s.startswith(_KUGOU_PLAYLIST_PREFIX):
        return ""
    rest = s[len(_KUGOU_PLAYLIST_PREFIX):]
    if ":" not in rest:
        return ""
    return rest.split(":", 1)[1]


def _kugou_playlist_field(it: dict, keys: tuple[str, ...], default: Any = "") -> Any:
    for k in keys:
        if it.get(k) not in (None, ""):
            return it.get(k)
    return default


def build_kugou_playlist_obj(it: dict) -> dict:
    # 酷狗 /user/playlist 返回: global_collection_id / listid / name / count / owner / pic
    coll_id = str(_kugou_playlist_field(it, ("global_collection_id", "globalCollectionId", "collection_id")) or "").strip()
    pid = str(_kugou_playlist_field(it, ("listid", "listId", "id", "playlistId", "playlist_id", "pid", "playId")) or "").strip()
    key = coll_id or pid
    name = str(_kugou_playlist_field(it, ("name", "title", "playlistName", "playlist_name")) or "").strip()
    guid = kugou_playlist_guid(key, name)
    # 歌曲数: 酷狗 /user/playlist 的 data.info[] 每项顶层 count 字段（即 data.info.count）
    track_count = int(_kugou_playlist_field(it, ("count", "trackCount", "track_count", "num", "playlist_num")) or 0)
    return {
        "guid": guid,
        "name": name or ("酷狗歌单" if key else ""),
        "coverId": guid,
        "collectionId": coll_id,
        "trackCount": track_count,
    }


def stamp_kugou_playlist_tracks(items: list[dict], now: float | None = None) -> list[dict]:
    ts = int(now or time.time())
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        # /playlist/track/all 的真实字段：FileName/SingerName/AlbumName/Duration/Image...
        raw = kugou_source.track_item_to_raw(it)
        title = str(raw.get("title") or "").strip()
        artist = str(raw.get("artist") or "").strip()
        album = str(raw.get("album") or "").strip()
        dur = raw.get("duration_s") or 0
        try:
            dur_s = float(dur or 0)
        except (TypeError, ValueError):
            dur_s = 0.0
        ext = str(raw.get("ext") or "mp3").strip().lower() or "mp3"
        hashv = str(raw.get("hash") or "").strip()
        guid = online_guid_from_item({"id": f"kugou:{hashv}", "source": "kugou"}) if hashv else ""
        cover = str(raw.get("cover_url") or "").strip()
        # 酷狗 URL 常见格式：.../Image/{size}x{size}/xxx.jpg；若缺少 size 则按前端尺寸补成 240x240
        if cover and "{size}" in cover:
            cover = cover.replace("{size}", "240")
        item = {
            "guid": guid,
            "id": guid,
            "title": title,
            "name": title,
            "artist": artist,
            "artists": [{"name": artist, "guid": f"{guid}:artist"}] if artist else [],
            "album": {
                "name": album,
                "guid": f"{guid}:album",
                "coverId": guid,
                "artists": [{"name": artist, "guid": f"{guid}:artist"}] if artist else [],
            },
            "duration": int(dur_s * 1000),
            "duration_ms": int(dur_s * 1000),
            "durationMs": int(dur_s * 1000),
            "duration_s": dur_s,
            "ext": ext,
            "format": ext,
            "coverId": guid,
            "coverUrl": cover,
            "cover_url": cover,
            "union_cover": cover,
            "source": "kugou",
            "is_online": bool(guid),
            "createdAt": ts,
            "updatedAt": ts,
            "isFavorite": False,
            "isCue": False,
            "accessStatus": 0,
        }
        out.append(item)
    return out


async def fetch_kugou_user_playlist_bundles() -> list[dict]:
    if not CONF.get("kugou_enabled", True):
        return []
    try:
        res = await kugou_source.get_user_playlists(1, 500)
    except Exception as e:
        logger.warning("[KUGOU_PLAYLIST] fetch failed: %s", e)
        return []
    return [build_kugou_playlist_obj(x) for x in (res.get("items") or []) if isinstance(x, dict)]


def stable_hash64(value: str) -> str:
    """稳定生成 64 位小写 hex，用来给酷狗数字 ID 补齐飞牛 GUID。"""
    digest = hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()
    return digest[:64]


def kugou_artist_guid(artist_id: int | str) -> str:
    return stable_hash64(f"kugou:artist:{artist_id}")


def kugou_album_guid(album_id: int | str) -> str:
    """专辑 GUID：online:kugou:album:<album_id>。封面解析由对应路由自己负责。"""
    return f"online:kugou:album:{album_id}"


def kugou_artist_cover_guid(artist_id: int | str) -> str:
    """歌手封面 ID 与歌手 GUID 同值，由封面解析路由自己识别。"""
    return f"online:kugou:artist:{artist_id}"


def kugou_album_cover_guid(album_id: int | str) -> str:
    """专辑封面 ID 与专辑 GUID 同值，由封面解析路由自己识别。"""
    return f"online:kugou:album:{album_id}"


def parse_kugou_artist_guid(raw_guid: str) -> tuple[str, str]:
    """解析酷狗歌手 GUID。

    当前在线歌曲歌手 GUID 形如 "kugou:artist:750408"，直接返回数字 ID；
    如果后续使用稳定 hash GUID，则原样返回，上层再决定是否反查上游。
    """
    s = str(raw_guid or "").strip()
    if s.startswith("online:kugou:artist:"):
        return s[len("online:kugou:artist:"):], "kugou_artist"
    if len(s) == 64 and not s.isdigit():
        return s, "hash_guid"
    return s, "id"


def parse_ts_to_unix(value: Any) -> int:
    """把 KuGouMusicApi 时间字段转成 Unix 秒；已是秒级时间戳则直接返回。"""
    if value in (None, ""):
        return int(time.time())
    if isinstance(value, (int, float)):
        ts = int(value)
        return ts if ts > 1000000000 else ts
    text = str(value).strip().replace("T", " ").replace("Z", "")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y/%m/%d"):
        try:
            return int(datetime.strptime(text[:19], fmt).timestamp())
        except (TypeError, ValueError):
            continue
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return int(time.time())


async def fetch_kugou_artist_album_list(app_state, artist_guid: str, page: int = 1, size: int = 60) -> dict | None:
    """酷狗歌手专辑 -> 飞牛 album/artist-detail/list 的专辑列表。

    数据源：KuGouMusicApi /artist/albums?id=<artist_id>
    （不再从 /artist/audios 聚合——那个接口拿不到真实专辑元数据，
      多歌手专辑也无法归集，trackCount 更是靠数本页歌曲凑出来的假数。）
    """
    parsed_artist_id, artist_kind = parse_kugou_artist_guid(artist_guid)
    if not parsed_artist_id or artist_kind != "kugou_artist":
        return None
    artist_id = parsed_artist_id
    try:
        result = await kugou_source.get_artist_albums(artist_id, page=page, pagesize=size)
    except Exception as e:
        logger.warning("[KUGOU_ARTIST_ALBUM] artist_id=%s error=%s", artist_id, e)
        return None
    items = result.get("items") or []
    logger.warning("[KUGOU_ARTIST_ALBUM] artist_id=%s page=%s size=%s got=%s total=%s",
                   artist_id, page, size, len(items), result.get("total"))

    seen: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        album_id = str(item.get("album_id") or "").strip()
        album_name = str(item.get("album_name") or item.get("album") or "").strip()
        if not album_id and not album_name:
            continue

        release_date = str(item.get("publish_date") or "").strip() or None
        ts = parse_ts_to_unix(release_date or time.time())
        # /artist/albums 回参没有专辑曲目数字段；前端该列展示为来源标识。
        track_count = "酷狗源"

        artists: list[dict[str, Any]] = []
        for art in item.get("artists") or []:
            if not isinstance(art, dict):
                continue
            name = str(art.get("name") or "").strip()
            if not name:
                continue
            aid = str(art.get("id") or "").strip()
            artists.append({
                # 有真实 author_id 用数字 ID，没有（仅 author_name 拆分而来）回退到当前歌手。
                "guid": f"online:kugou:artist:{aid}" if aid else f"online:kugou:artist:{artist_id}",
                "name": name,
                "coverId": kugou_artist_cover_guid(aid) if aid else kugou_artist_cover_guid(artist_id),
                "createdAt": ts,
                "updatedAt": ts,
            })
        # /artist/albums 的 authors 按酷狗自身顺序返回，当前歌手可能排在中间
        # （如「讯号」里郁可唯在第 6 位）。列表是「该歌手的专辑」，把当前歌手
        # 稳定排到最前，其余保持酷狗原始相对顺序。
        self_guid = f"online:kugou:artist:{artist_id}"
        artists.sort(key=lambda a: 0 if a.get("guid") == self_guid else 1)
        if not artists:
            continue

        key = album_id or album_name
        if key not in seen:
            seen[key] = {
                "guid": kugou_album_guid(album_id or album_name),
                "name": album_name,
                "coverId": kugou_album_cover_guid(album_id or album_name),
                "releaseDate": release_date,
                "barcode": None,
                "createdAt": ts,
                "updatedAt": ts,
                "artists": artists,
                "trackCount": track_count,
                "language": str(item.get("language") or "").strip() or None,
                "albumType": str(item.get("album_type") or "").strip() or None,
                "publishCompany": str(item.get("publish_company") or "").strip() or None,
            }
            order.append(key)
        entry = seen[key]
        entry["trackCount"] = track_count
        if not entry.get("releaseDate") and release_date:
            entry["releaseDate"] = release_date
        if not entry.get("language") and item.get("language"):
            entry["language"] = str(item.get("language") or "").strip() or None
        if not entry.get("albumType") and item.get("album_type"):
            entry["albumType"] = str(item.get("album_type") or "").strip() or None

    if not seen:
        return {
            "code": 0,
            "msg": "",
            "data": {"list": [], "total": int(result.get("total") or 0), "sort": "publishDate,desc"},
        }
    album_list = [seen[k] for k in order]
    return {
        "code": 0,
        "msg": "",
        "data": {"list": album_list, "total": int(result.get("total") or len(album_list)), "sort": "publishDate,desc"},
    }


async def fetch_kugou_artist_detail(app_state, artist_guid: str) -> dict | None:
    """酷狗歌手详情 -> 飞牛 artist/detail。"""
    parsed_artist_id, artist_kind = parse_kugou_artist_guid(artist_guid)
    if not parsed_artist_id or artist_kind != "kugou_artist":
        return None
    artist_id = parsed_artist_id
    now = int(time.time())
    try:
        async with httpx.AsyncClient(base_url=CONF["kugou_url"], timeout=float(CONF["kugou_search_timeout"]), follow_redirects=True) as c:
            auth = kugou_source._auth_header()
            headers = {"Authorization": auth} if auth else {}
            r = await c.get("/artist/detail", params={"id": artist_id}, headers=headers)
            if r.status_code != 200:
                logger.warning("[KUGOU_ARTIST_DETAIL] http=%s artist_id=%s body=%r", r.status_code, artist_id, r.text[:200])
                return None
            body = r.json()
            data = body.get("data") or {}
            if isinstance(data, list):
                data = data[0] if data and isinstance(data[0], dict) else {}
            if not isinstance(data, dict):
                logger.warning("[KUGOU_ARTIST_DETAIL] bad body artist_id=%s body=%r", artist_id, body)
                return None
            name = str(data.get("author_name") or data.get("name") or data.get("artist") or "").strip()
            track_count = int(data.get("song_count") or data.get("trackCount") or data.get("track_count") or 0)
            album_count = int(data.get("album_count") or data.get("albumCount") or 0)
            cover_url = str(data.get("sizable_avatar") or data.get("avatar") or data.get("pic") or "").strip()
            if not name:
                logger.warning("[KUGOU_ARTIST_DETAIL] empty name artist_id=%s body=%r", artist_id, body)
                return None
            guid = f"online:kugou:artist:{artist_id}"
            return {
                "code": 0,
                "msg": "",
                "data": {
                    "guid": guid,
                    "name": name,
                    "coverId": guid,
                    "coverUrl": cover_url,
                    "createdAt": now,
                    "updatedAt": now,
                    "trackCount": track_count,
                    "albumCount": album_count,
                },
            }
    except Exception as e:
        logger.warning("[KUGOU_ARTIST_DETAIL] error artist_id=%s err=%s", artist_id, e)
        return None


async def fetch_kugou_artist_tracks(app_state, artist_guid: str, page: int = 1, size: int = 50) -> dict:
    """酷狗歌手作品 -> 飞牛 track/artist-detail/list 的歌曲列表。

    上游接口：KuGouMusicApi /artist/audios?id=<artist_id>&sort=hot&page=&pagesize=。
    返回结构与 playlist-detail/list 一致：{"items": [...], "total": n, "page": p, "pagesize": s}。
    items 已由 get_artist_audios 归一化为内部格式，可直接喂给 build_online_track。
    """
    artist_id, artist_kind = parse_kugou_artist_guid(artist_guid)
    if artist_kind != "kugou_artist" or not artist_id:
        return {"items": [], "total": 0, "page": page, "pagesize": size}
    try:
        payload = await kugou_source.get_artist_audios(artist_id, sort="hot", page=page, pagesize=size)
    except Exception as e:
        logger.warning("[KUGOU_ARTIST_TRACKS] artist_id=%s page=%s size=%s err=%s", artist_id, page, size, e)
        return {"items": [], "total": 0, "page": page, "pagesize": size}
    items = payload.get("items") or []
    logger.warning("[KUGOU_ARTIST_TRACKS] artist_id=%s page=%s size=%s got=%s total=%s",
                   artist_id, page, size, len(items), payload.get("total"))
    return {"items": items, "total": int(payload.get("total") or len(items)), "page": page, "pagesize": size}


async def fetch_kugou_playlist_tracks(app_state, guid: str, page: int = 1, size: int = 50) -> dict:
    if not is_kugou_playlist_guid(guid):
        return {"items": [], "total": 0, "page": page, "pagesize": size}
    pid = kugou_playlist_id_from_guid(guid)
    if not pid:
        return {"items": [], "total": 0, "page": page, "pagesize": size}
    try:
        async with httpx.AsyncClient(base_url=CONF["kugou_url"], timeout=float(CONF["kugou_search_timeout"]), follow_redirects=True) as c:
            auth = kugou_source._auth_header()
            headers = {"Authorization": auth} if auth else {}
            r = await c.get("/playlist/track/all", params={"id": pid, "page": page, "pagesize": size}, headers=headers)
            if r.status_code != 200:
                return {"items": [], "total": 0, "page": page, "pagesize": size}
            data = r.json()
            st = data.get("status", data.get("code"))
            if st not in (1, 200, 0):
                return {"items": [], "total": 0, "page": page, "pagesize": size}
            payload = data.get("data") or {}
            if isinstance(payload, list):
                payload = {"list": payload}
            raw_list = None
            for key in ("list", "items", "lists", "data", "songs", "records", "info"):
                v = payload.get(key)
                if isinstance(v, list):
                    raw_list = v
                    break
            if raw_list is None and isinstance(payload, dict):
                for v in payload.values():
                    if isinstance(v, list):
                        raw_list = v
                        break
            total = int(payload.get("total") or payload.get("count") or len(raw_list or []))
            logger.warning("[KUGOU_PLAYLIST_TRACKS] guid=%s status=%s payload_keys=%s raw_len=%s first_keys=%s",
                           guid, st, list(payload.keys())[:20] if isinstance(payload, dict) else type(payload).__name__,
                           len(raw_list or []), list((raw_list[0].keys()) if raw_list and isinstance(raw_list[0], dict) else [])[:30])
            return {"items": raw_list or [], "total": total, "page": page, "pagesize": size}
    except Exception as e:
        logger.warning("[KUGOU_PLAYLIST_TRACKS] guid=%s err=%s", guid, e)
        return {"items": [], "total": 0, "page": page, "pagesize": size}

# ===== KuGouMusicApi 源适配（新增）=====
import kugou_source  # noqa: E402

# ===== 启动时从 .env 加载持久化凭证（若存在则优先于环境变量）=====
_ENV_FILE = Path(__file__).resolve().parents[1] / ".env"
if _ENV_FILE.exists():
    try:
        _loaded_env = {}
        for _line in _ENV_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
            _line = _line.strip()
            if not _line or _line.startswith("#") or "=" not in _line:
                continue
            _k, _v = _line.split("=", 1)
            _loaded_env[_k.strip()] = _v.strip()
        _KUGOU_KEY_MAP = {
            "FNMUSIC_KUGOU_ENABLED": ("kugou_enabled", "bool"),
            "FNMUSIC_KUGOU_URL": ("kugou_url", "str"),
            "FNMUSIC_KUGOU_QUALITY": ("kugou_quality", "str"),
            "FNMUSIC_KUGOU_TOKEN": ("kugou_token", "str"),
            "FNMUSIC_KUGOU_USERID": ("kugou_userid", "str"),
            "FNMUSIC_KUGOU_DFID": ("kugou_dfid", "str"),
            "FNMUSIC_KUGOU_/track/": ("kugou_t1", "str"),
            "FNMUSIC_KUGOU_MID": ("kugou_mid", "str"),
            "FNMUSIC_KUGOU_GUID": ("kugou_guid", "str"),
            "FNMUSIC_KUGOU_DEV": ("kugou_dev", "str"),
            "FNMUSIC_KUGOU_MAC": ("kugou_mac", "str"),
        }
        _changed = False
        for _env_k, (_conf_k, _t) in _KUGOU_KEY_MAP.items():
            if _env_k in _loaded_env and _loaded_env[_env_k]:
                _v = _loaded_env[_env_k]
                if _t == "bool":
                    _v = _v.lower() in ("1", "true", "yes")
                if CONF.get(_conf_k) != _v:
                    CONF[_conf_k] = _v
                    _changed = True
        logger.info(".env loaded: kugou_url=%s token_len=%d userid=%s",
                    CONF["kugou_url"], len(CONF["kugou_token"] or ""), CONF["kugou_userid"] or "(none)")
    except Exception as e:
        logger.warning("Failed to load .env: %s", e)

# 将 CONF 当前值注入 kugou_source
kugou_source.set_config({
    "kugou_url": CONF["kugou_url"],
    "kugou_quality": CONF["kugou_quality"],
    "kugou_search_timeout": CONF["kugou_search_timeout"],
    "kugou_enabled": CONF["kugou_enabled"],
    "kugou_token": CONF["kugou_token"],
    "kugou_userid": CONF["kugou_userid"],
    "kugou_dfid": CONF["kugou_dfid"],
    "kugou_t1": CONF["kugou_t1"],
    "kugou_mid": CONF["kugou_mid"],
    "kugou_guid": CONF["kugou_guid"],
    "kugou_dev": CONF["kugou_dev"],
    "kugou_mac": CONF["kugou_mac"],
})


async def fetch_kugou_search(keyword: str, limit: int, page: int = 1) -> dict | None:
    """调 KuGouMusicApi /search，返回统一结果对象，包含 items 和 total。"""
    try:
        return await kugou_source.search(keyword, limit, page)
    except Exception as e:
        logger.warning("kugou search failed: %s", e)
        return None


async def resolve_kugou_url(song_id: str) -> tuple[str | None, str | None]:
    """调 KuGouMusicApi /song/url，返回 (play_url, ext)。"""
    try:
        return await kugou_source.resolve_url(song_id)
    except Exception as e:
        logger.warning("kugou resolve_url failed: %s", e)
        return None, None


async def resolve_kugou_lyric(song_id: str) -> str:
    """调 KuGouMusicApi 拉歌词文本，失败返回空。"""
    try:
        return await kugou_source.fetch_lyric(song_id)
    except Exception as e:
        logger.warning("kugou fetch_lyric failed: %s", e)
        return ""

_REDACT_KEY_PARTS = ("api_key", "apikey", "token", "secret", "password")

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}

CACHE_EXTS = ("mp3", "flac", "wav", "ogg", "opus", "m4a", "aac", "ape", "wv", "dsf", "dff", "tta")

# 飞牛 Kl() 归一化：mpeg/mp3→mp3，wav/pcm→wav，m4a/aac/mp4→m4a，其余小写原样（flac/ogg/ape/wv…）
_FORMAT_ALIASES = {
    "mp3": "mp3",
    "mpeg": "mp3",
    "mpga": "mp3",
    "flac": "flac",
    "wav": "wav",
    "wave": "wav",
    "pcm": "wav",
    "lpcm": "wav",
    "ogg": "ogg",
    "vorbis": "ogg",
    "opus": "opus",
    "m4a": "m4a",
    "mp4": "m4a",
    "mp4a": "m4a",
    "aac": "m4a",
    "alac": "m4a",
    "ape": "ape",
    "wv": "wv",
    "wavpack": "wv",
    "dsf": "dsf",
    "dff": "dff",
    "dsd": "dsd",
    "tta": "tta",
    "tak": "tak",
    "wma": "wma",
    "aiff": "aiff",
    "aif": "aiff",
}


# 模块级搜索缓存
_SEARCH_CACHE: dict[str, dict] = {}
_STREAM_CACHE_MAX_ENTRIES = 8
_STREAM_CACHE_MAX_BYTES = 320 * 1024 * 1024  # 320 MB,防止长时间试听占满内存

# guid -> {"body": bytes, "ext": str, "ts": float}
_STREAM_CACHE: dict[str, dict] = {}


def _clean_search_cache() -> None:
    """写入时若 len(_SEARCH_CACHE) > 200，按 ts 升序砍掉最旧一半。"""
    if len(_SEARCH_CACHE) > 200:
        sorted_keys = sorted(_SEARCH_CACHE.keys(), key=lambda k: _SEARCH_CACHE[k].get("ts", 0))
        to_remove = sorted_keys[: len(sorted_keys) // 2]
        for k in to_remove:
            _SEARCH_CACHE.pop(k, None)


def _set_search_cache(keyword: str, entry: dict) -> None:
    _clean_search_cache()
    _SEARCH_CACHE[keyword] = entry


def _clean_stream_cache() -> None:
    """超出容量时按最近使用时间淘汰；纯内存缓存，不落盘。"""
    while len(_STREAM_CACHE) > _STREAM_CACHE_MAX_ENTRIES:
        oldest_guid = min(_STREAM_CACHE, key=lambda k: _STREAM_CACHE[k].get("ts", 0))
        _STREAM_CACHE.pop(oldest_guid, None)
    while True:
        total = sum(len(entry.get("body") or b"") for entry in _STREAM_CACHE.values())
        if total <= _STREAM_CACHE_MAX_BYTES or not _STREAM_CACHE:
            break
        oldest_guid = min(_STREAM_CACHE, key=lambda k: _STREAM_CACHE[k].get("ts", 0))
        _STREAM_CACHE.pop(oldest_guid, None)


def remember_stream_audio(guid: str, body: bytes, ext: str) -> None:
    """试听缓冲缓存到内存；进程重启即失效。"""
    if not guid or not body or len(body) < 1024:
        return
    if len(body) > 48 * 1024 * 1024:  # 单首超过 48MB 不放内存，避免异常大音频撑爆
        return
    _clean_stream_cache()
    _STREAM_CACHE[guid] = {
        "body": body,
        "ext": (ext or "mp3").lower(),
        "ts": time.time(),
    }
    logger.warning("[STREAM_CACHE] remembered guid=%s size=%d ext=%s entries=%d", guid, len(body), ext, len(_STREAM_CACHE))


def get_stream_audio(guid: str) -> bytes | None:
    entry = _STREAM_CACHE.get(guid)
    if not entry:
        return None
    body = entry.get("body")
    if body:
        entry["ts"] = time.time()
        logger.warning("[STREAM_CACHE] hit guid=%s size=%d", guid, len(body))
        return body
    return None


_STREAM_LYRIC_CACHE: dict[str, str] = {}


def remember_stream_lyric(guid: str, text: str) -> None:
    """歌词也只做进程内缓存，避免写 .lrc/.ref 到磁盘。"""
    if not guid or not text:
        return
    if len(text) > 256 * 1024:
        return
    _STREAM_LYRIC_CACHE[guid] = text
    logger.warning("[STREAM_LYRIC_CACHE] remembered guid=%s size=%d entries=%d", guid, len(text), len(_STREAM_LYRIC_CACHE))


def get_stream_lyric(guid: str) -> str:
    return _STREAM_LYRIC_CACHE.get(guid, "")


# === 本地封面缓存：coverId=guid 的解析结果进程内记忆，避免每张封面都回源 metadata + 酷狗搜索 ===

_LOCAL_COVER_URL_TTL_S = 24 * 3600
_LOCAL_COVER_FAIL_TTL_S = 5 * 60
_LOCAL_COVER_URL_CACHE: dict[str, tuple[float, str]] = {}


def remember_local_cover_url(guid: str, url: str) -> None:
    """记住本地曲子的封面 URL；url 为空表示解析失败，短缓存以免每张封面都打回源。"""
    if not guid:
        return
    ttl = _LOCAL_COVER_URL_TTL_S if url else _LOCAL_COVER_FAIL_TTL_S
    _LOCAL_COVER_URL_CACHE[guid] = (time.time() + ttl, url)


def remembered_local_cover_url(guid: str) -> str | None:
    """命中未过期缓存返回 URL 模板（可能为空串），未命中或过期返回 None。

    缓存的是未替换 {size} 占位符的原始 URL，取出后需再用 _fill_cover_size 按本次请求填尺寸。
    """
    entry = _LOCAL_COVER_URL_CACHE.get(guid)
    if not entry:
        return None
    expire_at, url = entry
    if expire_at <= time.time():
        _LOCAL_COVER_URL_CACHE.pop(guid, None)
        return None
    return url


def deduplicate_online_items(items: list[dict]) -> list[dict]:
    """在线条目合并去重：按 (title, artist) 小写，保留最先出现的（musicbox 优先）。"""
    seen = set()
    res = []
    for it in items:
        t = str(it.get("title") or it.get("name") or "").strip().lower()
        a = str(it.get("artist") or "").strip().lower()
        if t and a:
            key = (t, a)
            if key in seen:
                continue
            seen.add(key)
        res.append(it)
    return res


def play_format_from_ext(ext: str | None) -> str:
    raw = (ext or "mp3").strip().lower().lstrip(".")
    if raw.startswith("audio/"):
        raw = raw.split("/", 1)[-1]
    return _FORMAT_ALIASES.get(raw, raw or "mp3")


def filter_headers(headers: Any, exclude_keys: set | None = None) -> dict:
    exclude = HOP_BY_HOP | {k.lower() for k in (exclude_keys or set())}
    return {k: v for k, v in headers.items() if k.lower() not in exclude}


def copy_incoming_headers(request: Request) -> dict:
    """透传鉴权 Cookie / Token。Starlette 头名为小写，需显式回填以免丢失 music-token。"""
    headers = filter_headers(request.headers, exclude_keys={"host", "content-length"})
    headers["accept-encoding"] = "identity"
    for key in ("cookie", "authorization", "x-trim-music-temp-token"):
        val = request.headers.get(key)
        if val:
            headers[key] = val
    return headers


def get_by_path(d: Any, path: str) -> Any:
    curr = d
    for p in path.split("."):
        if isinstance(curr, dict) and p in curr:
            curr = curr[p]
        else:
            return None
    return curr


def _kugou_item_matches_local(item: dict, title: str, artist: str, duration: float) -> tuple[int, int, int, int, bool, float, str, str]:
    """按歌名/歌手/时长给酷狗候选打分。

    返回 (score, dur_rank, title_rank, matched, dur_diff, cand_title, cand_artist)。
    matched=True 表示可以作为"同款"版本：
      - 本地同时有歌名和歌手时，必须两者都命中（只同名不同人的不算）；
      - 本地只有其中一个时，要求那一个命中。
    """
    it_title = str(item.get("title") or item.get("FileName") or "").strip()
    it_artist = str(item.get("artist") or item.get("SingerName") or "").strip()
    try:
        it_dur = float(item.get("duration_s") or item.get("Duration") or 0)
    except Exception:
        it_dur = 0.0
    diff = abs(it_dur - duration) if duration > 0 and it_dur > 0 else 1e9
    score = 0
    title_hit = False
    if it_title and title and it_title == title:
        score += 1000
        title_hit = True
    elif it_title and title and it_title in title:
        score += 600
        title_hit = True
    elif it_title and title and title in it_title:
        score += 500
        title_hit = True
    artist_hit = False
    if it_artist and artist and it_artist == artist:
        score += 800
        artist_hit = True
    elif it_artist and artist and it_artist in artist:
        score += 450
        artist_hit = True
    elif it_artist and artist and artist in it_artist:
        score += 350
        artist_hit = True
    if diff <= 0.5:
        score += 2000
    elif diff <= 1.5:
        score += 1200
    elif diff <= 3.0:
        score += 600
    elif diff <= 5.0:
        score += 200
    if title and artist:
        matched = title_hit and artist_hit
    elif title:
        matched = title_hit
    elif artist:
        matched = artist_hit
    else:
        matched = False
    return (score, 0 if diff <= 1.5 else 1, 0 if it_title == title else 1, matched, diff, it_title, it_artist)


async def _kugou_candidates(keywords: str, limit: int = 30) -> list[dict]:
    """搜酷狗返回统一格式候选；失败或空都返回空列表。"""
    try:
        candidates = await kugou_source.search(keywords, limit=limit, page=1)
    except Exception as e:
        logger.warning("[KUGOU_SEARCH] error keywords=%r err=%s", keywords, e)
        return []
    items = (candidates or {}).get("items") or [] if isinstance(candidates, dict) else []
    return [x for x in items if isinstance(x, dict)]


async def fetch_local_lyric_by_keywords(title: str, artist: str, duration: float = 0.0) -> str:
    """仅用于飞牛本地歌：先搜酷狗候选，再按相关度和时长匹配取歌词。"""
    if not CONF.get("kugou_enabled", True):
        return ""
    keywords = " ".join(x for x in [title, artist] if x and str(x).strip()).strip()
    if not keywords:
        return ""

    try:
        items = await _kugou_candidates(keywords, limit=30)
        if not items:
            logger.warning("[LYRIC_FALLBACK] search empty keywords=%r", keywords)
            return ""
        scored = []
        for idx, item in enumerate(items):
            scored.append((_kugou_item_matches_local(item, title, artist, duration)[:3] + (-idx,), item))
        scored.sort(key=lambda x: x[0], reverse=True)
        ranked = [item for _, item in scored]
        selected = ranked[0]
        sid = str(selected.get("id") or "").split(":", 1)[-1].strip()
        if not sid:
            logger.warning("[LYRIC_FALLBACK] selected no hash keywords=%r items=%d", keywords, len(items))
            return ""
        lyric_text = await kugou_source.fetch_lyric(sid)
        logger.warning(
            "[LYRIC_FALLBACK] selected hash=%s title=%r artist=%r duration=%s lyrics_len=%d candidates=%d total=%s",
            sid,
            selected.get("title"),
            selected.get("artist"),
            selected.get("duration_s"),
            len(lyric_text),
            len(items),
        )
        return lyric_text
    except Exception as e:
        logger.warning("[LYRIC_FALLBACK] error keywords=%r err=%s", keywords, e)
        return ""


def _cover_url_from_item(item: dict) -> str:
    """取统一格式候选里的封面 URL。6c7213d 后 cover 字段已被拍平，按优先级取值。"""
    for key in ("union_cover", "cover_url", "cover", "image", "coverUrl", "picUrl"):
        value = item.get(key)
        if value:
            return str(value).strip()
    return ""


async def _local_audio_metadata(request: Request, client: httpx.AsyncClient, guid: str) -> tuple[str, str, float]:
    """回查上游 metadata 取歌名/歌手/时长；失败返回空。"""
    try:
        meta_req = client.build_request(
            "GET",
            f"/music/api/v1/track/metadata?guid={quote(guid, safe='')}",
            headers=copy_incoming_headers(request),
        )
        meta_resp = await client.send(meta_req)
        if meta_resp.status_code != 200:
            logger.warning("[STATIC_COVER] metadata probe http=%d guid=%s", meta_resp.status_code, guid)
            return "", "", 0.0
        meta_payload = meta_resp.json()
        if not isinstance(meta_payload, dict):
            return "", "", 0.0
        return _extract_local_lyric_meta(meta_payload)
    except Exception as e:
        logger.warning("[STATIC_COVER] metadata probe error guid=%s err=%s", guid, e)
        return "", "", 0.0


def _tags_from_local_audio(audio_path: str) -> tuple[str, str]:
    """读本地音频标签里的歌名/歌手；metadata 拿不到时用它兜底。"""
    if not audio_path or not os.path.exists(audio_path) or os.path.getsize(audio_path) <= 0:
        return "", ""
    try:
        from mutagen import File as MutagenFile
        audio = MutagenFile(audio_path, easy=True)
        if audio is None or getattr(audio, "tags", None) is None:
            return "", ""
        tags = audio.tags
        title = ""
        artist = ""
        for name in ("title", "songname", "song", "name"):
            val = getattr(tags, name, None)
            if val:
                try:
                    title = str(val[0]) if isinstance(val, (list, tuple)) else str(val)
                except Exception:
                    title = str(val)
                break
        for name in ("artist", "singer", "albumartist", "performer", "author"):
            val = getattr(tags, name, None)
            if val:
                try:
                    artist = str(val[0]) if isinstance(val, (list, tuple)) else str(val)
                except Exception:
                    artist = str(val)
                break
        if not title:
            stem = os.path.basename(os.path.splitext(audio_path)[0])
            if " - " in stem:
                a, t = stem.split(" - ", 1)
                artist = artist or a.strip()
                title = t.strip()
            else:
                title = stem.strip()
        return title.strip(), artist.strip()
    except Exception as e:
        logger.warning("[STATIC_COVER] read tags failed path=%s err=%s", audio_path, e)
        return "", ""


def _local_art_file(audio_path: str) -> str | None:
    """查音频同目录的 .cover.jpg / .cover.png / .jpg 封面文件。"""
    if not audio_path:
        return None
    stem = os.path.splitext(audio_path)[0]
    for name in (stem + ".cover.jpg", stem + ".cover.png", stem + ".jpg", stem + ".jpeg", stem + ".png"):
        if os.path.exists(name) and os.path.getsize(name) > 0:
            return name
    return None


_ART_EXT_MIME = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
}


def _local_sidecar_art_response(guid: str) -> Response | None:
    """本地音频同目录有封面文件时直接同源返回，不必打酷狗。"""
    audio_path = find_cache_file(guid)
    art_path = _local_art_file(audio_path) if audio_path else None
    if not art_path:
        return None
    try:
        data = open(art_path, "rb").read()
    except Exception as e:
        logger.warning("[STATIC_COVER] read sidecar art failed path=%s err=%s", art_path, e)
        return None
    if not data:
        return None
    mime = _ART_EXT_MIME.get(os.path.splitext(art_path)[1].lower(), "image/jpeg")
    logger.warning("[STATIC_COVER] sidecar art served guid=%s path=%s bytes=%d", guid, art_path, len(data))
    return Response(
        content=data,
        status_code=200,
        media_type=mime,
        headers={
            "Cache-Control": "public, max-age=86400",
            "Cross-Origin-Resource-Policy": "same-origin",
        },
    )


def _extract_artist_payload(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return str(value.get("name") or value.get("artist") or value.get("singer") or "").strip()
    if isinstance(value, (list, tuple)) and value:
        parts = [ _extract_artist_payload(item) for item in value ]
        return "/".join(x for x in parts if x)
    return str(value).strip()


def _extract_local_lyric_meta(payload: dict) -> tuple[str, str, float]:
    data = payload.get("data")
    if not isinstance(data, dict):
        return "", "", 0.0
    title = str(data.get("title") or data.get("name") or "").strip()
    artist = _extract_artist_payload(data.get("artist") or data.get("singer") or data.get("artists") or data.get("albumArtist") or data.get("album_artists"))
    if not title or not artist:
        track = data.get("track")
        if isinstance(track, dict):
            title = title or str(track.get("title") or track.get("name") or "").strip()
            artist = artist or _extract_artist_payload(track.get("artist") or track.get("singer") or track.get("artists") or track.get("albumArtist"))
        inner = track.get("track") if isinstance(track, dict) else None
        if not artist and isinstance(inner, dict):
            artist = _extract_artist_payload(inner.get("artist") or inner.get("singer") or inner.get("artists") or inner.get("albumArtist"))
    if not artist and isinstance(data.get("album"), dict):
        artist = _extract_artist_payload(data["album"].get("artists") or data["album"].get("artist") or data["album"].get("singer"))
    duration = 0.0
    for path in (
        ["data", "duration_s"],
        ["data", "duration"],
        ["data", "durationMs"],
        ["data", "track", "duration_s"],
        ["data", "track", "duration"],
        ["data", "track", "durationMs"],
        ["track", "duration_s"],
        ["track", "duration"],
    ):
        raw = get_by_path(payload, ".".join(path))
        try:
            if raw is None or raw == "":
                continue
            val = float(raw)
            if val <= 0:
                continue
            if val > 10000:
                val = val / 1000.0
            duration = val
            break
        except (TypeError, ValueError):
            continue
    return title, artist, duration


async def fetch_local_lyric_for_guid(request: Request, client: httpx.AsyncClient, guid: str) -> str:
    """本地歌歌词只在 lyric/list 内获取：用 metadata 解析标题/歌手/时长后搜酷狗。"""
    if not guid:
        return ""
    meta_req = client.build_request(
        "GET",
        f"/music/api/v1/track/metadata?guid={quote(guid, safe='')}",
        headers=copy_incoming_headers(request),
    )
    meta_resp = await client.send(meta_req)
    if meta_resp.status_code != 200:
        logger.warning("[LYRIC_FALLBACK] lyric/list metadata probe failed guid=%s http=%s", guid, meta_resp.status_code)
        return ""
    try:
        meta_payload = meta_resp.json()
    except Exception:
        logger.warning("[LYRIC_FALLBACK] lyric/list metadata probe bad json guid=%s", guid)
        return ""
    if not isinstance(meta_payload, dict):
        return ""
    title, artist, duration = _extract_local_lyric_meta(meta_payload)
    if not (title and artist):
        logger.warning("[LYRIC_FALLBACK] lyric/list metadata no title/artist guid=%s title=%r artist=%r duration=%s", guid, title, artist, duration)
        return ""
    text = await fetch_local_lyric_by_keywords(title, artist, duration)
    if text:
        write_lyric_cache(guid, text, title=title, artist=artist)
        logger.warning("[LYRIC_FALLBACK] lyric/list fetched from metadata guid=%s title=%r artist=%r duration=%s len=%d", guid, title, artist, duration, len(text))
    else:
        logger.warning("[LYRIC_FALLBACK] lyric/list fetched empty from metadata guid=%s title=%r artist=%r duration=%s", guid, title, artist, duration)
    return text


def _existing_local_lyric(payload: dict) -> str:
    for path in (
        ["lyric"],
        ["data", "lyric"],
        ["data", "track", "lyric"],
        ["data", "track", "track", "lyric"],
        ["track", "lyric"],
        ["data", "list", "0", "lyric"],
        ["data", "items", "0", "lyric"],
        ["data", "track", "items", "0", "lyric"],
    ):
        val = get_by_path(payload, ".".join(path))
        if val:
            return str(val).strip()
    return ""


async def forward_upstream_with_local_lyric_fallback(
    request: Request,
    client: httpx.AsyncClient,
    local_cover_guid: str = "",
    on_local_payload: Callable[[dict], None] | None = None,
) -> Response:
    """透传上游；本地歌 metadata 强制 hasLyric=True，避免前端首次播放不请求歌词。

    local_cover_guid 非空时（本地曲目）同步补 data.track.coverId = 该 guid；
    歌词路由不传，行为不变。

    on_local_payload：上游 code==0 时回调原 payload（供本地缺封面时安排后台
    上传换官方 coverId）；回调异常不影响主流程。
    """
    payload_or_resp = await fetch_upstream_envelope(request, client)
    if isinstance(payload_or_resp, Response):
        return payload_or_resp
    payload = payload_or_resp
    headers = payload.pop("_ext_headers", {})
    if not isinstance(payload, dict) or payload.get("code") != 0:
        return JSONResponse(content=payload, status_code=payload_or_resp_status(payload), headers=headers or None)
    force_has_lyric_true(payload)
    if local_cover_guid:
        fill_local_metadata_cover_id(payload, local_cover_guid)
    if on_local_payload is not None:
        try:
            on_local_payload(payload)
        except Exception as e:
            logger.warning("[COVERUPLOAD] on_local_payload err=%s:%s", type(e).__name__, e)
    return JSONResponse(content=payload, status_code=payload_or_resp_status(payload), headers=headers or None)


def force_has_lyric_true(payload: dict) -> None:
    """把 metadata/lyric 上游响应里所有可能的 hasLyric 字段强制置为 true。"""
    payload["data"]["track"]["hasLyric"] = True


def fill_local_metadata_cover_id(payload: dict, guid: str) -> None:
    """本地曲目透传飞牛后补 data.track.coverId。

    本地 guid 无 online: 前缀，track_metadata 走上游分支；上游的
    data.track 不保证带 coverId，前端按 track.coverId 取封面时会拿不到，
    因此用歌曲自身 guid 兜底（本地封面链路本就按 guid 解析）。
    已有非空 coverId 时不覆盖，避免改掉上游返回的有效值。
    """
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return
    track = data.get("track")
    if not isinstance(track, dict):
        return
    if str(track.get("coverId") or "").strip():
        return
    # 优先用已上传换到的官方 coverId（前端用它请求 /static/cover 直接命中飞牛
    # 官方封面）；未换到则退回自身 guid（本地封面链路本就按 guid 解析）。
    track["coverId"] = cover_id_from_cache(guid) or guid


def _fill_cover_id(item: dict, prefix: str = "") -> bool:
    """把空 coverId 兜底为该项自身 guid（可带实体类型前缀）。返回是否写入。

    本地实体无封面/无内嵌封面标签时上游标 coverId=null（甚至直接缺键），
    前端按 coverId 取封面拿不到；用自身 guid 兜底（本地封面链路本就按
    guid 解析）。线上实体 guid 形如 online:kugou:track:<id>，同样能被封面
    路由识别，故不分线上/线下统一兜底。已有非空 coverId 时不覆盖。

    prefix：实体封面加类型前缀（artist: / album:），曲目留空。前缀让封面
    路由能区分同构的 32 位 hex guid 到底是曲目还是歌手/专辑。
    """
    guid = str(item.get("guid") or "").strip()
    if not guid:
        return False
    if item.get("coverId") in (None, ""):
        item["coverId"] = f"{prefix}{guid}"
        return True
    return False


def fill_local_track_list_cover_ids(payload: dict) -> None:
    """补 data.list[] 下空的 coverId，填为本项自身的 guid。

    track/list、play-history/list、track/genre-detail/list 共用（均为曲目
    列表，data.list[] 元素为 track）。本地曲目没有专辑封面/内嵌
    封面标签时上游标 coverId=null，前端按 coverId 取封面会拿不到；
    用歌曲自身 guid 兜底（本地封面链路本就按 guid 解析）。线上曲目
    guid 形如 online:kugou:track:<id>，同样能被封面路由识别，故不分
    线上/线下统一兜底。已有非空 coverId 时不覆盖。
    """
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return
    list_obj = data.get("list")
    if not isinstance(list_obj, list):
        return
    for item in list_obj:
        if isinstance(item, dict):
            _fill_cover_id(item)


def fill_detail_cover_id(payload: dict, kind: str = "track") -> None:
    """补 detail 接口顶层 data.coverId，填为 data.guid（按实体类型加前缀）。

    artist/detail 与 album/detail 的 data 是实体对象本身（非 list），
    无封面时上游标 coverId=null，歌手/专辑头像位空白。补法与 list 类
    接口一致：用实体自身 guid 兜底，非空不覆盖。

    kind 由调用方路由声明（artist / album / track），不再从返回体字段猜：
    歌手和专辑详情都可能带 trackCount，字段判定不可靠。kind=artist 写
    coverId=artist:<guid>，kind=album 写 album:<guid>，曲目不加前缀。
    同一 payload 的 data.list[]（如专辑详情内嵌曲目）永远是曲目，不加前缀。
    """
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return
    prefix = COVER_KIND_PREFIX.get(kind, "")
    _fill_cover_id(data, prefix=prefix)
    list_obj = data.get("list")
    if isinstance(list_obj, list):
        for item in list_obj:
            if isinstance(item, dict):
                _fill_cover_id(item)


def fill_artist_list_cover_ids(payload: dict) -> None:
    """补 /artist/list 的 data.list[] 空 coverId，填为 artist:<guid>。

    歌手列表项的实体 guid 与本地曲目 guid 都是 32 位 hex，格式同构。
    补成裸 guid 会让 /static/cover 按曲目链路解析（音频目录找封面，必空）；
    必须写 artist: 前缀，封面路由才能进 step2-entity 取歌手头像。
    已有非空 coverId 不覆盖（上游 10.7+ 自带 artist: 前缀时保持原样，
    也不会双重前缀，因为 _fill_cover_id 只在空值时写入）。
    """
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return
    list_obj = data.get("list")
    if not isinstance(list_obj, list):
        return
    for item in list_obj:
        if isinstance(item, dict):
            _fill_cover_id(item, prefix=_LOCAL_ARTIST_COVER_PREFIX)


def fill_album_list_cover_ids(payload: dict) -> None:
    """补 /album/list 的 data.list[] 空 coverId，填为 album:<guid>。

    与 fill_artist_list_cover_ids 同构：专辑实体 guid 与本地曲目 guid
    同为 32 位 hex，补成裸 guid 会被 /static/cover 按曲目链路解析。
    非空不覆盖，上游自带 album: 前缀时保持原样，不会双重前缀。
    """
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return
    list_obj = data.get("list")
    if not isinstance(list_obj, list):
        return
    for item in list_obj:
        if isinstance(item, dict):
            _fill_cover_id(item, prefix=_LOCAL_ALBUM_COVER_PREFIX)


def payload_or_resp_status(payload: dict) -> int:
    return int(payload.get("_ext_status") or 200)


def set_by_path(d: dict, path: str, val: Any):
    parts = path.split(".")
    curr = d
    for p in parts[:-1]:
        if p not in curr or not isinstance(curr[p], dict):
            curr[p] = {}
        curr = curr[p]
    curr[parts[-1]] = val


def extract_keyword(request: Request) -> str:
    """前端打包用 q，部分调用/验收用 keyword。"""
    params = request.query_params
    return (params.get("keyword") or params.get("q") or params.get("query") or "").strip()


def online_guid_from_item(item: dict) -> str:
    raw_id = str(item.get("id") or "")
    src = str(item.get("source") or "")
    if raw_id.startswith("online:"):
        return raw_id
    if ":" in raw_id:
        return f"online:{raw_id}"
    return f"online:{src}:{raw_id}"


def song_id_from_online_guid(guid: str) -> str:
    # "online:kugou:HASH" -> "HASH"（剥掉 online: 和 source: 两层前缀）
    if not guid:
        return ""
    if guid.startswith("online:"):
        rest = guid[len("online:"):]
        if ":" in rest:
            rest = rest.split(":", 1)[-1]
        return rest
    # 无前缀时也兼容 "kugou:HASH"
    if guid.startswith("kugou:"):
        return guid.split(":", 1)[-1]
    return guid


# 本地实体封面 coverId 前缀。
#
# 本地歌手/专辑的实体 guid 与本地曲目 guid 都是 32 位 hex，格式完全同构，
# 无法靠格式或字段猜实体类型（靠 trackCount 猜在歌手详情上不稳定，已验证失效）。
# 用 entity: 前缀显式标记类型：fill_detail_cover_id 由路由声明实体类型后写前缀，
# 前端按普通字符串透传 coverId，代理内部即可确定性地选择封面链路。
#   artist:<guid>  -> 本地歌手头像
#   album:<guid>   -> 本地专辑封面
#   <guid>         -> 本地曲目（保持原有裸 guid 格式，行为不变）
_LOCAL_ARTIST_COVER_PREFIX = "artist:"
_LOCAL_ALBUM_COVER_PREFIX = "album:"
_LOCAL_ENTITY_COVER_PREFIXES = (_LOCAL_ARTIST_COVER_PREFIX, _LOCAL_ALBUM_COVER_PREFIX)


def is_local_entity_cover_id(cover_id: str | None) -> bool:
    """coverId 是否为本地实体（歌手/专辑）封面。"""
    raw = str(cover_id or "")
    return any(raw.startswith(p) for p in _LOCAL_ENTITY_COVER_PREFIXES)


def is_local_artist_cover_id(cover_id: str | None) -> bool:
    """coverId 是否为本地歌手封面。"""
    return str(cover_id or "").startswith(_LOCAL_ARTIST_COVER_PREFIX)


def local_artist_guid_from_cover_id(cover_id: str) -> str:
    """从本地歌手 coverId 还原实体 guid；非该格式返回空串。"""
    raw = str(cover_id or "")
    return raw[len(_LOCAL_ARTIST_COVER_PREFIX):].strip() if is_local_artist_cover_id(raw) else ""


def is_local_album_cover_id(cover_id: str | None) -> bool:
    """coverId 是否为本地专辑封面。"""
    return str(cover_id or "").startswith(_LOCAL_ALBUM_COVER_PREFIX)


def local_album_guid_from_cover_id(cover_id: str) -> str:
    """从本地专辑 coverId 还原实体 guid；非该格式返回空串。"""
    raw = str(cover_id or "")
    return raw[len(_LOCAL_ALBUM_COVER_PREFIX):].strip() if is_local_album_cover_id(raw) else ""


COVER_KIND_PREFIX = {
    "artist": _LOCAL_ARTIST_COVER_PREFIX,
    "album": _LOCAL_ALBUM_COVER_PREFIX,
    "track": "",
}


def is_online_guid(guid: str) -> bool:
    return bool(guid) and guid.startswith("online:")


def source_from_online_guid(guid: str) -> str:
    parts = (guid or "").split(":")
    return parts[1] if len(parts) >= 3 else ""

async def fetch_kugou_artist_search(app_state, keyword: str, page: int = 1, size: int = 24) -> dict:
    """酷狗歌手搜索 -> 飞牛 /music/api/v1/search/artist。

    数据源：KuGouMusicApi /search?keywords=<keyword>&page=&pagesize=&type=author。

    字段映射（实测 q=本兮 total=10；本接口字段是**大写首字母**风格，
    和 type=album 的小写字段完全不同，不能复用 search_albums_raw）：
      AuthorId     -> guid = online:kugou:artist:<id>（拼接格式，不用 kugou_artist_guid 的 hash）
      AuthorName   -> name
      Avatar(240)  -> 不使用；coverId = guid，封面走 /static/cover 的
                     online:kugou:artist:<id> 解析链
      AudioCount   -> trackCount（歌曲数）
      AlbumCount   -> albumCount（专辑数）

    Heat/FansNum 不作为评分依据：飞牛的 score 是 0~10 的相关度分（例 9.74），
    Heat 是千万级热度、FansNum 是百万级粉丝数，直接归一会失真。
    改用「精确匹配加权 + 递减」的合成分，保证结果在 0~10 区间且单调不升。
    """
    kw = str(keyword or "").strip()
    if not kw:
        return {"code": 0, "msg": "", "data": {"list": [], "total": 0}}
    try:
        async with kugou_source._client() as c:
            r = await c.get(
                "/search",
                params={"keywords": kw, "page": page, "pagesize": size, "type": "author"},
            )
            if r.status_code != 200:
                logger.warning("[KUGOU_ARTIST_SEARCH] http=%s kw=%r body=%r",
                               r.status_code, kw, r.text[:200])
                return {"code": 0, "msg": "", "data": {"list": [], "total": 0}}
            body = r.json()
            status = body.get("status", body.get("error_code"))
            if status not in (1, 0, 200, None):
                logger.warning("[KUGOU_ARTIST_SEARCH] status=%s kw=%r errmsg=%r",
                               status, kw, body.get("error_msg"))
                return {"code": 0, "msg": "", "data": {"list": [], "total": 0}}
            data = body.get("data") or {}
            lists = data.get("lists") or [] if isinstance(data, dict) else []
            lists = [x for x in lists if isinstance(x, dict)] if isinstance(lists, list) else []
            try:
                total = int(data.get("total") or 0)
            except (TypeError, ValueError):
                total = 0
    except Exception as e:
        logger.warning("[KUGOU_ARTIST_SEARCH] error kw=%r err=%s", kw, e)
        return {"code": 0, "msg": "", "data": {"list": [], "total": 0}}

    artist_list: list[dict[str, Any]] = []
    for idx, it in enumerate(lists):
        artist_id = str(it.get("AuthorId") or it.get("author_id")
                        or it.get("AuthorID") or "").strip()
        if not artist_id or artist_id == "0":
            continue
        name = str(it.get("AuthorName") or it.get("author_name") or "").strip()
        if not name:
            continue

        ts = int(time.time())
        try:
            track_count = int(it.get("AudioCount") or 0)
        except (TypeError, ValueError):
            track_count = 0
        try:
            album_count = int(it.get("AlbumCount") or 0)
        except (TypeError, ValueError):
            album_count = 0

        # 评分：精确同名满分 10.0；其余按位置递减，保留酷狗原始排序。
        # 前缀命中与无关命中不分支：idx=21 时 8.0-21*0.05=6.95 会与更高 idx 的
        # 8.0 分支产生交错，反而打乱位置顺序，故只用一条递减公式。
        score = 10.0 if name == kw else round(10.0 - (idx + 1) * 0.05, 6)
        score = round(max(0.1, min(10.0, score)), 6)

        artist_list.append({
            # 拼接格式，与 coverId 同值。不用 kugou_artist_guid()——那个返 hash，
            # 会导致 guid 与 coverId 不一致，飞牛侧无法用 coverId 反查同一歌手。
            "guid": kugou_artist_cover_guid(artist_id),
            "name": name,
            "coverId": kugou_artist_cover_guid(artist_id),
            "createdAt": ts,
            "updatedAt": ts,
            "trackCount": track_count,
            "albumCount": album_count,
            "score": score,
        })

    logger.warning("[KUGOU_ARTIST_SEARCH] kw=%r page=%d got=%d total=%d",
                   kw, page, len(artist_list), total)
    return {
        "code": 0,
        "msg": "",
        "data": {
            "list": artist_list,
            "total": total if total > 0 else len(artist_list),
        },
    }


async def fetch_kugou_album_search(app_state, keyword: str, page: int = 1, size: int = 24) -> dict:
    """酷狗专辑搜索 -> 飞牛 /music/api/v1/search/album。

    数据源：KuGouMusicApi /search?keywords=<keyword>&page=&pagesize=&type=album。
    字段映射（实测 张杰 total=500）：
      albumid      -> guid = online:kugou:album:<id>
      albumname    -> name
      img(240 URL) -> 不使用；coverId = guid，封面走 /static/cover 的
                     online:kugou:album:<id> 解析链（/album/detail sizable_cover）
      publish_time -> releaseDate（已是 YYYY-MM-DD）+ createdAt/updatedAt
      songcount    -> trackCount
      singers[]    -> artists[]（带真实歌手 ID）

    score：酷狗搜索不回传相关度分。给一个递减值，保证按 score 降序排序时
    酷狗自身的结果顺序不被打乱（前端若依赖 score 排序不至于跳序）。

    barcode：酷狗无 ISBN，固定 null（飞牛格式允许 null）。
    """
    try:
        result = await kugou_source.search_albums_raw(keyword, limit=size, page=page)
    except Exception as e:
        logger.warning("[KUGOU_ALBUM_SEARCH] search/type=album error kw=%r err=%s", keyword, e)
        return {"code": 0, "msg": "", "data": {"list": [], "total": 0}}

    lists = result.get("lists") or []
    album_list: list[dict[str, Any]] = []
    for idx, it in enumerate(lists):
        if not isinstance(it, dict):
            continue
        album_id = str(it.get("albumid") or it.get("album_id") or "").strip()
        if not album_id:
            continue
        name = str(it.get("albumname") or it.get("album_name") or "").strip()
        if not name:
            continue

        release_date = str(it.get("publish_time") or it.get("publish_date") or "").strip() or None
        ts = parse_ts_to_unix(release_date or time.time())

        artists: list[dict[str, Any]] = []
        seen: set[str] = set()
        for sg in (it.get("singers") or it.get("Singers") or []):
            if not isinstance(sg, dict):
                continue
            aname = str(sg.get("name") or "").strip()
            if not aname or aname in seen:
                continue
            aid = str(sg.get("id") or "").strip()
            if aid in ("0", ""):
                aid = ""
            if aid:
                artists.append({
                    "guid": kugou_artist_guid(aid),
                    "name": aname,
                    "coverId": kugou_artist_cover_guid(aid),
                    "createdAt": ts,
                    "updatedAt": ts,
                })
            seen.add(aname)
        if not artists:
            singer_name = str(it.get("singer") or it.get("singername") or "").strip()
            if singer_name:
                artists = [{
                    "guid": "",
                    "name": singer_name,
                    "coverId": None,
                    "createdAt": ts,
                    "updatedAt": ts,
                }]

        try:
            track_count = int(it.get("songcount") or 0)
        except (TypeError, ValueError):
            track_count = 0

        album_list.append({
            "guid": kugou_album_guid(album_id),
            "name": name,
            "coverId": kugou_album_cover_guid(album_id),
            "releaseDate": release_date,
            "barcode": None,
            "createdAt": ts,
            "updatedAt": ts,
            "artists": artists,
            "trackCount": track_count,
            # 递减相关度分：idx=0 -> 1.0，缓慢衰减，保留酷狗原始排序。
            "score": round(1.0 / (1.0 + idx * 0.05), 6),
        })

    total = int(result.get("total") or 0)
    logger.warning("[KUGOU_ALBUM_SEARCH] kw=%r page=%d got=%d total=%d",
                   keyword, page, len(album_list), total)
    return {
        "code": 0,
        "msg": "",
        "data": {
            "list": album_list,
            "total": total if total > 0 else len(album_list),
        },
    }


async def fetch_kugou_playlist_search(app_state, keyword: str, page: int = 1, size: int = 24) -> dict:
    """酷狗专题歌单搜索 -> 飞牛 /music/api/v1/search/playlist。

    数据源：KuGouMusicApi /search?keywords=<kw>&page=&pagesize=&type=special。

    字段映射（实测 q=测试 total=480）：
      gid / suid   -> guid = online:kugou:playlist:<gid>
      specialid    -> 仅兜底 gid 为空时用
      specialname  -> name
      img(150)     -> 不使用；coverId = guid，封面走 /static/cover 的
                     online:kugou:playlist:<gid> 解析链（/playlist/detail pic）
      song_count   -> trackCount（字符串，需转 int）
      publish_time -> createdAt / updatedAt（"YYYY-MM-DD HH:MM:SS"）

    guid 必须取 gid 而不是 specialid：实测 /playlist/track/all 与 /playlist/detail
    传 specialid 均报 20010 "get other list file fail" 或返回空 data，只有传 gid
    （形如 collection_3_408871768_26_0，含 userid 与 listid）才能拿到歌曲与封面。
    gid 本身是下划线连接的安全字符串，直接当 guid 用不必再 sanitize。

    score：与 search/album 同一套递减公式，保证按 score 降序排序时不打乱酷狗原始顺序。
    """
    kw = str(keyword or "").strip()
    if not kw:
        return {"code": 0, "msg": "", "data": {"list": [], "total": 0}}
    try:
        async with kugou_source._client() as c:
            r = await c.get(
                "/search",
                params={"keywords": kw, "page": page, "pagesize": size, "type": "special"},
            )
            if r.status_code != 200:
                logger.warning("[KUGOU_PLAYLIST_SEARCH] http=%s kw=%r body=%r",
                               r.status_code, kw, r.text[:200])
                return {"code": 0, "msg": "", "data": {"list": [], "total": 0}}
            body = r.json()
            status = body.get("status", body.get("error_code"))
            if status not in (1, 0, 200, None):
                logger.warning("[KUGOU_PLAYLIST_SEARCH] status=%s kw=%r errmsg=%r",
                               status, kw, body.get("error_msg"))
                return {"code": 0, "msg": "", "data": {"list": [], "total": 0}}
            data = body.get("data") or {}
            lists = data.get("lists") or [] if isinstance(data, dict) else []
            lists = [x for x in lists if isinstance(x, dict)] if isinstance(lists, list) else []
            try:
                total = int(data.get("total") or 0)
            except (TypeError, ValueError):
                total = 0
    except Exception as e:
        logger.warning("[KUGOU_PLAYLIST_SEARCH] error kw=%r err=%s", kw, e)
        return {"code": 0, "msg": "", "data": {"list": [], "total": 0}}

    playlist_list: list[dict[str, Any]] = []
    for idx, it in enumerate(lists):
        gid = str(it.get("gid") or "").strip()
        specialid = str(it.get("specialid") or it.get("special_id") or "").strip()
        suid = str(it.get("suid") or "").strip()
        if not gid and not specialid and not suid:
            continue
        name = str(it.get("specialname") or it.get("name") or "").strip()
        if not name:
            continue

        # 优先 gid；缺失时回退 specialid/suid（旧格式，track/detail 会拿不到数据但 guid 仍可用）
        key = gid or specialid or suid
        guid = kugou_playlist_guid(key)

        release_ts = parse_ts_to_unix(it.get("publish_time") or it.get("create_time"))
        try:
            track_count = int(it.get("song_count") or 0)
        except (TypeError, ValueError):
            track_count = 0

        playlist_list.append({
            "guid": guid,
            "name": name,
            "coverId": guid,
            "createdAt": release_ts,
            "updatedAt": release_ts,
            "trackCount": track_count,
            "score": round(1.0 / (1.0 + idx * 0.05), 6),
        })

    logger.warning("[KUGOU_PLAYLIST_SEARCH] kw=%r page=%d got=%d total=%d",
                   kw, page, len(playlist_list), total)
    return {
        "code": 0,
        "msg": "",
        "data": {
            "list": playlist_list,
            "total": total if total > 0 else len(playlist_list),
        },
    }


async def fetch_kugou_album_detail(app_state, album_guid: str) -> dict | None:
    """酷狗专辑详情 -> 飞牛 /music/api/v1/album/detail。

    两步串行（keywords 依赖 detail 结果，无法并行）：
      1. /album/detail?id=<album_id> → album_name + author_name（keywords 必需，
         不能靠外部传入）+ sizable_cover + publish_date + type + language。
      2. /search?keywords=<歌手名+专辑名>&type=album → 按 albumid 精确匹配拿
         singers[{name,id}]（带真实歌手 ID）、songcount、publish_time、intro、company。

    只搜专辑名不行：keywords=小心思 → total=500 且首位是别的同名专辑。
    非 online:kugou:album: GUID 返回 None，由调用方转飞牛上游。
    """
    s = str(album_guid or "").strip()
    if not s.startswith("online:kugou:album:"):
        return None
    album_id = s[len("online:kugou:album:"):].strip()
    if not album_id:
        return None

    try:
        bundle = await kugou_source.get_album_detail_bundle(album_id)
    except Exception as e:
        logger.warning("[KUGOU_ALBUM_DETAIL] bundle error album_id=%s err=%s", album_id, e)
        return None

    detail = bundle.get("detail") or {}
    match = bundle.get("match") or {}
    logger.warning("[KUGOU_ALBUM_DETAIL] album_id=%s detail=%s match=%s",
                   album_id, bool(detail), bool(match))
    if not detail and not match:
        return None

    # 字段合并：search 命中优先（singers 带真实 ID、intro/company 更完整），
    # 落空时用 detail 字段。
    name = str(match.get("albumname") or detail.get("album_name")
               or detail.get("albumName") or detail.get("album") or "").strip()
    release_date = str(match.get("publish_time") or detail.get("publish_date")
                       or detail.get("publishDate") or "").strip() or None
    ts = parse_ts_to_unix(release_date or time.time())

    # 歌手：search 的 singers[] 直接带 name+id；没命中时回退 detail.authors[]。
    artists: list[dict[str, Any]] = []
    seen: set[str] = set()
    for sg in match.get("singers") or []:
        if not isinstance(sg, dict):
            continue
        aname = str(sg.get("name") or "").strip()
        if not aname or aname in seen:
            continue
        aid = str(sg.get("id") or "").strip()
        if aid in ("0", ""):
            aid = ""
        artists.append({
            "guid": f"online:kugou:artist:{aid}" if aid else kugou_artist_guid(album_id),
            "name": aname,
            "coverId": kugou_artist_cover_guid(aid) if aid else kugou_artist_cover_guid(album_id),
            "createdAt": ts,
            "updatedAt": ts,
        })
        seen.add(aname)
    if not artists:
        for au in detail.get("authors") or []:
            if not isinstance(au, dict):
                continue
            aname = str(au.get("author_name") or au.get("authorName") or au.get("name") or "").strip()
            if not aname or aname in seen:
                continue
            aid = str(au.get("author_id") or au.get("authorId") or "").strip()
            if aid in ("0", ""):
                aid = ""
            artists.append({
                "guid": f"online:kugou:artist:{aid}" if aid else kugou_artist_guid(album_id),
                "name": aname,
                "coverId": kugou_artist_cover_guid(aid) if aid else kugou_artist_cover_guid(album_id),
                "createdAt": ts,
                "updatedAt": ts,
            })
            seen.add(aname)

    # songcount 取 search 命中值；缺失时用 0 而不是编造数字。
    try:
        track_count = int(match.get("songcount") or 0)
    except (TypeError, ValueError):
        track_count = 0

    intro = str(match.get("intro") or match.get("short_intro")
                or detail.get("intro") or "").strip() or None
    company = str(match.get("company") or detail.get("publish_company") or "").strip() or None
    language = str(match.get("language") or detail.get("language") or "").strip() or None
    album_type = str(detail.get("type") or detail.get("album_type") or "").strip() or None

    payload = {
        "code": 0,
        "msg": "",
        "data": {
            "guid": kugou_album_guid(album_id),
            "name": name,
            "coverId": kugou_album_cover_guid(album_id),
            "releaseDate": release_date,
            "barcode": None,
            "createdAt": ts,
            "updatedAt": ts,
            "artists": artists,
            "trackCount": track_count,
            "language": language,
            "albumType": album_type,
            "publishCompany": company,
            "intro": intro,
        },
    }
    return payload


async def fetch_kugou_album_tracks(app_state, album_guid: str, page: int = 1, size: int = 50) -> dict:
    """酷狗专辑歌曲列表 -> 飞牛 /track/album-detail/list。

    数据源：KuGouMusicApi /album/songs?id=<album_id>&page=&pagesize=。
    返回结构对齐 fetch_kugou_artist_tracks：{"items":[raw...], "total":n,...}。
    非 online:kugou:album: GUID 返回 None，由调用方转飞牛上游。
    """
    s = str(album_guid or "").strip()
    if not s.startswith("online:kugou:album:"):
        return None
    album_id = s[len("online:kugou:album:"):].strip()
    if not album_id:
        return None
    try:
        result = await kugou_source.get_album_songs(album_id, page=page, pagesize=size)
    except Exception as e:
        logger.warning("[KUGOU_ALBUM_TRACKS] album/songs error album=%s err=%s", album_id, e)
        return {"items": [], "total": 0, "page": page, "pagesize": size, "album_id": album_id}
    return result


def _safe_int_or_none(v: Any) -> int | None:
    """把 discNo/trackNo 之类的可选序号字段转成 int 或 None。

    0/空值/非数字都视为无值（飞牛对 discNo/trackNo 接受 null）。
    """
    if v is None or v == "":
        return None
    try:
        n = int(v)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def build_online_track(item: dict) -> dict:
    """对齐飞牛音乐列表标准格式：只返回飞牛标准字段。"""
    guid = online_guid_from_item(item)
    src = str(item.get("source") or source_from_online_guid(guid) or "")
    title = str(item.get("title") or item.get("name") or "")
    artist = str(item.get("artist") or "")
    album = str(item.get("album") or "")

    duration_s = item.get("duration_s") or 0
    try:
        duration_s = float(duration_s)
    except (TypeError, ValueError):
        duration_s = 0
    duration_ms = int(duration_s * 1000)

    ext = str(item.get("ext") or "mp3") or "mp3"
    play_format = play_format_from_ext(ext)

    file_size = item.get("file_size") or 0
    try:
        file_size = int(file_size or 0)
    except (TypeError, ValueError):
        file_size = 0

    bitrate = item.get("bitrate") or 0
    try:
        bitrate = int(bitrate or 0)
    except (TypeError, ValueError):
        bitrate = 0
    if not bitrate:
        bitrate = 1411000 if play_format in ("flac", "wav", "ape", "wv") else 320000
    elif bitrate < 1000:
        bitrate *= 1000

    spec_path = f"online/{src}/{guid}.{play_format}"
    album_guid = f"{guid}:album"
    created_at = int(item.get("createdAt") or time.time())
    updated_at = int(item.get("updatedAt") or created_at)
    album_id = item.get("album_id") or ""
    release_date = str(item.get("release_date") or "").strip() or None
    year: int | None = None
    if release_date:
        try:
            y = int(str(release_date)[:4])
            if 1900 <= y <= 2100:
                year = y
        except (TypeError, ValueError):
            year = None

    raw_artists = item.get("artists")
    artists_list: list[dict] = []
    if isinstance(raw_artists, list):
        for idx, raw_artist in enumerate(raw_artists):
            if not isinstance(raw_artist, dict):
                continue
            raw_name = str(raw_artist.get("name") or "").strip()
            if not raw_name:
                continue
            raw_id = str(raw_artist.get("id") or "").strip()
            artists_list.append({
                "guid": f"online:kugou:artist:{raw_id}" if raw_id else f"{guid}:artist:{idx + 1}",
                "name": raw_name,
                "coverId": f"online:kugou:artist:{raw_id}" if raw_id else f"{guid}:artist:{idx + 1}",
                "createdAt": created_at,
                "updatedAt": updated_at,
            })
    elif artist:
        artists_list = [
            {
                "guid": f"{guid}:artist:1",
                "name": artist,
                "coverId": None,
                "createdAt": created_at,
                "updatedAt": updated_at,
            }
        ]

    # KuGouMusicApi 没有 album 详情接口，且 /static/cover 无法解析 kugou:album:<id>。
    # 酷狗单曲 cover 本身就是专辑封面（album/v8/<albumid>_{size}.jpg），
    # 复用曲目 guid 走 /static/cover 的 online:kugou:<hash> 解析链路。
    album_obj = {
        "guid": f"online:kugou:album:{album_id}" if album_id not in ("", None) else album_guid,
        "name": album,
        "coverId": guid,
        "releaseDate": release_date,
        "barcode": None,
        "createdAt": created_at,
        "updatedAt": updated_at,
    }

    audio_spec = {
        "bitDepth": 16,
        "sampleRate": 44100,
        "channel": 2,
        "bitrate": bitrate,
        "codec": "酷狗源" if src == "kugou" else play_format,
        "container": "",
        "duration": duration_ms,
        "format": "酷狗源" if src == "kugou" else play_format,
        "path": spec_path,
        "size": file_size,
    }

    return {
        "guid": guid,
        "title": title,
        "coverId": guid,
        "year": year,
        "discNo": _safe_int_or_none(item.get("disc_no") or item.get("discNo")),
        "trackNo": _safe_int_or_none(item.get("track_no") or item.get("trackNo")),
        "isrc": None,
        "duration": duration_ms,
        "isCue": False,
        "createdAt": created_at,
        "updatedAt": updated_at,
        "album": album_obj,
        "artists": artists_list,
        "genres": [],
        "audioSpec": audio_spec,
        "isFavorite": False,
        "accessStatus": 0,
    }


def artist_from_track(item: dict) -> str:
    if not isinstance(item, dict):
        return ""
    a = item.get("artist") or item.get("singer") or item.get("singers") or ""
    if isinstance(a, list):
        names = []
        for x in a:
            if isinstance(x, dict):
                names.append(str(x.get("name") or ""))
            else:
                names.append(str(x))
        return " ".join(n for n in names if n).strip().lower()
    if isinstance(a, dict):
        return str(a.get("name") or "").strip().lower()
    return str(a).strip().lower()


def title_from_track(item: dict) -> str:
    if not isinstance(item, dict):
        return ""
    return str(item.get("title") or item.get("name") or "").strip().lower()


def should_cache(range_header: str | None) -> bool:
    """完整拉取才落盘：无 Range，或 bytes=0-（开区间）。Safari bytes=0-1 探测不落盘。"""
    if not range_header:
        return True
    r = range_header.strip().lower()
    return bool(re.match(r"^bytes=0-$", r))


def cache_safe_guid(guid: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]", "_", guid)


def online_file_id(guid: str) -> str:
    """online:migu:600929… → 600929…，仅用于查找旧文件，不再写进文件名。"""
    return song_id_from_online_guid(guid).rsplit(":", 1)[-1]


def safe_basename_title(title: str) -> str:
    t = re.sub(r'[/\\:\0]', "_", (title or "").strip()) or "unknown"
    t = re.sub(r"\s+", " ", t).strip(" .")
    return t[:120]


def library_basename(title: str, artist: str = "") -> str:
    """曲库文件名：歌手 - 歌名（无源站 id）。飞牛无标签时会用文件名当标题。"""
    title_s = safe_basename_title(title)
    artist_s = safe_basename_title(artist) if (artist or "").strip() else ""
    if artist_s and artist_s.lower() != title_s.lower() and artist_s != "unknown":
        return f"{artist_s} - {title_s}"
    return title_s


def media_ref_path(guid: str) -> str:
    return os.path.join(CONF["cache_dir"], f"{cache_safe_guid(guid)}.ref")


def _path_stem(path: str) -> str:
    root, ext = os.path.splitext(path)
    known = set(CACHE_EXTS) | {"lrc", "part"}
    if ext.lstrip(".").lower() in known:
        return root
    return path


def remember_media_path(guid: str, media_path: str) -> None:
    """记住曲库里的文件词干（不含扩展名），音频和 .lrc 共用。"""
    try:
        os.makedirs(CONF["cache_dir"], exist_ok=True)
        with open(media_ref_path(guid), "w", encoding="utf-8") as f:
            f.write(_path_stem(media_path))
    except Exception as e:
        logger.warning("Failed to remember media path for %s: %s", guid, e)


def recalled_media_stem(guid: str) -> str | None:
    ref = media_ref_path(guid)
    if not os.path.exists(ref):
        return None
    try:
        with open(ref, encoding="utf-8") as f:
            stem = _path_stem(f.read().strip())
        if stem:
            return stem
    except Exception:
        return None
    return None


def recalled_media_path(guid: str) -> str | None:
    stem = recalled_media_stem(guid)
    if not stem:
        return None
    for ext in CACHE_EXTS:
        path = f"{stem}.{ext}"
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return path
    return None


def unique_library_path(directory: str, basename: str, ext: str) -> str:
    dest = os.path.join(directory, f"{basename}.{ext}")
    if not os.path.exists(dest):
        return dest
    n = 2
    while os.path.exists(os.path.join(directory, f"{basename} ({n}).{ext}")):
        n += 1
    return os.path.join(directory, f"{basename} ({n}).{ext}")


def write_audio_tags(path: str, title: str, artist: str = "", album: str = "") -> None:
    """写入 title/artist/album，飞牛扫描后用标签而不是文件名显示。"""
    title, artist, album = (title or "").strip(), (artist or "").strip(), (album or "").strip()
    if not title and not artist:
        return
    try:
        from mutagen import File as MutagenFile

        audio = MutagenFile(path, easy=True)
        if audio is None:
            return
        if getattr(audio, "tags", None) is None:
            try:
                audio.add_tags()
            except Exception:
                pass
        if title:
            audio["title"] = title
        if artist:
            audio["artist"] = artist
        if album:
            audio["album"] = album
        audio.save()
    except Exception as e:
        logger.warning("Failed to write audio tags for %s: %s", path, e)


def detect_library_dir() -> str:
    """优先环境变量，否则读飞牛 music.db 的共享库路径，最后回退到仓库 cache/。"""
    explicit = str(CONF.get("library_dir") or "").strip()
    if explicit:
        return explicit
    db = str(CONF.get("music_db") or "")
    if db and os.path.exists(db):
        try:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            try:
                rows = con.execute("SELECT path FROM shared_library ORDER BY id").fetchall()
            finally:
                con.close()
            for (path,) in rows:
                if path and os.path.isdir(path):
                    return path
        except Exception as e:
            logger.warning("Failed to read shared_library path: %s", e)
    return CONF["cache_dir"]


def iter_media_dirs() -> list[str]:
    dirs: list[str] = []
    lib = detect_library_dir()
    for d in (lib, CONF["cache_dir"]):
        if d and d not in dirs:
            dirs.append(d)
    return dirs


def adopt_library_perms(path: str) -> None:
    try:
        parent = os.path.dirname(path) or "."
        st = os.stat(parent)
        os.chown(path, st.st_uid, st.st_gid)
        os.chmod(path, 0o644)
    except Exception:
        pass


def find_cache_file(guid: str) -> str | None:
    recalled = recalled_media_path(guid)
    if recalled:
        return recalled
    file_id = online_file_id(guid)
    safe = cache_safe_guid(guid)
    for d in iter_media_dirs():
        if not os.path.isdir(d):
            continue
        for ext in CACHE_EXTS:
            exact = os.path.join(d, f"{safe}.{ext}")
            if os.path.exists(exact) and os.path.getsize(exact) > 0:
                return exact
            pattern = os.path.join(d, f"* - {glob.escape(file_id)}.{ext}")
            for path in glob.glob(pattern):
                if os.path.getsize(path) > 0:
                    return path
    return None


def promote_cache_hit(guid: str, audio_path: str) -> str:
    """旧 cache/ 音频：若曲库已有对应文件或歌词，则对齐过去。"""
    recalled = recalled_media_path(guid)
    if recalled:
        return recalled
    lib = detect_library_dir()
    try:
        if os.path.abspath(os.path.dirname(audio_path)) == os.path.abspath(lib):
            remember_media_path(guid, audio_path)
            return audio_path
    except Exception:
        return audio_path
    file_id = online_file_id(guid)
    ext = os.path.splitext(audio_path)[1] or ".mp3"
    dest = None
    if os.path.isdir(lib):
        for lrc in glob.glob(os.path.join(lib, f"* - {glob.escape(file_id)}.lrc")):
            dest = os.path.splitext(lrc)[0] + ext
            break
    if not dest:
        return audio_path
    if not os.path.exists(dest):
        try:
            os.makedirs(lib, exist_ok=True)
            shutil.copy2(audio_path, dest)
            adopt_library_perms(dest)
        except Exception as e:
            logger.warning("Failed to promote cache audio into library: %s", e)
            return audio_path
    remember_media_path(guid, dest)
    return dest


def library_media_path(guid: str, title: str, ext: str, artist: str = "") -> str:
    recalled = recalled_media_path(guid)
    if recalled:
        return recalled
    stem = recalled_media_stem(guid)
    if stem:
        return f"{stem}.{ext}"
    lib = detect_library_dir()
    file_id = online_file_id(guid)
    if os.path.isdir(lib):
        for path in glob.glob(os.path.join(lib, f"* - {glob.escape(file_id)}.{ext}")):
            if os.path.getsize(path) > 0:
                return path
    os.makedirs(lib, exist_ok=True)
    return unique_library_path(lib, library_basename(title, artist), ext)


def find_lyric_file(guid: str) -> str | None:
    stem = recalled_media_stem(guid)
    if stem:
        sibling = f"{stem}.lrc"
        if os.path.exists(sibling) and os.path.getsize(sibling) > 0:
            return sibling
    audio = find_cache_file(guid)
    if audio:
        sibling = os.path.splitext(audio)[0] + ".lrc"
        if os.path.exists(sibling) and os.path.getsize(sibling) > 0:
            return sibling
    file_id = online_file_id(guid)
    safe = cache_safe_guid(guid)
    for d in iter_media_dirs():
        if not os.path.isdir(d):
            continue
        exact = os.path.join(d, f"{safe}.lrc")
        if os.path.exists(exact) and os.path.getsize(exact) > 0:
            return exact
        pattern = os.path.join(d, f"* - {glob.escape(file_id)}.lrc")
        for path in glob.glob(pattern):
            if os.path.getsize(path) > 0:
                return path
    return None


def lyric_cache_path(guid: str, title: str = "", artist: str = "") -> str:
    found = find_lyric_file(guid)
    if found:
        return found
    audio = find_cache_file(guid)
    if audio:
        return os.path.splitext(audio)[0] + ".lrc"
    d = detect_library_dir()
    os.makedirs(d, exist_ok=True)
    if (title or "").strip() or (artist or "").strip():
        return os.path.join(d, f"{library_basename(title, artist)}.lrc")
    return os.path.join(d, f"{cache_safe_guid(guid)}.lrc")


def read_lyric_cache(guid: str) -> str:
    """歌词纯内存缓存；不再读取或要求磁盘 .lrc。"""
    return get_stream_lyric(guid)


def write_lyric_cache(guid: str, text: str, title: str = "", artist: str = "") -> None:
    """歌词纯内存缓存；不落盘，避免写 .lrc/.ref/.part。"""
    text = (text or "").strip()
    if not text:
        return
    if text == read_lyric_cache(guid):
        return
    remember_stream_lyric(guid, text)


async def resolve_online_lyric(request: Request, guid: str) -> str:
    """本地 .lrc 优先；没有再向源站要，拿到就落盘。"""
    cached = read_lyric_cache(guid)
    if cached:
        return cached

    src = source_from_online_guid(guid)
    if src == "kugou":
        raw_song_id = song_id_from_online_guid(guid)
        text = await resolve_kugou_lyric(raw_song_id)
        if text:
            write_lyric_cache(guid, text)
        return text

    data = await _online_info(request, guid)
    text = str((data or {}).get("lyric") or "").strip()
    if text:
        write_lyric_cache(
            guid,
            text,
            title=str((data or {}).get("title") or ""),
            artist=str((data or {}).get("artist") or ""),
        )
    return text


def media_type_for_ext(ext: str) -> str:
    return {
        "mp3": "audio/mpeg",
        "flac": "audio/flac",
        "wav": "audio/wav",
        "ogg": "audio/ogg",
        "opus": "audio/ogg",
        "m4a": "audio/mp4",
        "aac": "audio/aac",
        "ape": "audio/x-ape",
        "wv": "audio/x-wavpack",
        "dsf": "audio/x-dsd",
        "dff": "audio/x-dff",
        "tta": "audio/x-tta",
        "wma": "audio/x-ms-wma",
        "aiff": "audio/aiff",
    }.get(ext.lower(), "application/octet-stream")


def ext_from_content_type(content_type: str) -> str:
    ct = (content_type or "").lower()
    if "flac" in ct:
        return "flac"
    if "wavpack" in ct or "x-wv" in ct:
        return "wv"
    if "wav" in ct or "wave" in ct:
        return "wav"
    if "opus" in ct:
        return "opus"
    if "ogg" in ct:
        return "ogg"
    if "ape" in ct:
        return "ape"
    if "aiff" in ct:
        return "aiff"
    if "mp4" in ct or "m4a" in ct:
        return "m4a"
    if "aac" in ct:
        return "aac"
    if "mpeg" in ct or "mp3" in ct:
        return "mp3"
    return play_format_from_ext(ct.split("/")[-1] if "/" in ct else "mp3")


def parse_http_range(range_header: str | None, file_size: int) -> tuple[int, int] | None:
    if not range_header:
        return None
    m = re.match(r"bytes=(\d*)-(\d*)", range_header.strip(), re.I)
    if not m:
        return None
    start_s, end_s = m.group(1), m.group(2)
    if start_s == "" and end_s == "":
        return None
    if start_s == "":
        suffix = int(end_s)
        start = max(file_size - suffix, 0)
        end = file_size - 1
    else:
        start = int(start_s)
        end = int(end_s) if end_s else file_size - 1
    end = min(end, file_size - 1)
    if start < 0 or start >= file_size or start > end:
        return None
    return start, end


def serve_file_with_range(path: str, range_header: str | None, media_type: str) -> Response:
    file_size = os.path.getsize(path)
    rng = parse_http_range(range_header, file_size)

    def iter_file(offset: int, length: int) -> AsyncGenerator[bytes, None]:
        async def gen() -> AsyncGenerator[bytes, None]:
            remaining = length
            with open(path, "rb") as fp:
                fp.seek(offset)
                while remaining > 0:
                    chunk = fp.read(min(64 * 1024, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk

        return gen()

    if rng is None:
        return StreamingResponse(
            iter_file(0, file_size),
            status_code=200,
            headers={
                "Content-Type": media_type,
                "Content-Length": str(file_size),
                "Accept-Ranges": "bytes",
            },
        )

    start, end = rng
    length = end - start + 1
    return StreamingResponse(
        iter_file(start, length),
        status_code=206,
        headers={
            "Content-Type": media_type,
            "Content-Length": str(length),
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
        },
    )


def serve_bytes_with_range(body: bytes, range_header: str | None, media_type: str) -> Response:
    """纯内存响应：不落盘，仅在当前进程内缓存的音频上支持 Range。"""
    file_size = len(body)
    rng = parse_http_range(range_header, file_size)

    def iter_bytes(offset: int, length: int) -> AsyncGenerator[bytes, None]:
        async def gen() -> AsyncGenerator[bytes, None]:
            pos = offset
            end = offset + length
            while pos < end:
                yield body[pos:min(pos + 64 * 1024, end)]
                pos += 64 * 1024

        return gen()

    if rng is None:
        return StreamingResponse(
            iter_bytes(0, file_size),
            status_code=200,
            headers={
                "Content-Type": media_type,
                "Content-Length": str(file_size),
                "Accept-Ranges": "bytes",
            },
        )

    start, end = rng
    length = end - start + 1
    return StreamingResponse(
        iter_bytes(start, length),
        status_code=206,
        headers={
            "Content-Type": media_type,
            "Content-Length": str(length),
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
        },
    )


def get_upstream_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "upstream_client", None)
    if client is None:
        transport = httpx.AsyncHTTPTransport(uds=CONF["upstream_sock"])
        client = httpx.AsyncClient(transport=transport, base_url="http://unix", timeout=30.0)
        fastapi_app.state.upstream_client = client
    return client


def _is_get_request(request: Request) -> bool:
    """代理只接管 GET 请求，其他方法一律透传上游。

    音乐端（/music/api/v1）所有被接管路由统一走此闸门：POST/PUT/DELETE/
    PATCH/HEAD/OPTIONS 等不进入本代理的处理逻辑，直接原样转上游，避免
    代理改动非 GET 请求语义、请求体或返回体。
    """
    return request.method.upper() == "GET"


async def forward_to_upstream(request: Request, client: httpx.AsyncClient) -> Response:
    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"

    headers = copy_incoming_headers(request)
    body = await request.body()

    req = client.build_request(
        method=request.method,
        url=url_path,
        headers=headers,
        content=body if body else None,
    )
    resp = await client.send(req, stream=True)
    resp_headers = filter_headers(resp.headers, exclude_keys={"content-length", "content-encoding"})

    async def body_stream() -> AsyncGenerator[bytes, None]:
        try:
            async for chunk in resp.aiter_bytes():
                yield chunk
        finally:
            await resp.aclose()

    return StreamingResponse(
        body_stream(),
        status_code=resp.status_code,
        headers=resp_headers,
    )


async def fetch_upstream_envelope(request: Request, client: httpx.AsyncClient) -> Response | dict:
    """透传上游并解析 JSON 信封。失败时返回 Response，成功返回 dict。"""
    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"
    headers = copy_incoming_headers(request)
    body = await request.body()
    req = client.build_request(
        method=request.method,
        url=url_path,
        headers=headers,
        content=body if body else None,
    )
    resp = await client.send(req)
    resp_headers = filter_headers(resp.headers, exclude_keys={"content-length", "content-encoding"})
    if resp.status_code != 200:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    try:
        payload = resp.json()
    except Exception:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    if not isinstance(payload, dict):
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    payload["_ext_headers"] = resp_headers
    payload["_ext_status"] = resp.status_code
    return payload


async def fetch_upstream_envelope_at(
    request: Request, client: httpx.AsyncClient, path: str,
    params: dict | None = None,
) -> Response | dict:
    """按指定路径请求飞牛上游并解析 JSON 信封。

    fetch_upstream_envelope 透传的是 request.url.path（当前请求路径），
    在 /static/cover 处理过程中调用它去取歌手/专辑名字，实际请求的还是
    /static/cover，上游返回图片流导致 JSON 解析失败、名字取不到。
    本函数固定请求 path（如 /music/api/v1/artist/detail?guid=<g>），
    鉴权头仍透传当前请求（上游靠 token 判断调用方身份）。
    """
    headers = copy_incoming_headers(request)
    req = client.build_request(
        method="GET",
        url=path,
        params=params,
        headers=headers,
    )
    resp = await client.send(req)
    resp_headers = filter_headers(resp.headers, exclude_keys={"content-length", "content-encoding"})
    if resp.status_code != 200:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    try:
        payload = resp.json()
    except Exception:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    if not isinstance(payload, dict):
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    payload["_ext_headers"] = resp_headers
    payload["_ext_status"] = resp.status_code
    return payload


def ensure_search_list(upstream_json: dict) -> list:
    """保证 data.list 存在，本地 0 条时仍能追加在线条目。"""
    data = upstream_json.get("data")
    if not isinstance(data, dict):
        data = {}
        upstream_json["data"] = data
    target = get_by_path(upstream_json, CONF["search_list_path"])
    if isinstance(target, list):
        return target
    for key in ("list", "items", "tracks", "records"):
        if isinstance(data.get(key), list):
            if key != "list":
                data["list"] = data[key]
            return data["list"]
    data["list"] = []
    if "total" not in data:
        data["total"] = 0
    return data["list"]


def _search_data_root(upstream_json: dict) -> dict:
    parts = CONF["search_list_path"].split(".")
    parent = upstream_json
    for p in parts[:-1]:
        if isinstance(parent, dict) and p in parent:
            parent = parent[p]
    if isinstance(parent, dict):
        return parent
    data = upstream_json.get("data")
    if isinstance(data, dict):
        return data
    return {}


def _read_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _kugou_page_params(cap: int, step: int) -> list[tuple[int, int, int]]:
    """生成酷狗分页序列，末页用「剩余条数」当 size，避免越界。

    酷狗 /search 有结果条数上限 cap（歌曲/歌单 480，专辑/歌手 500），且末页
    必须满足 from + size <= cap，否则返回 149 "Out Page Range"（HTTP 502，
    body 里 error_code=149 / from / size）。

    关键：酷狗的 from 是按**本次请求的 size** 算的，不是固定步长：
        from = (page - 1) * size
    所以末页的 size 变小了，页号也必须跟着重算：page = offset // size + 1，
    否则 from 会偏到错误区间（例：cap=480 step=50 时末页若取 (10,30)，
    from=(10-1)*30=270，实际取的是 270~300，不是想要的 450~480）。

    步长 step 固定；前若干页各取 step 条，最后一页用 remainder = cap -
    offset 当 size，页号取 offset // remainder + 1，使 from + size 恰好等于
    cap，不越界。数据覆盖无缝：前段 0~offset，末页 offset~cap。

    例：cap=480, step=50 -> (1,50)...(9,50) 后接 (16,30)：from=450, 450+30=480。
        cap=500, step=50 -> 正好 (1,50)...(10,50)，450+50=500 同样不越界。
        cap=99,  step=50 -> (1,50) 后接 (3,25)(6,12)(37,2)：remainder 不整除
        offset 时逐步降级 size，保证 from 始终精确落在 offset，不丢不重。

    返回 [(page, size, page_total), ...]，page_total = from + size。
    """
    step = max(1, int(step))
    out: list[tuple[int, int, int]] = []
    offset = 0
    while offset < cap:
        s = min(step, cap - offset)
        if offset > 0:
            # from = (page-1)*size，要精确落在 offset 必须 size 整除 offset；
            # 否则页号算出来 from 会错位（如 offset=50 时 size=49 -> from=49）。
            while offset % s != 0:
                s -= 1
        page = offset // s + 1
        out.append((page, s, offset + s))
        offset += s
    return out


def _kugou_item_key(item: dict) -> tuple[str, str]:
    """酷狗 item 的跨页去重键：id/guid 为主，标题+歌手兜底。"""
    raw_id = item.get("id") or item.get("guid") or ""
    id_str = str(raw_id).strip()
    title = str(item.get("title") or item.get("name") or "").strip().lower()
    artist = str(item.get("artist") or item.get("singer") or "").strip().lower()
    return (id_str, title, artist)


def merge_online_tracks(
    upstream_json: dict,
    online_data: dict | list[dict] | None,
    page: int = 1,
    size: int | None = 50,
) -> dict:
    target_list = ensure_search_list(upstream_json)
    for item in target_list:
        if isinstance(item, dict) and not is_online_guid(str(item.get("guid") or "")):
            if item.get("coverId") in (None, ""):
                item["coverId"] = str(item.get("guid") or "")
    if isinstance(online_data, dict):
        online_result = online_data
        raw_items = online_data.get("items", [])
    elif isinstance(online_data, list):
        online_result = None
        raw_items = online_data
    else:
        online_result = None
        raw_items = []

    parent = _search_data_root(upstream_json)
    official_total = _read_int(parent.get("total"), len(target_list))

    if not raw_items:
        logger.warning(
            "[SEARCH_MERGE] official_count=%d official_total=%d online_items=%d online_total=%d page=%d size=%d",
            len(target_list), official_total, 0, 0, page, size,
        )
        return upstream_json

    existing_keys = set()
    for item in target_list:
        t = title_from_track(item)
        a = artist_from_track(item)
        if t and a:
            existing_keys.add((t, a))

    filtered_online = []
    for online_item in raw_items:
        ot = str(online_item.get("title") or online_item.get("name") or "").strip().lower()
        oa = str(online_item.get("artist") or "").strip().lower()
        if ot and oa and (ot, oa) in existing_keys:
            continue
        filtered_online.append(online_item)


    for it in filtered_online:
        target_list.append(build_online_track(it))

    # total 恒等于实际返回条数，绝不使用两侧的「声明总数」。
    #
    # 旧逻辑 total = official_total + online_total 用的是两边的声明总数，与
    # 列表实际条数无关：本兮 官方声明 22 + 酷狗声明 480 = 502，而列表实际
    # 只有 316 项，客户端按 502 算出十页，翻到第三页就空——表现为「没有
    # 返回所有结果」。酷狗的 total 又是结果上限值而非实际计数（周杰伦只有
    # 99 首同样报 480），更不能拿它做 total。
    online_total = len(filtered_online)

    # PC 端按 page/size 切片；手机端不传 page/size 时 page=1/size=50 由路由
    # 层改写为全量（见 search_track），故此处切片是安全的。
    # 切片前记录 total 作为「全量条数」，切片后写回 parent，前端据此算页数。
    total = len(target_list)
    # 官方实际合并条数：在切片前就算好，切片后列表变短不能再用 len 倒推。
    official_items = total - len(filtered_online)
    if size is not None and total > 0:
        # 整表换成「本页那一段」。必须用全量快照切片后再赋回：
        # target_list 是原列表的引用，就地删改会把 data.list 本身弄坏。
        # 不能用 [start:] = [] 这种「删到末尾」写法——page=1 时 start=0，
        # 会把整张表清空（实测 page=1 returned=0、page=2 只剩 50 条）。
        full = list(target_list)
        start = max(0, (page - 1) * size)
        del target_list[:]
        target_list.extend(full[start:start + size])
        parent["total"] = total
    elif total > 0:
        parent["total"] = total
    logger.warning(
        "[SEARCH_MERGE] official_items=%d official_declared=%d online_items=%d online_declared=%d total=%d page=%d size=%d returned=%d",
        official_items, official_total, len(filtered_online),
        _read_int((online_result or {}).get("total"), 0), total, page, size,
        len(target_list),
    )

    return upstream_json


def merge_search_meta(
    upstream_json: dict,
    kugou_payload: dict | None,
    tag: str = "meta",
) -> dict:
    """歌手 / 歌单 / 专辑搜索：官方（本地+飞牛线上）结果在前，酷狗结果在后。

    此前三个 /search/{artist,playlist,album} 路由只要有酷狗结果就直接
    整包返回 kugou_payload，官方搜索结果被整个替换掉（本地歌手被覆盖）；
    且请求上游的这一步从未发生过。

    - 不去重：本地/飞牛线上与酷狗是不同数据源，同名歌手/专辑/歌单是
      不同的可播放对象，全部保留，官方在前、酷狗在后；由用户自行选择
      点哪一个（与 /search/track 的行为一致）
    - 空 coverId 一律兜底为本项自身 guid（本地/线上均同）：
      本地曲目无专辑封面时上游标 coverId=null；酷狗侧 guid 形如
      online:kugou:{artist|album|playlist}:<id>，封面路由本身就该按
      guid 解析，不能因带 online: 前缀就跳过
    - 酷狗侧失败/为空时原样返回官方结果；total 取两边相加
    """
    target_list = ensure_search_list(upstream_json)

    def _fill_cover(item: dict) -> None:
        """空 coverId 兜底为本项自身 guid（本地/线上/酷狗项均适用）。

        酷狗 item 的 guid 形如 online:kugou:{artist|album|playlist}:<id>，
        封面路由本就按 guid 解析，不能因带 online: 前缀就跳过。
        """
        guid = str(item.get("guid") or "").strip()
        if guid and item.get("coverId") in (None, ""):
            item["coverId"] = guid

    # 官方（本地+飞牛线上）项先补一遍；酷狗项在 append 时补。
    for item in target_list:
        if isinstance(item, dict):
            _fill_cover(item)

    kugou_list: list[dict] = []
    kugou_total = 0
    if isinstance(kugou_payload, dict):
        if isinstance(kugou_payload.get("code"), int) and kugou_payload.get("code") != 0:
            kugou_list = []
        else:
            kd = kugou_payload.get("data")
            if isinstance(kd, dict) and isinstance(kd.get("list"), list):
                kugou_list = [x for x in kd["list"] if isinstance(x, dict)]
                kugou_total = _read_int(kd.get("total"), len(kugou_list))

    official_total = _read_int(_search_data_root(upstream_json).get("total"), len(target_list))

    if not kugou_list:
        logger.warning(
            "[SEARCH_%s] official_count=%d official_total=%d kugou_items=0 merged=%d",
            tag.upper(), len(target_list), official_total, len(target_list),
        )
        return upstream_json

    # 酷狗项按原序全部追加（不去重）
    merged = []
    for item in kugou_list:
        _fill_cover(item)
        merged.append(item)

    target_list.extend(merged)
    total = official_total + kugou_total
    if total > 0:
        _search_data_root(upstream_json)["total"] = total

    logger.warning(
        "[SEARCH_%s] official_count=%d official_total=%d kugou_items=%d merged=%d total=%d",
        tag.upper(), len(target_list) - len(merged), official_total,
        len(kugou_list), len(target_list), total,
    )
    return upstream_json


async def _upstream_search_envelope(
    request: Request, client: httpx.AsyncClient
) -> dict | None:
    """拉取官方搜索结果；非 200 / 非 JSON / code!=0 一律返回 None。

    返回 None 时调用方决定回退（用酷狗结果或再透传一次上游响应）。
    """
    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"
    headers = copy_incoming_headers(request)
    req = client.build_request("GET", url_path, headers=headers)
    resp = await client.send(req)
    if resp.status_code != 200:
        logger.warning("[SEARCH_META_UPSTREAM] http=%s path=%s", resp.status_code, request.url.path)
        return None
    try:
        payload = resp.json()
    except Exception:
        logger.warning("[SEARCH_META_UPSTREAM] non-json path=%s", request.url.path)
        return None
    if not isinstance(payload, dict) or payload.get("code") != 0:
        logger.warning(
            "[SEARCH_META_UPSTREAM] bad envelope path=%s code=%r",
            request.url.path, payload.get("code") if isinstance(payload, dict) else None,
        )
        return None
    return payload


async def merged_search_meta(
    request: Request,
    fetcher: Callable[..., Awaitable[dict | None]],
    tag: str,
    cap: int = 500,
) -> Response:
    """歌手/歌单/专辑搜索的统一处理：官方在前，酷狗在后。

    空关键词原样透传上游（官方默认列表），不发酷狗请求。
    酷狗侧失败时不影响官方结果；上游侧失败时用酷狗结果兜底，
    两者都拿不到则再透传上游原始响应。

    分页口径按客户端是否传 page/size 区分：
    - 传了 page/size（PC 网页）：酷狗按该页取，走原有单页合并
    - 未传 page/size（手机端）：酷狗按 50 条/页轮询全部结果再合并，
      一次返回完整列表，客户端不需要翻页
      （酷狗 /search 不支持一次全量，只能循环翻页）
    """
    if not CONF.get("merge_search_meta", True):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    upstream_client = get_upstream_client(request.app)
    keyword = extract_keyword(request)
    if not keyword:
        return await forward_to_upstream(request, upstream_client)

    has_page = request.query_params.get("page") not in (None, "")
    has_size = request.query_params.get("size") not in (None, "")

    if has_page or has_size:
        # PC 网页：按客户端分页参数取单页
        try:
            page = max(1, int(request.query_params.get("page") or 1))
        except (TypeError, ValueError):
            page = 1
        try:
            # 酷狗 /search 未观察到 pagesize 硬上限，50 是响应体积保护。
            size = max(1, int(request.query_params.get("size") or 24))
        except (TypeError, ValueError):
            size = 24
        if size > 50:
            size = 50
        kugou_payload = await fetcher(request.app, keyword, page=page, size=size)
    else:
        # 手机端：不传分页参数，酷狗按余数末页法轮询全部结果一次返回
        kugou_payload = await _fetch_kugou_all_pages(fetcher, request.app, keyword, tag=tag, cap=cap)

    upstream_json = await _upstream_search_envelope(request, upstream_client)

    if not isinstance(upstream_json, dict):
        if isinstance(kugou_payload, dict):
            return JSONResponse(content=kugou_payload, status_code=200)
        return await forward_to_upstream(request, upstream_client)

    return JSONResponse(content=merge_search_meta(upstream_json, kugou_payload, tag=tag))


def extract_guid(request: Request, path_guid: str | None = None) -> str:
    if path_guid:
        return path_guid
    return (
        request.query_params.get("guid")
        or request.query_params.get("trackGUID")
        or request.query_params.get("trackGuid")
        or request.query_params.get("coverId")
        or request.query_params.get("id")
        or request.query_params.get("trackId")
        or ""
    )


async def extract_guid_from_body(request: Request) -> str:
    guid = extract_guid(request)
    if guid:
        return guid
    try:
        body = await request.json()
    except Exception:
        return ""
    if isinstance(body, dict):
        return str(
            body.get("guid")
            or body.get("trackGUID")
            or body.get("trackGuid")
            or body.get("id")
            or body.get("trackId")
            or ""
        )
    return ""


def empty_ok() -> JSONResponse:
    return JSONResponse(content={"code": 0, "msg": "ok", "data": {}})


_EMPTY_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000d49444154789c62f8cfc0f01f00050001009a444a970000000049454e44ae426082"
)


def _static_cover_placeholder_response() -> Response:
    return Response(
        content=_EMPTY_PNG,
        status_code=200,
        media_type="image/png",
        headers={
            "Cache-Control": "public, max-age=3600",
            "Cross-Origin-Resource-Policy": "same-origin",
        },
    )


# 兜底封面：酷狗官方无封面图，所有解析不到的封面统一走这张。
# 通过 /music/api/v1/static/cover 同源输出，避免 COEP 跨域拦截。
_STATIC_COVER_FALLBACK_URL = "https://singerimg.kugou.com/uploadpic/softhead/none.jpg"
_COVER_FALLBACK_CACHE: dict[str, tuple[bytes, str]] = {}

# P1: 共享连接池。原来每个封面请求都新建 AsyncClient —— DNS + TCP + TLS
# 握手每次重做，这是日志里耗时从 p50 0.5s 拉到 max 16.8s 的主因之一。
# 复用后 keepalive 连接在池里挂着，同域后续请求直接命中热连接。
# asyncio.Lock 防止并发首次创建出多个 client 实例（泄漏连接池）。
_COVER_HTTP_TIMEOUT = httpx.Timeout(
    connect=3.0,    # 原来 10s 共用。连不上就等满 10 秒才退兜底，白等
    read=8.0,       # 图最大 500KB，慢网络仍给足
    write=5.0,
    pool=3.0,
)
_COVER_HTTP_LIMITS = httpx.Limits(
    max_connections=50,
    max_keepalive_connections=20,
    keepalive_expiry=30.0,
)
_COVER_HTTP: httpx.AsyncClient | None = None
_COVER_HTTP_LOCK = asyncio.Lock()

# 磁盘缓存：酷狗图片 URL 对同一歌曲恒定不变（stdmusic/160/...），同一张图
# 被反复下载十几次，批量刷列表时 10+ 并发同刷又把连接池顶爆 -> 超时 ->
# fallback 默认图。缓存后首次回源，后续全走本地。
# ============================================================
# 本地曲目缺封面：代理主动上传换官方 coverId，并主动 POST 回写。
#
# 触发条件（严格）：GET /track/metadata 上游返回 data.track.coverId 为空 且
# guid 是本地曲目（无 online:/artist:/album: 前缀）。其余场景（线上曲目、
# 歌单、歌手/专辑实体、已有封面）一律不触碰。
#
# 关键约束：所有内部 POST 必须走 get_upstream_client() 直连上游 unix socket。
# 代理自身有 GET 闸门（0ce5f94），非 GET 会被透传回上游；若内部 POST 走
# self.app.state.upstream_client 之外的自路径则形成自回环。
# ============================================================
import json as _json

_COVER_UPLOAD_LOCK = threading.Lock()
# guid -> 官方 coverId 磁盘映射（懒加载）。
_COVER_ID_CACHE: dict[str, str] = {}
_COVER_ID_CACHE_LOADED = False
# guid -> 开始时间，避免同一首歌并发重复上传（封面批量刷新时并发请求多）。
_COVER_UPLOAD_INFLIGHT: dict[str, float] = {}
_INFLIGHT_STALE_SEC = 600.0
# 失败冷却：上传/回写失败后不再重试，避免坏请求反复打上游。
_COVER_UPLOAD_FAILED: dict[str, float] = {}
_FAILED_COOLDOWN_SEC = 1800.0


async def _trigger_local_cover_upload(
    request: Request,
    app_state,
    guid: str,
    image_bytes: bytes = None,
    fetch: Callable[[], Awaitable[bytes]] = None,
) -> None:
    """本地曲目缺封面时的完整链路：预检 -> 上传换官方 coverId -> POST 回写 -> 缓存。

    严格边界：仅本地曲目 + 上游 track.coverId 为空才上传。线上曲目/歌单/
    歌手专辑实体不进入此函数；已有封面时不产生任何上游写入。
    全程静默失败，不影响当前封面响应。

    image_bytes 为空时可传 fetch(url) 供本函数抓取（例如按 cover_upload_size
    回源酷狗大图）。缓存命中/冷却/配置关闭的检查都在抓取之前，
    保证不会为了拿一张用不上的图而浪费一次网络。

    内部请求必须带音乐端鉴权头（copy_incoming_headers）：上游 socket 的
    track/metadata 无鉴权会回 401。
    """
    global _COVER_UPLOAD_FAILED
    if not guid:
        return
    if not CONF.get("cover_upload_enabled", True):
        return
    now = time.monotonic()
    try:
        if cover_id_from_cache(guid):
            return
        with _COVER_UPLOAD_LOCK:
            if guid in _COVER_UPLOAD_FAILED and now - _COVER_UPLOAD_FAILED[guid] < _FAILED_COOLDOWN_SEC:
                return
            if guid in _COVER_UPLOAD_INFLIGHT:
                return
            _COVER_UPLOAD_INFLIGHT[guid] = now
            # 清理过期条目，避免 dict 无限增长。
            _COVER_UPLOAD_FAILED = {k: t for k, t in _COVER_UPLOAD_FAILED.items() if now - t < _FAILED_COOLDOWN_SEC}
            for g in [k for k, t in _COVER_UPLOAD_INFLIGHT.items() if now - t > _INFLIGHT_STALE_SEC]:
                _COVER_UPLOAD_INFLIGHT.pop(g, None)
        headers = copy_incoming_headers(request)
        # 预检：上游 track.coverId 已存在则直接退出，不给已有封面的歌重复上传。
        client = get_upstream_client(app_state)
        req = client.build_request(
            "GET",
            f"/music/api/v1/track/metadata?guid={quote(guid, safe='')}",
            headers=headers,
            timeout=10.0,
        )
        resp = await client.send(req)
        if resp.status_code != 200:
            logger.warning("[COVERUPLOAD] precheck http=%d guid=%s -> skip", resp.status_code, guid)
            return
        upstream_data = resp.json()
        _data = upstream_data.get("data") if isinstance(upstream_data, dict) else None
        _track = _data.get("track") if isinstance(_data, dict) else None
        if not isinstance(_track, dict):
            _track = {}
        if str(_track.get("coverId") or "").strip():
            return  # 已有封面，不动
        # post_official_cover_id 内部读 data.get("track")，所以传 data 层而非外层
        # {code, data:{track:{...}}}。传外层会取不到 track，回写必报 skip-no-title。
        inner_data = _data if isinstance(_data, dict) else {}
        # 未直接给出图字节时，用调用方提供的 fetch 回源（例如按 cover_upload_size
        # 拉酷狗大图）。抓取失败不阻断：降级为空字节，upload 内部会直接返回空。
        if image_bytes is None and fetch is not None:
            try:
                image_bytes = await fetch() or b""
            except Exception as e:
                logger.warning("[COVERUPLOAD] fetch-err guid=%s err=%s:%s",
                               guid, type(e).__name__, e)
                image_bytes = b""
        cid = await upload_cover_to_official(app_state, image_bytes, headers)
        if not cid:
            _COVER_UPLOAD_FAILED[guid] = time.monotonic()
            return
        # 换到官方 coverId 就先落缓存：即使回写失败，用户下次请求也能看到封面，
        # 不能因为回写没成就把已取得的 coverId 丢掉。
        remember_cover_id(guid, cid)
        if await post_official_cover_id(app_state, guid, cid, inner_data, headers):
            _COVER_UPLOAD_FAILED.pop(guid, None)
        else:
            # 回写失败设上传冷却：coverId 已缓存，下次请求缓存命中会早退，
            # 不会重复上传；冷却过后仍可重试回写。
            _COVER_UPLOAD_FAILED[guid] = time.monotonic()
    except Exception as e:
        logger.warning("[COVERUPLOAD] err guid=%s err=%s:%s", guid, type(e).__name__, e)
        _COVER_UPLOAD_FAILED[guid] = time.monotonic()
    finally:
        _COVER_UPLOAD_INFLIGHT.pop(guid, None)


def _cover_id_cache_path() -> str:
    return CONF.get("cover_id_cache_file") or os.path.join(CONF["cache_dir"], "cover_ids.json")


def _load_cover_id_cache() -> dict:
    global _COVER_ID_CACHE_LOADED
    if _COVER_ID_CACHE_LOADED:
        return _COVER_ID_CACHE
    _COVER_ID_CACHE_LOADED = True
    try:
        with open(_cover_id_cache_path(), "r", encoding="utf-8") as f:
            data = _json.load(f)
        if isinstance(data, dict):
            _COVER_ID_CACHE.clear()
            for k, v in data.items():
                if isinstance(k, str) and isinstance(v, str) and v:
                    _COVER_ID_CACHE[k] = v
    except (OSError, ValueError):
        pass
    except Exception as e:
        logger.warning("[COVERUPLOAD] cache-load err=%s", e)
    return _COVER_ID_CACHE


def remember_cover_id(guid: str, cover_id: str) -> None:
    """记本地 guid -> 官方 coverId 映射并落盘（tmp + os.replace 原子替换）。"""
    if not guid or not cover_id:
        return
    _COVER_ID_CACHE[guid] = cover_id
    path = _cover_id_cache_path()
    tmp = path + ".tmp"
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            _json.dump(_COVER_ID_CACHE, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, path)
    except OSError as e:
        logger.warning("[COVERUPLOAD] cache-write-err err=%s", e)
        try:
            os.unlink(tmp)
        except OSError:
            pass
    except Exception as e:
        logger.warning("[COVERUPLOAD] cache-write-err err=%s", e)


def cover_id_from_cache(guid: str) -> str:
    """取已换到的官方 coverId；无记录返回空串（空串表示未命中，调用方保留 guid 兜底）。"""
    if not guid:
        return ""
    return _load_cover_id_cache().get(guid, "")


async def upload_cover_to_official(app_state, image_bytes: bytes, headers: dict) -> str:
    """POST /music/api/v1/static/cover/track (multipart/form-data, name=file) 换官方 coverId。

    headers 必须带音乐端鉴权（cookie / authorization / x-trim-music-temp-token），
    否则飞牛回 401。
    """
    if not image_bytes or len(image_bytes) > int(CONF.get("cover_upload_max_bytes") or 4 * 1024 * 1024):
        return ""
    if image_bytes.startswith(b"\x89PNG"):
        ext = "png"
    elif image_bytes[:3] == b"\xff\xd8\xff":
        ext = "jpg"
    elif image_bytes[:6] in (b"GIF87a", b"GIF89a"):
        ext = "gif"
    elif image_bytes[:4] == b"RIFF":
        ext = "webp"
    else:
        ext = "jpg"
    mime = {"jpg": "image/jpeg", "png": "image/png", "gif": "image/gif",
            "webp": "image/webp"}[ext]
    # 直接用 bytes 当 multipart 内容，不落临时文件、不持文件句柄。
    # 注意：build_request() 只记录 content，真正读取发生在 send()；
    # 若用 with open() 包住 build_request，退出 with 时会先关文件再读，报
    # "seek of closed file"。字节内容无此问题。
    try:
        client = get_upstream_client(app_state)
        req = client.build_request(
            "POST",
            "/music/api/v1/static/cover/track",
            files={"file": (f"cover.{ext}", image_bytes, mime)},
            headers=headers,
            timeout=float(CONF.get("cover_upload_timeout") or 15),
        )
        resp = await client.send(req)
        if resp.status_code != 200:
            logger.warning("[COVERUPLOAD] http=%d body=%s", resp.status_code, resp.text[:200])
            return ""
        data = resp.json()
        d = data.get("data") or {}
        cid = str(d.get("coverId") or "").strip()
        if not cid and isinstance(d, dict):
            cid = str(d.get("guid") or "").strip()
        logger.warning("[COVERUPLOAD] code=%s coverId=%s bytes=%d", data.get("code"), cid or "(empty)", len(image_bytes))
        return cid if data.get("code") == 0 else ""
    except Exception as e:
        logger.warning("[COVERUPLOAD] err=%s:%s", type(e).__name__, e)
        return ""


async def post_official_cover_id(app_state, guid: str, cover_id: str, data: dict, headers: dict) -> bool:
    """POST /music/api/v1/track/metadata 回写官方 coverId，用户无感。

    仅提交上游已刮削的有值字段，再加 coverId/coverGUID。空数组与 None
    一律不提交 —— metadata correction 端点会把空 artistGUIDs 解成
    「清空歌手」，误传会把歌曲刮削结果弄坏。
    """
    track = data.get("track") if isinstance(data, dict) else None
    track = track if isinstance(track, dict) else {}
    payload: dict[str, Any] = {"guid": guid, "coverId": cover_id, "coverGUID": cover_id}
    for key in ("title", "album"):
        value = track.get(key) or data.get(key)
        if str(value or "").strip():
            payload[key] = value
    for key in ("artistGUIDs", "genreGUIDs"):
        value = track.get(key) if track.get(key) is not None else data.get(key)
        if isinstance(value, list) and value:
            payload[key] = value
    for key in ("year", "discNo", "trackNo"):
        value = track.get(key) if track.get(key) is not None else data.get(key)
        if value is not None:
            payload[key] = value
    if not str(payload.get("title") or "").strip():
        logger.warning("[COVERWRITE] skip-no-title guid=%s (metadata incomplete)", guid)
        return False
    try:
        client = get_upstream_client(app_state)
        req = client.build_request(
            "POST",
            "/music/api/v1/track/metadata",
            json=payload,
            headers={**headers, "Content-Type": "application/json"},
            timeout=20.0,
        )
        resp = await client.send(req)
        if resp.status_code != 200:
            logger.warning("[COVERWRITE] http=%d body=%s guid=%s", resp.status_code, resp.text[:200], guid)
            return False
        r = resp.json()
        ok = r.get("code") == 0
        logger.warning("[COVERWRITE] %s code=%s guid=%s coverId=%s", "OK" if ok else "FAIL", r.get("code"), guid, cover_id)
        return ok
    except Exception as e:
        logger.warning("[COVERWRITE] err=%s:%s", type(e).__name__, e)
        return False


_COVER_DISK_TTL = 24 * 3600
_COVER_DISK_MAX_FILES = 500


def _cover_disk_dir() -> str:
    return CONF["cache_dir"]


def _cover_disk_file(url: str) -> str:
    return os.path.join(
        _cover_disk_dir(),
        hashlib.sha256(url.encode("utf-8")).hexdigest()[:32] + ".cover",
    )


def _cover_disk_get(url: str) -> Response | None:
    """读磁盘缓存；不存在/过期/读失败均返回 None。"""
    path = _cover_disk_file(url)
    try:
        st = os.stat(path)
        if time.time() - st.st_mtime > _COVER_DISK_TTL:
            return None
        with open(path, "rb") as f:
            body = f.read()
        if not body:
            return None
    except (FileNotFoundError, OSError):
        return None
    except Exception as e:
        logger.warning("[COVERCACHE] read-fail path=%s err=%s", path, e)
        return None
    return Response(
        content=body,
        status_code=200,
        media_type="image/jpeg",
        headers={
            "Cache-Control": "public, max-age=86400",
            "Cross-Origin-Resource-Policy": "same-origin",
        },
    )


def _cover_disk_put(url: str, body: bytes) -> None:
    """写磁盘缓存。tmp + os.replace 原子替换，防并发写半截文件。"""
    path = _cover_disk_file(url)
    tmp = path + ".tmp"
    try:
        os.makedirs(_cover_disk_dir(), exist_ok=True)
        with open(tmp, "wb") as f:
            f.write(body)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning("[COVERCACHE] write-fail path=%s err=%s", path, e)
        try:
            os.unlink(tmp)
        except OSError:
            pass


def _cover_disk_cleanup() -> None:
    """超限时按 mtime 清旧，避免缓存无限增长。"""
    try:
        files = [
            (os.stat(p).st_mtime, p)
            for p in (
                os.path.join(_cover_disk_dir(), n)
                for n in os.listdir(_cover_disk_dir())
                if n.endswith(".cover")
            )
        ]
        if len(files) <= _COVER_DISK_MAX_FILES:
            return
        for _, p in sorted(files)[: len(files) - _COVER_DISK_MAX_FILES]:
            try:
                os.unlink(p)
            except OSError:
                pass
    except Exception as e:
        logger.warning("[COVERCACHE] cleanup-fail err=%s", e)


async def _cover_http() -> httpx.AsyncClient:
    """进程内共享的封面下载 client；关闭/异常后按请求重建。"""
    global _COVER_HTTP
    if _COVER_HTTP is not None and not _COVER_HTTP.is_closed:
        return _COVER_HTTP
    async with _COVER_HTTP_LOCK:
        if _COVER_HTTP is None or _COVER_HTTP.is_closed:
            _COVER_HTTP = httpx.AsyncClient(
                timeout=_COVER_HTTP_TIMEOUT,
                limits=_COVER_HTTP_LIMITS,
                follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.kugou.com/"},
            )
    return _COVER_HTTP


# 同一 URL 的并发兜底请求只让第一个真回源，其余等它 —— singleflight。
# 冷启动时 N 个封面同时失败，会同时触发 N 个兜底回源，而缓存要等第一个
# 回源成功后才写入；日志实锤 7 个并发各自回源、全超时、全退 1x1。
_COVER_FALLBACK_LOCK = asyncio.Lock()


async def _fetch_cover_fallback_response() -> Response:
    """兜底封面响应：优先内存缓存，回源一次后常驻；拉取失败退回 1x1 透明 PNG。

    进程生命周期内只回源一次，避免每个 guid 的兜底请求都打一次网络。
    并发请求共用同一次回源结果，避免冷启动时重复打同一个 URL。
    """
    hit = _COVER_FALLBACK_CACHE.get("resp")
    if hit is not None:
        body, ct = hit
        logger.warning("[COVERFALLBACK] cache-hit bytes=%d ct=%s", len(body), ct)
        return Response(
            content=body,
            status_code=200,
            media_type=ct,
            headers={
                "Cache-Control": "public, max-age=86400",
                "Cross-Origin-Resource-Policy": "same-origin",
            },
        )
    async with _COVER_FALLBACK_LOCK:
        # 抢到锁后再查一次：可能前一个持有者刚写入缓存
        hit = _COVER_FALLBACK_CACHE.get("resp")
        if hit is not None:
            body, ct = hit
            logger.warning("[COVERFALLBACK] cache-hit-after-wait bytes=%d ct=%s", len(body), ct)
            return Response(
                content=body,
                status_code=200,
                media_type=ct,
                headers={
                    "Cache-Control": "public, max-age=86400",
                    "Cross-Origin-Resource-Policy": "same-origin",
                },
            )
        resp = await _fetch_cover_image_response(_STATIC_COVER_FALLBACK_URL)
        if resp is not None:
            _COVER_FALLBACK_CACHE["resp"] = (resp.body, resp.media_type or "image/jpeg")
            logger.warning("[COVERFALLBACK] origin-ok bytes=%d ct=%s url=%s",
                           len(resp.body), resp.media_type, _STATIC_COVER_FALLBACK_URL)
            return resp
        # 兜底图本身也拉不下来，退回 1x1 透明 PNG —— 用户看到的 1x1 就是这一路
        logger.warning("[COVERFALLBACK] origin-FAILED -> 1x1 PLACEHOLDER url=%s", _STATIC_COVER_FALLBACK_URL)
        return _static_cover_placeholder_response()


def build_lyric_list_payload(guid: str, lyric_text: str) -> dict:
    """对齐飞牛 $n.lyric.list → xr(list, preferred)。

    每条需有非空 content；source=2 表示 EXTERNAL_LRC（非内嵌，不强制 offset）。
    """
    text = (lyric_text or "").strip()
    if not text:
        return {"code": 0, "msg": "ok", "data": {"list": [], "preferred": ""}}
    lyric_guid = f"{guid}:lyric"
    now = int(time.time())
    item = {
        "guid": lyric_guid,
        "content": text,
        "source": 2,
        "isLRC": True,
        "offset": 0,
        "createdAt": now,
        "updatedAt": now,
    }
    return {
        "code": 0,
        "msg": "ok",
        "data": {"list": [item], "preferred": lyric_guid},
    }


def stub_online_info(guid: str) -> dict:
    song_id = song_id_from_online_guid(guid)
    return {
        "id": song_id,
        "source": source_from_online_guid(guid),
        "title": "",
        "artist": "",
        "album": "",
        "duration_s": 0,
        "ext": "mp3",
        "file_size": 0,
        "cover_url": "",
        "lyric": "",
    }


def build_metadata_payload(guid: str, data: dict | None) -> dict:
    """飞牛 resolveTrackPlayback._h() 会无防护读取 data.track.genres.join / album / artists。

    缺 genres 或 album 不是对象时直接抛错，播放器跳过且不会请求 stream。
    """
    info = dict(data or {})
    info.setdefault("id", song_id_from_online_guid(guid))
    info.setdefault("source", source_from_online_guid(guid))
    cover_url = str(info.get("cover_url") or info.get("coverUrl") or "").strip()
    vo = build_online_track(info)
    album_obj = vo["album"] if isinstance(vo.get("album"), dict) else {
        "name": str(vo.get("album") or ""),
        "guid": f"{guid}:album",
        "artists": vo.get("artists") or [],
        "coverId": guid,
    }
    track = {
        "guid": guid,
        "id": guid,
        "title": vo.get("title") or "",
        "artists": vo.get("artists") or [],
        "album": album_obj,
        "genres": list(vo.get("genres") or []),
        "duration": vo.get("duration") or 0,
        "coverId": guid,
        "coverUrl": cover_url,
        "format": vo.get("format") or "mp3",
        "hasLyric": True,
        "isFavorite": False,
        "isCue": False,
        "accessStatus": 0,
        "audioSpec": vo["audioSpec"],
    }
    return {
        "code": 0,
        "msg": "ok",
        "data": {
            **vo,
            "guid": guid,
            "id": guid,
            "album": album_obj,
            "audioSpec": vo["audioSpec"],
            "track": track,
        },
    }


def _conf_log_value(key: str, value: Any) -> Any:
    lowered = key.lower()
    if any(part in lowered for part in _REDACT_KEY_PARTS):
        return "***" if value else ""
    return value


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    logger.info("=== fnmusic-ext configuration ===")
    for k, v in CONF.items():
        logger.info("  %s = %s", k, _conf_log_value(k, v))
    logger.info("==================================")

    created_upstream = False

    if getattr(fastapi_app.state, "upstream_client", None) is None:
        fastapi_app.state.upstream_client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=CONF["upstream_sock"]),
            base_url="http://unix",
            timeout=30.0,
        )
        created_upstream = True

    try:
        yield
    finally:
        if created_upstream and getattr(fastapi_app.state, "upstream_client", None):
            await fastapi_app.state.upstream_client.aclose()
            fastapi_app.state.upstream_client = None

app = FastAPI(title="fnmusic-ext", lifespan=lifespan)


@app.get("/_ext/healthz")
async def ext_healthz(request: Request):
    upstream_client = get_upstream_client(request.app)

    upstream_status = "fail"
    kugou_status = "fail"

    try:
        r = await upstream_client.get("/music/api/v1/search/track?keyword=healthz_probe", timeout=2.0)
        if r.status_code < 500:
            upstream_status = "ok"
    except Exception as e:
        logger.debug("Upstream health check failed: %s", e)

    if not CONF.get("kugou_enabled", True):
        kugou_status = "disabled"
    else:
        try:
            async with httpx.AsyncClient(timeout=3.0) as kc:
                # /login/qr/key 不需要 Authorization 头，适合作为纯连通性探测
                r = await kc.get(CONF["kugou_url"].rstrip("/") + "/login/qr/key")
                if r.status_code == 200:
                    kugou_status = "ok"
                else:
                    kugou_status = f"http_{r.status_code}"
        except Exception as e:
            logger.debug("Kugou health check failed: %s", e)
    source_ok = kugou_status == "ok"

    return {
        "ok": upstream_status == "ok" and source_ok,
        "upstream": upstream_status,
        "kugou": kugou_status,
    }


@app.get("/music/api/v1/search/track")
@app.get("/music/api/v1/search/track/{subpath:path}")
async def search_track(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    upstream_client = get_upstream_client(request.app)
    keyword = extract_keyword(request)

    page_str = request.query_params.get("page")
    try:
        page = int(page_str) if page_str else 1
    except (TypeError, ValueError):
        page = 1
    if page < 1:
        page = 1

    size_str = request.query_params.get("size")
    try:
        size = int(size_str) if size_str else None
    except (TypeError, ValueError):
        size = None
    # 没传 size = 手机端：返回合并全量，不切片（size=None）。
    # 传了 size = PC 端：按客户端 size 切片（飞牛 PC 每页 50），由
    # merge_online_tracks 按 page/size 切合并全量列表。
    if size is not None and size < 1:
        size = 50

    url_path = request.url.path
    params = dict(request.query_params)
    params.pop("size", None)
    params.pop("page", None)
    url_path = f"{url_path}?{urlencode(params)}" if params else url_path
    headers = copy_incoming_headers(request)

    req = upstream_client.build_request("GET", url_path, headers=headers)
    upstream_resp = await upstream_client.send(req)

    resp_headers = filter_headers(upstream_resp.headers, exclude_keys={"content-length", "content-encoding"})

    if upstream_resp.status_code != 200:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    try:
        upstream_json = upstream_resp.json()
    except Exception:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    if not isinstance(upstream_json, dict) or upstream_json.get("code") != 0:
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    if not keyword:
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    logger.warning("[SEARCH] keyword=%r page=%d size=%d kugou_enabled=%s token_len=%d userid=%s",
                   keyword, page, size, CONF.get("kugou_enabled"),
                   len(CONF.get("kugou_token") or ""), CONF.get("kugou_userid") or "(none)")

    now = time.time()
    cached_entry = _SEARCH_CACHE.get(keyword)
    is_valid_cache = cached_entry is not None and (now - cached_entry.get("ts", 0) < CONF["search_cache_ttl"])

    if not (is_valid_cache and cached_entry is not None):
        cached_entry: dict[str, Any] = {"ts": time.time(), "official_total": None, "online": {}}
        _set_search_cache(keyword, cached_entry)
    entry = cached_entry
    entry.setdefault("online", {})

    async def _get_online_result(kugou_page: int, page_size: int) -> dict | None:
        # 缓存键必须带页大小：同一页号在不同 pagesize 下内容不同，
        # 只按页号存会在步长调整后串数据。
        key = (kugou_page, page_size)
        cache = entry.setdefault("online", {})
        cached = cache.get(key)
        if isinstance(cached, dict):
            return cached
        try:
            kugou_res = await asyncio.wait_for(
                fetch_kugou_search(keyword, page_size, page=kugou_page), timeout=min(float(CONF["search_timeout"]), 8.0)
            )
        except Exception as exc:
            logger.warning("kugou search failed keyword=%r page=%d: %s", keyword, kugou_page, exc)
            return None
        if not isinstance(kugou_res, dict):
            return None
        result = {
            **kugou_res,
            "items": deduplicate_online_items(kugou_res.get("items", [])),
            "pagesize": page_size,
        }
        cache[key] = result
        return result

    async def _fetch_online_pages() -> dict | None:
        """歌曲搜索：余数末页法轮询酷狗，取满上限后交给上层按 page/size 切片。

        酷狗 /search 有结果条数上限（歌曲/歌单 480）且末页必须满足
        from + size <= 上限，否则返回 149 "Out Page Range"（HTTP 502）。
        步长固定 50，最后一页用剩余条数当 size 并把页号对齐到
        offset // step + 1，使 from + size 恰好等于上限，不越界。
        例：上限 480 -> (1,50)...(9,50) 后接 (16,30)，from=450, 450+30=480。

        终止判据用「本轮是否新增」而不是「本页条数」：酷狗跨页重复会让某页
        去重后不足页大小，按条数判末页会提前收工。total 是上限值而非实际
        计数（周杰伦只有 99 首同样报 480），也不能拿来判断取满。
        """
        step = max(1, int(CONF.get("kugou_step") or 50))
        cap = int(CONF.get("kugou_search_limit") or 480)
        deadline = time.time() + 25.0
        items: list[dict] = []
        seen: set[tuple[str, str, str]] = set()
        declared_total = 0
        cache = entry.setdefault("online", {})
        params = _kugou_page_params(cap, step)
        pages_fetched = 0

        for p, step_size, step_total in params:
            pages_fetched = p
            cached = cache.get((p, step_size))
            if not isinstance(cached, dict):
                cached = await _get_online_result(p, step_size)
                if not isinstance(cached, dict):
                    logger.warning("[SEARCH_KUGOU_PAGE_FAIL] keyword=%r kugou_page=%d size=%d", keyword, p, step_size)
                    break
            page_items = cached.get("items", [])
            if not isinstance(page_items, list):
                page_items = []
            added = 0
            for item in page_items:
                if not isinstance(item, dict):
                    continue
                key = _kugou_item_key(item)
                if key in seen:
                    continue
                seen.add(key)
                items.append(item)
                added += 1
            declared = _read_int(cached.get("total"), 0)
            if declared > declared_total:
                declared_total = declared
            logger.warning(
                "[SEARCH_KUGOU_PAGE_FETCH] keyword=%r kugou_page=%d size=%d page_items=%d added=%d merged_items=%d cap=%d declared=%d",
                keyword, p, step_size, len(page_items), added, len(items), cap, declared,
            )
            if len(items) >= cap:
                break
            if p > 1 and added == 0:
                break
            if len(items) >= 5000:
                break
            if time.time() >= deadline:
                break

        # 取满上限即 complete。total 保留酷狗声明值仅供日志与前端「结果上限」
        # 提示；实际返回条数由上层 merge_online_tracks 计算并写回 parent。
        result = {
            "items": deduplicate_online_items(items),
            "total": declared_total if declared_total > 0 else len(items),
            "page": pages_fetched,
            "pagesize": step,
            "pages": pages_fetched,
            "fetched_pages": pages_fetched,
            "fetched_items": len(items),
            "complete": len(items) >= cap,
        }
        cache["__all__"] = result
        logger.warning("[SEARCH_KUGOU_ALL] keyword=%r items=%d cap=%d complete=%s",
                       keyword, len(result.get("items", [])), cap, result["complete"])
        return result

    online_all: dict | None = None

    # 飞牛官方 total 在首页更稳定；后续页官方结果可能已空，沿用已缓存的官方 total。
    parent = _search_data_root(upstream_json)
    current_official_total = _read_int(parent.get("total"), len(ensure_search_list(upstream_json)))
    cached_official_total = entry.get("official_total")
    if cached_official_total is None or current_official_total > _read_int(cached_official_total, 0):
        entry["official_total"] = current_official_total
    official_total = _read_int(entry.get("official_total"), current_official_total)

    if CONF.get("kugou_enabled", True):
        # 酷狗不支持全量拉取，按 50 条/页轮询直到取够 total。
        online_all = await _fetch_online_pages()

    logger.warning("[SEARCH_KUGOU_PAGE] keyword=%r official_total=%d kugou_pages=%s fetched_pages=%s online_items=%d online_total=%s",
                   keyword, official_total, ((online_all or {}).get("page")), ((online_all or {}).get("fetched_pages")), len((online_all or {}).get("items", [])), (online_all or {}).get("total"))

    merged = merge_online_tracks(upstream_json, online_all, page=page, size=size)
    return JSONResponse(content=merged, status_code=upstream_resp.status_code, headers=resp_headers)


@app.get("/music/api/v1/search/artist")
@app.get("/music/api/v1/search/artist/{subpath=path}")
async def search_artist(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    """/search/artist：酷狗歌手搜索；空关键词走飞牛上游。

    飞牛请求形如 /music/api/v1/search/artist?q=%E6%9C%AC%E5%85%AE&page=1&size=24。
    数据源：KuGouMusicApi /search?keywords=<kw>&page=&pagesize=&type=author。

    响应结构对齐飞牛原生 /search/artist（{"code":0,"data":{"list":[...],"total":n}}），
    artist 项字段：guid/name/coverId/createdAt/updatedAt/trackCount/albumCount/score。
    本地与酷狗结果合并，官方在前、酷狗在后（见 merged_search_meta）。
    """
    return await merged_search_meta(request, fetch_kugou_artist_search, tag="artist")

@app.get("/music/api/v1/search/album")
@app.get("/music/api/v1/search/album/{subpath=path}")
async def search_album(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    """/search/album：酷狗专辑搜索；空关键词走飞牛上游。

    飞牛请求形如 /music/api/v1/search/album?q=%E5%BC%A0%E6%9D%B0&page=1&size=24。
    数据源：KuGouMusicApi /search?keywords=<kw>&page=&pagesize=&type=album。

    响应结构对齐飞牛原生 /search/album（{"code":0,"data":{"list":[...],"total":n}}），
    album 项字段：guid/name/coverId/releaseDate/barcode/createdAt/updatedAt/
    artists/trackCount/score。本地与酷狗结果合并，官方在前、酷狗在后。
    """
    return await merged_search_meta(request, fetch_kugou_album_search, tag="album")


async def _fetch_kugou_all_pages(
    fetcher: Callable[..., Awaitable[dict | None]],
    app_state,
    keyword: str,
    tag: str = "meta",
    cap: int = 500,
) -> dict:
    """歌手/专辑/歌单：余数末页法轮询酷狗，汇总全部结果。

    歌手/专辑搜索上限 500，步长 50 正好整除，末页 (10,50) 时
    from=450, 450+50=500 同样不越界。歌单走同一上限（480）时末页
    自动降级为 (16,30)，from=450, 450+30=480。

    终止判据用「本轮是否新增」：酷狗跨页重复会让某页去重后不足页大小，
    按条数判末页会提前收工。total 是上限值而非实际计数，不能拿来判断取满。
    """
    step = max(1, int(CONF.get("kugou_step") or 50))
    # cap 由调用方指定（专辑/歌手 500，歌单 480）；未传时回落到配置默认值。
    cap = int(cap or CONF.get("kugou_meta_limit") or 500)
    per_page_timeout = float(CONF.get("meta_full_poll_page_timeout") or 8)

    all_items: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    declared_total = 0
    pages_fetched = 0

    for p, step_size, _step_total in _kugou_page_params(cap, step):
        pages_fetched = p
        try:
            res = await asyncio.wait_for(
                fetcher(app_state, keyword, page=p, size=step_size),
                timeout=per_page_timeout,
            )
        except Exception as exc:
            logger.warning("[SEARCH_%s] full poll page=%d size=%d failed: %s",
                           tag.upper(), p, step_size, exc)
            break

        if not isinstance(res, dict):
            break
        data = res.get("data") or {}
        if not isinstance(data, dict):
            break
        items = data.get("list") or []
        if not isinstance(items, list):
            items = []
        declared = _read_int(data.get("total"), 0)
        if declared > declared_total:
            declared_total = declared

        added = 0
        for it in items:
            if not isinstance(it, dict):
                continue
            key = _kugou_item_key(it)
            if key in seen:
                continue
            seen.add(key)
            all_items.append(it)
            added += 1

        logger.warning(
            "[SEARCH_%s] full poll page=%d size=%d got=%d added=%d items=%d cap=%d declared=%d",
            tag.upper(), p, step_size, len(items), added, len(all_items), cap, declared,
        )
        if len(all_items) >= cap:
            break
        if p > 1 and added == 0:
            break
        if len(all_items) >= 5000:
            break
        if not items:
            break

    logger.warning(
        "[SEARCH_%s] full poll done pages=%d items=%d cap=%d declared=%d",
        tag.upper(), pages_fetched, len(all_items), cap, declared_total,
    )
    return {
        "code": 0,
        "msg": "",
        "data": {
            "list": all_items,
            "total": len(all_items),
            "pages": pages_fetched,
            "pagesize": step,
        },
    }

@app.get("/music/api/v1/search/suggest")
@app.get("/music/api/v1/search/suggest/{subpath:path}")
async def search_suggest(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    if not CONF["merge_suggest"]:
        return await forward_to_upstream(request, get_upstream_client(request.app))

    upstream_client = get_upstream_client(request.app)
    keyword = extract_keyword(request)

    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"
    headers = copy_incoming_headers(request)

    kugou_task: asyncio.Task | None = None
    if keyword and CONF.get("kugou_enabled", True):
        kugou_task = asyncio.create_task(fetch_kugou_search(keyword, 8, page=1))

    req = upstream_client.build_request("GET", url_path, headers=headers)
    upstream_resp = await upstream_client.send(req)
    resp_headers = filter_headers(upstream_resp.headers, exclude_keys={"content-length", "content-encoding"})

    if upstream_resp.status_code != 200:
        if kugou_task:
            kugou_task.cancel()
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    try:
        upstream_json = upstream_resp.json()
    except Exception:
        if kugou_task:
            kugou_task.cancel()
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    if not isinstance(upstream_json, dict) or upstream_json.get("code") != 0:
        if kugou_task:
            kugou_task.cancel()
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    kugou_data: dict | list[dict] | None = None
    if kugou_task:
        try:
            # 先向上游响应，再等 Kugou；Kugou 重试可能 2-3s，给 15s 余量
            kugou_data = await asyncio.wait_for(
                asyncio.shield(kugou_task), timeout=15.0
            )
        except asyncio.TimeoutError:
            logger.warning("[suggest] kugou task timeout keyword=%r", keyword)
        except Exception as e:
            logger.warning("Suggest kugou error: %s", e)
            kugou_task.cancel()
        else:
            logger.info("[suggest] keyword=%r kugou items=%d", keyword, len((kugou_data or {}).get("items", []) if isinstance(kugou_data, dict) else (kugou_data or [])))

    data_field = upstream_json.get("data")
    logger.warning("[SUGGEST] keyword=%r upstream data type=%s keys=%s",
                   keyword, type(data_field).__name__,
                   list(data_field.keys())[:10] if isinstance(data_field, dict) else (len(data_field) if isinstance(data_field, list) else 'N/A'))

    # 自动探测：支持多种 data 结构
    # 飞牛 suggest 实际结构：data = {track:{total,items[]}, album:{...}, artist:{...}, playlist:{...}}
    # 把 Kugou 歌曲追加到 track.items
    target_list = None
    target_type_key = None  # 记录追加到的顶层 key，以便同步更新 total
    if isinstance(data_field, list):
        target_list = data_field
    elif isinstance(data_field, dict):
        # 优先探测 track(飞牛 suggest 标准结构，单数)，再试常见变体
        for k in ("track", "tracks", "songs", "items", "list", "results", "searchResults"):
            v = data_field.get(k)
            if isinstance(v, list):
                target_list = v
                target_type_key = k
                break
            elif isinstance(v, dict) and isinstance(v.get("items"), list):
                # v = {total, items[]}，追到 items
                target_list = v["items"]
                target_type_key = k
                break
        if target_list is None:
            # 无匹配时新建 track 结构，方便前端也能读到
            data_field["track"] = {"total": 0, "items": []}
            target_list = data_field["track"]["items"]
            target_type_key = "track"

    if isinstance(kugou_data, dict):
        kugou_list = kugou_data.get("items", []) if isinstance(kugou_data.get("items", []), list) else []
    elif isinstance(kugou_data, list):
        kugou_list = kugou_data
    else:
        kugou_list = []

    if isinstance(target_list, list) and kugou_list:
        # 收集已有项身份去重
        existing_keys = set()
        for item in target_list:
            if isinstance(item, str):
                existing_keys.add(item.strip().lower())
            elif isinstance(item, dict):
                t = str(item.get("title") or item.get("songname") or item.get("name") or "").strip().lower()
                a = str(item.get("artist") or item.get("singername") or item.get("singer") or "").strip().lower()
                if t and a:
                    existing_keys.add(f"{t}|{a}")
                elif t:
                    existing_keys.add(t)

        # 探测 target_list 元素类型
        sample = target_list[0] if target_list else None
        if isinstance(sample, str):
            elem_type = "str"
        elif isinstance(sample, dict):
            elem_type = "dict"
        else:
            elem_type = "dict"

        added = 0
        for item in kugou_list:
            title = str(item.get("title") or "").strip()
            artist = str(item.get("artist") or "").strip()
            key_t = title.lower()
            key_ta = f"{key_t}|{artist.lower()}" if artist else key_t
            if not key_t or key_t in existing_keys or key_ta in existing_keys:
                continue
            if elem_type == "str":
                target_list.append(f"{title}-{artist}" if artist else title)
            else:
                # 飞牛 track.items 元素是完整 track 对象，用 build_online_track 构造
                target_list.append(build_online_track(item))
            existing_keys.add(key_ta or key_t)
            existing_keys.add(key_t)
            added += 1
            if added >= 8:
                break

        # 同步更新对应 type 的 total
        if target_type_key and isinstance(data_field, dict):
            tp = data_field.get(target_type_key)
            if isinstance(tp, dict) and "total" in tp:
                try:
                    tp["total"] = int(tp.get("total") or 0) + added
                except (TypeError, ValueError):
                    tp["total"] = len(target_list)

        logger.warning("[SUGGEST] keyword=%r merged added=%d target=%s len=%d elem=%s",
                       keyword, added, target_type_key or 'list', len(target_list), elem_type)

    # ---- 酷狗 专辑/歌手/歌单 注入 ----
    # 飞牛 suggest 的 data 是分组结构 {track, album, artist, playlist}，此前只注入了
    # track 这一组；客户端在其余三组拿到空列表，下拉建议就只剩歌曲。
    # 数据源与 /search/album、/search/artist、/search/playlist 三个路由同接口同字段，
    # 直接复用各自的 fetch 函数，避免映射逻辑出现第二份。
    # 每项截 6 条：suggest 是下拉补全场景，给 track 留位置。
    SUGGEST_KUGOU_PER_TYPE = 6
    SUGGEST_KUGOU_TIMEOUT_S = 15.0

    async def _fetch_kugou_suggest_all():
        if not CONF.get("kugou_enabled", True):
            return {}
        results: dict[str, Any] = {}
        pending = [
            asyncio.create_task(
                fetch_kugou_album_search(request.app, keyword, page=1, size=SUGGEST_KUGOU_PER_TYPE)),
            asyncio.create_task(
                fetch_kugou_artist_search(request.app, keyword, page=1, size=SUGGEST_KUGOU_PER_TYPE)),
            asyncio.create_task(
                fetch_kugou_playlist_search(request.app, keyword, page=1, size=SUGGEST_KUGOU_PER_TYPE)),
        ]
        names = ("album", "artist", "playlist")
        try:
            done = await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True),
                                           timeout=SUGGEST_KUGOU_TIMEOUT_S)
            for name, res in zip(names, done):
                if isinstance(res, Exception):
                    logger.warning("[SUGGEST] kugou %s error keyword=%r err=%s",
                                   name, keyword, res)
                    continue
                lst = (res or {}).get("data", {}).get("list") or []
                results[name] = lst[:SUGGEST_KUGOU_PER_TYPE]
        except asyncio.TimeoutError:
            logger.warning("[SUGGEST] kugou album/artist/playlist timeout keyword=%r", keyword)
            for t in pending:
                t.cancel()
        return results

    kugou_grouped = await _fetch_kugou_suggest_all()

    def _merge_kugou_group(type_key: str, items: list[Any]) -> int:
        """把酷狗结果并进 data[type_key]，兼容 {total,items[]} / {total,list[]} / 数组三种形态。

        只合并已存在的组，不新建：客户端不请求该类型时，凭空加字段反而可能改变
        它解析 data 的假设。同 guid 或同名视为已有，不重复追加。
        """
        slot = data_field.get(type_key) if isinstance(data_field, dict) else None
        # total_key 必须在三个分支都赋值，否则裸数组分支会 NameError。
        total_key = "total"
        if isinstance(slot, list):
            arr, holder = slot, data_field
        elif isinstance(slot, dict):
            if isinstance(slot.get("items"), list):
                arr, holder = slot["items"], slot
            elif isinstance(slot.get("list"), list):
                arr, holder = slot["list"], slot
            else:
                return 0
        else:
            return 0
        seen = set()
        for x in arr:
            if isinstance(x, str):
                seen.add(x.strip().lower())
            elif isinstance(x, dict):
                g = str(x.get("guid") or "").strip().lower()
                if g:
                    seen.add(g)
                n = str(x.get("name") or x.get("title") or "").strip().lower()
                if n:
                    seen.add(n)
        merged = 0
        for it in items:
            if not isinstance(it, dict):
                continue
            g = str(it.get("guid") or "").strip().lower()
            n = str(it.get("name") or "").strip().lower()
            if (g and g in seen) or (n and n in seen):
                continue
            arr.append(it)
            if g:
                seen.add(g)
            if n:
                seen.add(n)
            merged += 1
        try:
            holder[total_key] = int(holder.get(total_key) or 0) + merged
        except (TypeError, ValueError):
            holder[total_key] = len(arr)
        return merged

    merged_counts = {n: _merge_kugou_group(n, v) for n, v in kugou_grouped.items()}
    logger.warning("[SUGGEST] kugou groups keyword=%r merged=%s data_keys=%s",
                   keyword, merged_counts,
                   list(data_field.keys())[:15] if isinstance(data_field, dict) else 'N/A')

    return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)


def stream_tee_response(
    resp: httpx.Response,
    guid: str,
    range_header: str | None,
    coro_factory: Callable[[], Coroutine[Any, Any, Any]] | None = None,
    client_to_close: httpx.AsyncClient | None = None,
    resolved_ext: str | None = None,
    pre_info: dict | None = None,
) -> Response:
    out_headers = {"Accept-Ranges": "bytes"}
    for k in ("content-type", "content-length", "content-range"):
        v = resp.headers.get(k)
        if v:
            out_headers[k] = v

    if resolved_ext:
        out_headers["content-type"] = media_type_for_ext(resolved_ext)

    status_code = resp.status_code
    content_length_str = resp.headers.get("content-length")
    content_length = (
        int(content_length_str) if content_length_str and content_length_str.isdigit() else None
    )

    ext = (resolved_ext or "").strip().lower() or ext_from_content_type(resp.headers.get("content-type") or "")

    if should_cache(range_header):
        queue: asyncio.Queue[bytes | None] = asyncio.Queue()

        async def _downloader():
            parts: list[bytes] = []
            written = 0
            try:
                async for chunk in resp.aiter_bytes():
                    if chunk:
                        parts.append(chunk)
                        written += len(chunk)
                        await queue.put(chunk)
            except Exception as e:
                logger.warning("tee download failed for %s: %s", guid, e)
            finally:
                await resp.aclose()
                if client_to_close:
                    await client_to_close.aclose()
                complete = written >= 1024 and (content_length is None or written == content_length)
                if complete:
                    remember_stream_audio(guid, b"".join(parts), ext)
                else:
                    logger.warning("stream cache skipped guid=%s written=%d complete=%s content_length=%s ext=%s", guid, written, complete, content_length, ext)
                await queue.put(None)

        dl_task = asyncio.create_task(_downloader())

        async def stream_tee() -> AsyncGenerator[bytes, None]:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    break
                yield chunk

        return StreamingResponse(stream_tee(), status_code=status_code, headers=out_headers)

    async def stream_no_cache() -> AsyncGenerator[bytes, None]:
        try:
            async for chunk in resp.aiter_bytes():
                if chunk:
                    yield chunk
        finally:
            await resp.aclose()
            if client_to_close:
                await client_to_close.aclose()

    return StreamingResponse(stream_no_cache(), status_code=status_code, headers=out_headers)


@app.get("/music/api/v1/track/stream")
@app.get("/music/api/v1/track/stream/{subpath:path}")
async def stream_track(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    guid = extract_guid(request)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    range_header = request.headers.get("range")
    memory_body = get_stream_audio(guid)
    if memory_body:
        entry = _STREAM_CACHE.get(guid) or {}
        ext = str(entry.get("ext") or "mp3").lstrip(".") or "mp3"
        return serve_bytes_with_range(memory_body, range_header, media_type_for_ext(ext))

    src = source_from_online_guid(guid)
    if src == "kugou":
        raw_song_id = song_id_from_online_guid(guid)
        play_url, resolved_ext = await resolve_kugou_url(raw_song_id)
        if not play_url:
            return JSONResponse(
                content={"code": 404, "msg": "online source unavailable", "data": None},
                status_code=404,
            )
        req_headers = {"Range": range_header} if range_header else {}
        stream_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        try:
            stream_req = stream_client.build_request("GET", play_url, headers=req_headers)
            resp = await stream_client.send(stream_req, stream=True)
            if resp.status_code >= 400:
                await resp.aclose()
                await stream_client.aclose()
                return JSONResponse(
                    content={"code": 404, "msg": "online source unavailable", "data": None},
                    status_code=404,
                )
        except Exception as e:
            logger.warning("kugou stream failed url=%s guid=%s err=%s", play_url, guid, e)
            await stream_client.aclose()
            return JSONResponse(
                content={"code": 404, "msg": "online source unavailable", "data": None},
                status_code=404,
            )
        return stream_tee_response(
            resp,
            guid=guid,
            range_header=range_header,
            coro_factory=None,
            client_to_close=stream_client,
            resolved_ext=resolved_ext or "mp3",
            pre_info=None,
        )
    # 非 kugou 源（历史 netease 条目等）不再支持，直接 404
    return JSONResponse(
        content={"code": 404, "msg": "online source unavailable", "data": None},
        status_code=404,
    )


@app.get("/music/api/v1/track/hls/{guid}/preset.m3u8")
@app.get("/music/api/v1/track/hls/{guid}/{filename}")
async def track_hls(request: Request, guid: str, filename: str = "preset.m3u8"):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    info = await _online_info(request, guid)
    duration_s = 0
    if info:
        try:
            duration_s = int(float(info.get("duration_s") or 0))
        except (TypeError, ValueError):
            duration_s = 0
    if duration_s <= 0:
        duration_s = 240

    stream_url = f"/music/api/v1/track/stream?guid={quote(guid, safe='')}"
    playlist = (
        "#EXTM3U\n"
        "#EXT-X-VERSION:3\n"
        f"#EXT-X-TARGETDURATION:{max(duration_s, 1)}\n"
        "#EXT-X-PLAYLIST-TYPE:VOD\n"
        "#EXT-X-MEDIA-SEQUENCE:0\n"
        f"#EXTINF:{duration_s:.3f},\n"
        f"{stream_url}\n"
        "#EXT-X-ENDLIST\n"
    )
    return Response(content=playlist, media_type="application/vnd.apple.mpegurl")


@app.api_route("/music/api/v1/track/transcode/heartbeat", methods=["GET", "POST"])
@app.api_route("/music/api/v1/track/transcode/quit", methods=["GET", "POST"])
async def track_transcode_session(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    guid = await extract_guid_from_body(request)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    return JSONResponse(content={"code": 0, "msg": "ok", "data": {"guid": guid}})


@app.api_route("/music/api/v1/track/transcode", methods=["GET", "POST"])
async def track_transcode(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    guid = await extract_guid_from_body(request)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    return JSONResponse(
        content={
            "code": 0,
            "msg": "ok",
            "status": "success",
            "data": {"guid": guid, "status": "ready"},
        }
    )


async def _online_info(request: Request, guid: str) -> dict | None:
    src = source_from_online_guid(guid)
    if src == "kugou":
        raw_song_id = song_id_from_online_guid(guid)
        info = await kugou_source.get_info(raw_song_id) or {}
        info["id"] = f"kugou:{raw_song_id}"
        info["source"] = "kugou"
        cached_lyric = read_lyric_cache(guid)
        if cached_lyric:
            info["lyric"] = cached_lyric
        else:
            lyric_text = await resolve_kugou_lyric(raw_song_id)
            if lyric_text:
                info["lyric"] = lyric_text
                write_lyric_cache(guid, lyric_text)
        return info
    # 非 kugou 源不再支持
    return None


# 歌手图 URL 形如 singerimg.kugou.com/uploadpic/softhead/240/<date>/x.jpg，
# /uploadpic/<子路径>/<像素>/ 末段是尺寸。酷狗 author 搜索的 Avatar 字段
# 固定是 240 图，前端要 size=1024 时得换这段，否则拿到的是模糊原图。
# 封面图 URL 尺寸段。两类酷狗路径，尺寸都在 /uploadpic/<子路径>/ 末段：
#   歌手头像 /uploadpic/avatar2/202404/240/1.jpg
#   专辑封面 /uploadpic/album/240/1234.jpg（或 /uploadpic/img2/album/240/x）
# 请求 size 不同时必须替换，否则封面固定 240。分组 1 = /uploadpic/<子路径>/，
# 分组 2 = 结尾 /；不要写成 (uploadpic|album) 的并列备选——upload 里含 /a 后接 lbum
# 时不会误配，但子路径含 /album/ 的专辑封面会被错配成 /album 分支。
_SINGER_SIZE_RE = re.compile(r"(/uploadpic/[^/]+)/\d+(/)")

# 路径段形式的具体尺寸，如 /stdmusic/160/、/custom/600/、/uploadpic/softhead/160/。
# 2-4 位数字兼避 6 位日期段（/uploadpic/avatar2/202404/240/ 里的 202404 不会误换）。
_KUGOU_SIZE_SEG_RE = re.compile(
    r"(/(?:stdmusic|custom|soft/collection|uploadpic/[^/]+)/)\d{2,4}(?=/)"
)

# album/v8/<id>_<N>.jpg 后缀形式。
_KUGOU_ALBUM_SUF_RE = re.compile(r"(album/v8/\d+)_\d+\.jpg")




def _fill_cover_size(cover: str, request: Request) -> str:
    if not cover:
        return ""
    size = request.query_params.get("size") or "240"
    try:
        size_s = str(int(size)).strip()
    except (TypeError, ValueError):
        size_s = "240"
    cover = cover.replace("{size}", size_s).replace("{SIZE}", size_s)
    def _sub_cover_size(m):
        # group(1)="/uploadpic/<子路径>" 不含尾斜杠，group(2)="/"。
        # 必须补上 group1 与 size 之间的斜杠，否则得到 /uploadpic/softhead1024/
        # 这种坏 URL（生产日志已出现，size=1024 的歌手封面全 404）。
        return f"{m.group(1)}/{size_s}{m.group(2)}"

    cover = _SINGER_SIZE_RE.sub(_sub_cover_size, cover, count=1)
    return cover


def _fill_cover_size_for_upload(cover: str, request: Request) -> str:
    """按 cover_upload_size 填尺寸占位符，用于「为换 coverId 回源拉大图」。

    前端请求的 size 通常只有 160/240，拿那张小图去上传换官方 coverId，
    入库的就是缩略图。这里改成按配置的分辨率回源。

    尺寸在酷狗 URL 里有三种形态，必须都覆盖：
    1. {size}/{SIZE} 占位符（代理内部缓存的原模板）
    2. 路径段：/stdmusic/160/、/custom/600/、/uploadpic/softhead/160/
       —— 这是主力形态，旧正则只匹配 uploadpic 一种，其余全部失配
    3. 后缀：album/v8/<id>_<N>.jpg
    匹配不到的 URL（albkmid、music163 等本身不带尺寸）原样返回，由调用方走降级。
    0 表示不缩放。
    """
    target = int(CONF.get("cover_upload_size") or 0)
    if not cover:
        return ""
    if not target:
        return _fill_cover_size(cover, request)
    size_s = str(target)
    out = cover.replace("{size}", size_s).replace("{SIZE}", size_s)
    # 要求 2-4 位数字：既覆盖 160/600/1024，也天然避开 6 位日期段（如
    # /uploadpic/avatar2/202404/240/ 里的 202404 不会被误当尺寸替换）。
    out = _KUGOU_SIZE_SEG_RE.sub(lambda m: f"{m.group(1)}{size_s}", out, count=1)
    out = _KUGOU_ALBUM_SUF_RE.sub(lambda m: f"{m.group(1)}_{size_s}.jpg", out, count=1)
    return out


async def _resize_cover_to_upload_size(image_bytes: bytes) -> bytes:
    """本地 sidecar 封面缩到 cover_upload_size 宽度后再上传。

    酷狗 URL 分支能直接回源 1600，但本地音频同目录的 .jpg/.png 是静态文件，
    尺寸由原始导入决定，只能在这里转。系统 ffmpeg 可用且代理 venv 里没有
    Pillow，所以走 subprocess + asyncio.to_thread，不阻塞事件循环。
    目标尺寸已达标 / 缩放失败 / ffmpeg 不存在时一律原样返回，宁可传小图
    也不能因为缩放把封面弄丢。
    """
    target = int(CONF.get("cover_upload_size") or 0)
    if not target or not image_bytes or len(image_bytes) < 16 * 1024:
        return image_bytes
    if not shutil.which("ffmpeg"):
        return image_bytes
    # 先探测原图宽度：宽度已 ≤ 目标则直接返回原图。放大只会糊，不划算。
    tmp_in = ""
    try:
        with tempfile.NamedTemporaryFile(prefix="covsrc_", suffix=".img", delete=False) as f:
            f.write(image_bytes)
            tmp_in = f.name
        # _probe_image_width 是同步函数，不能 await；误写 await 会抛
        # TypeError: object int can't be used in 'await' expression，被下面的
        # except 吞掉后静默返回原图（看起来像「缩放不生效」）。
        w = _probe_image_width(tmp_in)
        if 0 < w <= target:
            return image_bytes
        tmp_out = tmp_in + ".out.jpg"
        await asyncio.to_thread(
            subprocess.run,
            ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
             "-i", tmp_in, "-vf", f"scale={target}:-2", "-q:v", "2", tmp_out],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=20,
        )
        if not os.path.exists(tmp_out) or os.path.getsize(tmp_out) <= 0:
            return image_bytes
        with open(tmp_out, "rb") as f:
            out = f.read()
        if not out or len(out) > int(CONF.get("cover_upload_max_bytes") or 4 * 1024 * 1024):
            return image_bytes
        logger.warning("[COVERRESIZE] %dx→%d宽 bytes=%d->%d", w, target, len(image_bytes), len(out))
        return out
    except Exception as e:
        logger.warning("[COVERRESIZE] err=%s:%s", type(e).__name__, e)
        return image_bytes
    finally:
        # tmp_in 为空时不要退化成相对路径 ".out.jpg"，否则会误删 cwd 下的同名文件。
        if tmp_in:
            for p in (tmp_in, tmp_in + ".out.jpg"):
                try:
                    os.unlink(p)
                except OSError:
                    pass


def _probe_image_width(path: str) -> int:
    """用 ffprobe/ffmpeg 读图宽；失败返 0，由调用方决定降级。"""
    p = shutil.which("ffprobe")
    if p:
        try:
            r = subprocess.run(
                [p, "-v", "error", "-select_streams", "v:0",
                 "-show_entries", "stream=width", "-of", "default=nk=1:nw=1", path],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=10)
            return int(str(r.stdout.decode()).strip() or 0)
        except Exception:
            return 0
    try:
        r = subprocess.run(
            ["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", path,
             "-f", "null", "-"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        m = re.search(r"(\d+)x(\d+)", r.stderr.decode(errors="replace"))
        return int(m.group(1)) if m else 0
    except Exception:
        return 0


async def _fetch_kugou_playlist_cover_url(request: Request, coll_id: str) -> str:
    """用歌单 ID 回源酷狗，取歌单详情里的 pic 封面。

    [KUGOU_PLCOVER] 诊断日志：记录请求耗时、HTTP 状态、items 数量与 pic 原值，
    用于区分“接口没数据”“有数据但 pic 字段为空”两种情况。
    """
    if not coll_id:
        logger.warning("[KUGOU_PLCOVER] step6.5 detail coll_id=EMPTY")
        return ""
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(base_url=CONF["kugou_url"], timeout=float(CONF["kugou_search_timeout"]), follow_redirects=True) as c:
            auth = kugou_source._auth_header()
            headers = {"Authorization": auth} if auth else {}
            r = await c.get("/playlist/detail", params={"ids": coll_id}, headers=headers)
            if r.status_code != 200:
                logger.warning("[KUGOU_PLCOVER] step6.5 detail NOT200 http=%s elapsed=%.2fs coll_id=%s",
                               r.status_code, time.monotonic() - t0, coll_id)
                return ""
            data = r.json()
            items = data.get("data") or []
            logger.warning("[KUGOU_PLCOVER] step6.5 detail OK status=%s error_code=%s items=%s elapsed=%.2fs coll_id=%s",
                           data.get("status"), data.get("error_code"),
                           len(items) if isinstance(items, list) else type(items).__name__,
                           time.monotonic() - t0, coll_id)
            if isinstance(items, list) and items:
                first = items[0] if isinstance(items[0], dict) else {}
                pic = str(first.get("pic") or first.get("cover") or "").strip()
                logger.warning("[KUGOU_PLCOVER] step6.5 pic name=%s pic=%s", first.get("name"), pic[:120])
                if pic:
                    return _fill_cover_size(pic, request)
                logger.warning("[KUGOU_PLCOVER] step6.5 pic EMPTY keys=%s", sorted(first.keys()))
            else:
                logger.warning("[KUGOU_PLCOVER] step6.5 detail EMPTY_DATA coll_id=%s data=%r", coll_id, data)
    except httpx.TimeoutException as e:
        logger.warning("[KUGOU_PLCOVER] step6.5 detail TIMEOUT %s elapsed=%.2fs coll_id=%s",
                       type(e).__name__, time.monotonic() - t0, coll_id)
    except Exception as e:
        logger.warning("[KUGOU_PLCOVER] step6.5 detail EXCEPTION %s:%s elapsed=%.2fs coll_id=%s",
                       type(e).__name__, e, time.monotonic() - t0, coll_id)
    return ""


async def _fetch_kugou_artist_cover_url(request: Request, artist_id: str) -> str:
    """用歌手 ID 回源酷狗，取歌手详情里的 sizable_avatar 封面。"""
    if not artist_id:
        return ""
    try:
        async with httpx.AsyncClient(base_url=CONF["kugou_url"], timeout=float(CONF["kugou_search_timeout"]), follow_redirects=True) as c:
            auth = kugou_source._auth_header()
            headers = {"Authorization": auth} if auth else {}
            r = await c.get("/artist/detail", params={"id": artist_id}, headers=headers)
            if r.status_code != 200:
                logger.warning("[KUGOU_ARTIST_COVER] detail http=%s artist_id=%s", r.status_code, artist_id)
                return ""
            data = r.json()
            payload = data.get("data") if isinstance(data, dict) else None
            if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                payload = payload[0]
            if not isinstance(payload, dict):
                return ""
            cover = str(payload.get("sizable_avatar") or payload.get("avatar") or payload.get("pic") or "").strip()
            if cover:
                return _fill_cover_size(cover, request)
    except Exception as e:
        logger.warning("[KUGOU_ARTIST_COVER] detail error artist_id=%s err=%s", artist_id, e)
    return ""


async def _fetch_kugou_album_cover_url(request: Request, album_id: str) -> str:
    """用专辑 ID 直接调酷狗 /album/detail 取 sizable_cover。"""
    if not album_id:
        return ""
    try:
        detail = await kugou_source.get_album_detail(album_id)
    except Exception as e:
        logger.warning("[KUGOU_ALBUM_COVER] album/detail error album_id=%s err=%s", album_id, e)
        return ""
    if not detail:
        logger.warning("[KUGOU_ALBUM_COVER] album/detail empty album_id=%s", album_id)
        return ""
    cover = str(detail.get("sizable_cover") or detail.get("coverUrl") or detail.get("Image") or "").strip()
    if cover:
        return _fill_cover_size(cover, request)
    logger.warning("[KUGOU_ALBUM_COVER] no cover field album_id=%s keys=%s",
                   album_id, sorted(detail.keys()))
    return ""


# 本地歌手封面：/artist/detail 取名字 -> 酷狗 author 搜索取 Avatar。
# 本地歌手 guid 无 online: 前缀，此前落入"按曲目解析"分支（/track/metadata
# 查不到 title，几十毫秒就写空串失败缓存并透传上游），歌手头像位空白。


async def _local_artist_name_by_guid(request: Request, guid: str) -> str | None:
    """取本地歌手名字：调飞牛 /artist/detail?guid=<g> 读 data.name。

    调用方已靠 coverId 的 artist: 前缀确定性判定过实体类型，这里只负责取
    名字，不再用返回体字段（trackCount 等）判定是否歌手——歌手和专辑详情
    都可能带 trackCount，字段判定不可靠且会让无歌歌手（有详情无曲目）丢封面。
    取不到名字（非本地/接口失败/无 name）返回 None。

    注意必须用 fetch_upstream_envelope_at 指定路径：fetch_upstream_envelope
    透传当前请求路径，在 /static/cover 处理中调用它会去请求 /static/cover，
    上游返回图片流、JSON 解析失败，名字永远取不到。
    """
    if not guid or is_online_guid(guid) or is_kugou_playlist_guid(guid):
        return None
    try:
        envelope = await fetch_upstream_envelope_at(
            request, get_upstream_client(request.app),
            "/music/api/v1/artist/detail", {"guid": guid},
        )
        if isinstance(envelope, Response):
            return None
        if envelope.get("code") != 0:
            return None
        data = envelope.get("data")
        if not isinstance(data, dict):
            return None
        name = str(data.get("name") or data.get("artist") or data.get("singer") or "").strip()
        if not name:
            return None
        # 不再用 trackCount 判定是否歌手：调用方已经靠 coverId 前缀确定性
        # 判定过实体类型，这里只需取名字。trackCount<=0 的歌手（有详情无歌）
        # 也有头像，之前会被误判为"不是歌手"导致封面空白。
        return name
    except Exception as e:
        logger.warning("[LOCAL_ARTIST] detail error guid=%s err=%s", guid, e)
        return None


async def _kugou_author_avatar_by_name(name: str, page: int = 1, limit: int = 30) -> str:
    """用歌手名搜酷狗，返回歌手头像 URL。

    数据源 KuGouMusicApi /search?keywords=<歌手名>&type=author。
    精确同名优先；无精确同名时返回酷狗原始排序的第一名（可能挂到同名的
    不同歌手，宁可模糊命中也不留空白）。返回未填尺寸的原图，尺寸由调用方
    按请求参数统一替换。
    """
    kw = str(name or "").strip()
    if not kw:
        return ""
    try:
        async with kugou_source._client() as c:
            r = await c.get(
                "/search",
                params={"keywords": kw, "page": page, "pagesize": limit, "type": "author"},
            )
            if r.status_code != 200:
                logger.warning("[KUGOU_ARTIST_AVATAR] http=%s kw=%r", r.status_code, kw)
                return ""
            body = r.json()
            data = body.get("data") or {} if isinstance(body, dict) else {}
            lists = data.get("lists") or [] if isinstance(data, dict) else []
            if not isinstance(lists, list):
                return ""
    except Exception as e:
        logger.warning("[KUGOU_ARTIST_AVATAR] error kw=%r err=%s", kw, e)
        return ""

    first = ""
    for it in lists:
        if not isinstance(it, dict):
            continue
        author = str(it.get("AuthorName") or it.get("author_name") or "").strip()
        avatar = str(it.get("Avatar") or it.get("avatar") or it.get("img") or "").strip()
        if not author or not avatar:
            continue
        if author == kw:
            return avatar
        if not first:
            first = avatar
    if first:
        logger.warning("[KUGOU_ARTIST_AVATAR] no exact-name match kw=%r, using first result", kw)
    return first


async def _kugou_album_cover_url_by_name(name: str, page: int = 1, limit: int = 30) -> str:
    """用专辑名搜酷狗，返回专辑封面 URL。

    数据源 KuGouMusicApi /search?keywords=<专辑名>&type=album，返回字段 img
    是完整 240 URL（无 {size} 占位）。精确同名优先；无精确同名时返回第一名，
    宁可模糊命中也不留空白。返回未填尺寸的原图。
    """
    kw = str(name or "").strip()
    if not kw:
        return ""
    result = await kugou_source.search_albums_raw(kw, limit=limit, page=page)
    lists = result.get("lists") if isinstance(result, dict) else None
    if not isinstance(lists, list):
        return ""
    first = ""
    for it in lists:
        if not isinstance(it, dict):
            continue
        album_name = str(it.get("albumname") or it.get("album_name") or "").strip()
        img = str(it.get("img") or it.get("cover") or it.get("album_cover") or "").strip()
        if not album_name or not img:
            continue
        if album_name == kw:
            return img
        if not first:
            first = img
    if first:
        logger.warning("[KUGOU_ALBUM_COVER] no exact-name match kw=%r, using first result", kw)
    return first


async def _local_album_name_by_guid(request: Request, guid: str) -> str | None:
    """guid 若是飞牛本地专辑：返回专辑名字；取不到返回 None。"""
    if not guid or is_online_guid(guid) or is_kugou_playlist_guid(guid):
        return None
    try:
        envelope = await fetch_upstream_envelope_at(
            request, get_upstream_client(request.app),
            "/music/api/v1/album/detail", {"guid": guid},
        )
        if isinstance(envelope, Response):
            return None
        if envelope.get("code") != 0:
            return None
        data = envelope.get("data")
        if not isinstance(data, dict):
            return None
        name = str(data.get("name") or data.get("album") or data.get("albumName") or "").strip()
        return name or None
    except Exception as e:
        logger.warning("[LOCAL_ALBUM] detail error guid=%s err=%s", guid, e)
        return None


async def _resolve_local_entity_cover_url(request: Request, guid: str, kind: str) -> str:
    """本地实体（歌手/专辑）封面解析。

    coverId 带 artist:/album: 前缀时才走到这里，实体类型已确定性判定，
    不再靠返回体字段猜。链路：上游 /{kind}/detail 取名字 -> 酷狗
    /search?type={author|album} 取头像/封面。返回未填尺寸的原图，
    尺寸由调用方按本次请求的 size 统一替换。
    """
    if not guid:
        return ""
    if not CONF.get("kugou_enabled", True):
        return ""
    t0 = time.monotonic()
    if kind == "artist":
        name = await _local_artist_name_by_guid(request, guid)
        if not name:
            logger.warning("[STATIC_COVER] entity branch=local_artist no_name guid=%s elapsed=%.2fs",
                           guid, time.monotonic() - t0)
            return ""
        url = await _kugou_author_avatar_by_name(name, page=1, limit=30)
        logger.warning("[STATIC_COVER] entity branch=local_artist guid=%s name=%r avatar=%s elapsed=%.2fs",
                       guid, name, url[:80] if url else "NO", time.monotonic() - t0)
        return url
    if kind == "album":
        name = await _local_album_name_by_guid(request, guid)
        if not name:
            logger.warning("[STATIC_COVER] entity branch=local_album no_name guid=%s elapsed=%.2fs",
                           guid, time.monotonic() - t0)
            return ""
        url = await _kugou_album_cover_url_by_name(name, page=1, limit=30)
        logger.warning("[STATIC_COVER] entity branch=local_album guid=%s name=%r cover=%s elapsed=%.2fs",
                       guid, name, url[:80] if url else "NO", time.monotonic() - t0)
        return url
    return ""


async def _fetch_cover_url_by_guid(request: Request, guid: str) -> str:
    """按 GUID 取封面：在线直取在线信息，酷狗歌单取 pic，酷狗专辑查详情，本地回退到酷狗搜索。"""
    if not guid:
        return ""
    # [COVERURL] 诊断日志：step6 解析封面 URL，先记录走了哪个分支再拿结果
    if is_kugou_playlist_guid(guid):
        coll_id = kugou_playlist_id_from_guid(guid)
        if coll_id:
            t0 = time.monotonic()
            url = await _fetch_kugou_playlist_cover_url(request, coll_id)
            logger.warning("[COVERURL] step6 branch=kugou_playlist coll_id=%s got=%s elapsed=%.2fs guid=%s",
                           coll_id, "yes" if url else "NO", time.monotonic() - t0, guid)
            return url
        logger.warning("[COVERURL] step6 branch=kugou_playlist coll_id=EMPTY guid=%s", guid)
        return ""
    if isinstance(guid, str) and guid.startswith("online:kugou:album:"):
        t0 = time.monotonic()
        url = await _fetch_kugou_album_cover_url(request, guid[len("online:kugou:album:"):])
        logger.warning("[COVERURL] step6 branch=kugou_album id=%s got=%s elapsed=%.2fs",
                       guid[len("online:kugou:album:"):], "yes" if url else "NO", time.monotonic() - t0)
        return url
    if isinstance(guid, str) and guid.startswith("online:kugou:artist:"):
        t0 = time.monotonic()
        url = await _fetch_kugou_artist_cover_url(request, guid[len("online:kugou:artist:"):])
        logger.warning("[COVERURL] step6 branch=kugou_artist id=%s got=%s elapsed=%.2fs",
                       guid[len("online:kugou:artist:"):], "yes" if url else "NO", time.monotonic() - t0)
        return url
    if is_online_guid(guid):
        t0 = time.monotonic()
        data = await _online_info(request, guid)
        raw = str((data or {}).get("cover_url") or "")
        url = _fill_cover_size(raw, request)
        logger.warning("[COVERURL] step6 branch=online cover_url=%s filled=%s elapsed=%.2fs guid=%s",
                       raw[:80], "yes" if url else "NO", time.monotonic() - t0, guid)
        return url
    # 本地歌手/专辑封面不再走这里：coverId 带 entity: 前缀，由 static_cover
    # 主路由的 step2-entity 分支确定性处理。此处只处理裸 guid（本地曲目）。
    t0 = time.monotonic()
    url = await _resolve_local_static_cover_url(request, guid)
    logger.warning("[COVERURL] step6 branch=local got=%s elapsed=%.2fs guid=%s",
                   "yes" if url else "NO", time.monotonic() - t0, guid)
    return url


async def _resolve_local_static_cover_url(request: Request, guid: str) -> str:
    """本地 coverId=guid 的封面兜底。

    顺序：进程内缓存 → 本地同目录封面文件 → 音频标签元数据 → 搜酷狗挑同款版本取封面。
    结果（含失败）都写入进程内缓存，避免每张封面都回源 metadata + 酷狗搜索。
    """
    if not guid or is_online_guid(guid):
        return ""
    cached = remembered_local_cover_url(guid)
    if cached is not None:
        logger.warning("[STATIC_COVER] cache hit guid=%s has_url=%s", guid, bool(cached))
        return cached

    if not CONF.get("kugou_enabled", True):
        return ""

    client = get_upstream_client(request.app)
    title, artist, duration = await _local_audio_metadata(request, client, guid)

    # 上游 metadata 常拿不到标签信息，回退到本地音频文件标签与文件名
    audio_path = find_cache_file(guid)
    if (not title or not artist) and audio_path:
        tag_title, tag_artist = _tags_from_local_audio(audio_path)
        title = title or tag_title
        artist = artist or tag_artist
    if audio_path and not duration:
        try:
            from mutagen import File as MutagenFile
            audio = MutagenFile(audio_path, easy=False)
            if audio is not None:
                duration = float(getattr(audio.info, "length", 0) or 0)
        except Exception:
            pass

    logger.warning(
        "[STATIC_COVER] resolve guid=%s title=%r artist=%r duration=%s audio=%s",
        guid, title, artist, duration, audio_path,
    )
    if not (title and artist):
        remember_local_cover_url(guid, "")
        return ""

    keyword = " ".join(x for x in [title, artist] if x).strip()
    items = await _kugou_candidates(keyword, limit=10)
    if not items:
        remember_local_cover_url(guid, "")
        return ""

    scored = [
        (_kugou_item_matches_local(item, title, artist, duration), item)
        for item in items
    ]
    scored.sort(key=lambda x: x[0][:3], reverse=True)
    chosen = None
    best_key = None
    for key, item in scored:
        score, _, _, matched, diff, cand_title, cand_artist = key
        if not matched:
            continue
        # 必须真同款：时长差 ≤5s 才算版本一致，避免封面挂到别的首版本上
        if duration > 0 and diff > 5.0:
            continue
        chosen = item
        best_key = (score, diff, cand_title, cand_artist)
        break

    if chosen is None:
        top = items[0]
        cover = _cover_url_from_item(top)
        logger.warning(
            "[STATIC_COVER] no matched version, falling back to top search result guid=%s cand_title=%r cand_artist=%r cover=%s",
            guid, top.get("title"), top.get("artist"), cover[:80] if cover else "",
        )
    else:
        cover = _cover_url_from_item(chosen)
        logger.warning(
            "[STATIC_COVER] matched version guid=%s cand_title=%r cand_artist=%r dur_diff=%s cover=%s",
            guid, best_key[2], best_key[3], best_key[1], cover[:80] if cover else "",
        )

    if not cover:
        remember_local_cover_url(guid, "")
        return ""
    # 返回未替换 {size} 的原始模板，由路由层按本次请求尺寸填；缓存里也只存模板
    return cover


async def _resolve_static_cover_guid(request: Request, subpath: str) -> tuple[str, Response | None]:
    """解析封面 GUID；遇到鉴权失败直接返回错误响应。

    coverId 可能是本地实体封面前缀格式（artist:<guid> / album:<guid>），
    含 ':' 会被 URL 百分号编码。query 优先走 extract_guid（已由 Starlette
    解码），空则回退到路径形式；路径形式需手动 unquote，否则前缀格式与
    online:kugou:* 都会拿不到。
    """
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not guid and subpath:
        unquoted = unquote(subpath)
        guid = unquoted if is_online_guid(unquoted) else ""
    return guid, None


async def _fetch_cover_image_response(url: str) -> Response | None:
    """COEP 拦截 302 跨域跳转，改为服务端代理拉取图片后同源流式返回。

    磁盘缓存：cache/<sha256(url)[:32]>.cover，24h TTL，回源成功后落盘。
    同一张列表缩略图 URL 对同一歌曲恒定不变，无缓存时被下载十几次；
    缓存后只让首次请求打网络，后续全走本地，批量刷新时的并发超时消失。
    """
    t0 = time.monotonic()
    cached = _cover_disk_get(url)
    if cached is not None:
        return cached
    return await _fetch_cover_image_origin(url, t0)


async def _fetch_cover_image_origin(url: str, t0: float) -> Response | None:
    """纯网络回源：共享连接池 + connect 3s / read 8s + 超时重试一次。

    成功后写磁盘缓存；失败返回 None，由上层决定兜底。
    """
    try:
        c = await _cover_http()
        for attempt in range(2):
            try:
                cr = await c.get(url)
                break
            except httpx.TimeoutException as te:
                if attempt == 0:
                    # P0: 重试一次。日志里 21:07:22 那批同域同路径全超时，
                    # 但 21:07:26 同域一张就成功了 —— 是瞬时拥塞不是 CDN 挂。
                    await asyncio.sleep(0.3)
                    continue
                logger.warning(
                    "[COVERIMG] step7-fetch TIMEOUT %s:%s elapsed=%.2fs url=%s",
                    type(te).__name__, te, time.monotonic() - t0, url,
                )
                return None
    except httpx.TimeoutException as e:
        logger.warning(
            "[COVERIMG] step7-fetch TIMEOUT %s:%s elapsed=%.2fs url=%s",
            type(e).__name__, e, time.monotonic() - t0, url,
        )
        return None
    except Exception as e:
        logger.warning(
            "[COVERIMG] step7-fetch EXCEPTION %s:%s elapsed=%.2fs url=%s",
            type(e).__name__, e, time.monotonic() - t0, url,
        )
        return None

    if cr is None or cr.status_code != 200 or not cr.content:
        logger.warning(
            "[COVERIMG] step7-fetch NOT200 http=%s len=%s elapsed=%.2fs url=%s",
            getattr(cr, "status_code", None), len(getattr(cr, "content", b"") or b""),
            time.monotonic() - t0, url,
        )
        return None

    ct = cr.headers.get("content-type") or "image/jpeg"
    _cover_disk_put(url, cr.content)
    _cover_disk_cleanup()
    logger.warning(
        "[COVERIMG] step7-fetch http=200 ok bytes=%d ct=%s elapsed=%.2fs url=%s",
        len(cr.content), ct, time.monotonic() - t0, url,
    )
    return Response(
        content=cr.content,
        status_code=200,
        media_type=ct,
        headers={
            "Cache-Control": "public, max-age=86400",
            "Cross-Origin-Resource-Policy": "same-origin",
        },
    )


@app.get("/music/api/v1/lyric/list")
@app.get("/music/api/v1/lyric/list/{subpath:path}")
async def lyric_list(request: Request, subpath: str = ""):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not is_online_guid(guid):
        cached = get_stream_lyric(guid)
        if cached:
            logger.warning("[LYRIC_FALLBACK] lyric/list cache hit guid=%s len=%d", guid, len(cached))
            return JSONResponse(content=build_lyric_list_payload(guid, cached))

        client = get_upstream_client(request.app)
        payload_or_resp = await fetch_upstream_envelope(request, client)
        if isinstance(payload_or_resp, Response):
            return payload_or_resp
        payload = payload_or_resp
        headers = payload.pop("_ext_headers", {})
        title, artist, duration = _extract_local_lyric_meta(payload)
        existing = _existing_local_lyric(payload)
        if existing:
            write_lyric_cache(guid, existing)
            logger.warning("[LYRIC_FALLBACK] lyric/list existing lyric found guid=%s title=%r artist=%r len=%d", guid, title, artist, len(existing))
            return JSONResponse(content=build_lyric_list_payload(guid, existing), headers=headers or None)
        if title and artist:
            lyric_text = await fetch_local_lyric_by_keywords(title, artist, duration)
            if lyric_text:
                write_lyric_cache(guid, lyric_text, title=title, artist=artist)
                logger.warning("[LYRIC_FALLBACK] lyric/list fallback fetched guid=%s title=%r artist=%r len=%d", guid, title, artist, len(lyric_text))
                return JSONResponse(content=build_lyric_list_payload(guid, lyric_text), headers=headers or None)
        lyric_text = await fetch_local_lyric_for_guid(request, client, guid)
        if lyric_text:
            return JSONResponse(content=build_lyric_list_payload(guid, lyric_text), headers=headers or None)
        logger.warning("[LYRIC_FALLBACK] lyric/list no lyric text guid=%s title=%r artist=%r duration=%s", guid, title, artist, duration)
        return JSONResponse(content=build_lyric_list_payload(guid, ""), headers=headers or None)

    lyric_text = await resolve_online_lyric(request, guid)
    return JSONResponse(content=build_lyric_list_payload(guid, lyric_text))


@app.get("/music/api/v1/track/lyrics")
@app.get("/music/api/v1/track/lyrics/{subpath:path}")
@app.get("/music/api/v1/detail/lyrics/{subpath:path}")
async def track_lyrics(request: Request, subpath: str = ""):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not is_online_guid(guid):
        return await forward_upstream_with_local_lyric_fallback(request, get_upstream_client(request.app))

    lyric_text = await resolve_online_lyric(request, guid)
    if lyric_text:
        res = {"code": 0, "msg": "ok", "data": {"guid": guid, "lyric": lyric_text}}
        set_by_path(res, CONF["lyric_field"], lyric_text)
        return JSONResponse(content=res)
    return empty_ok()


@app.get("/music/api/v1/track/metadata")
@app.get("/music/api/v1/track/metadata/{subpath:path}")
@app.get("/music/api/v1/track/audio-info")
async def track_metadata(request: Request, subpath: str = ""):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not is_online_guid(guid):
        # 本地曲目：透传后补 data.track.coverId（优先官方 coverId，未换到则用 guid）。
        # 上传与回写由 static_cover 在拿到封面 bytes 时触发，此处不重复回源。
        return await forward_upstream_with_local_lyric_fallback(
            request, get_upstream_client(request.app), local_cover_guid=guid
        )

    data = await _online_info(request, guid) or stub_online_info(guid)
    cached_lyric = read_lyric_cache(guid)
    if cached_lyric:
        data = {**data, "lyric": cached_lyric}
    elif data.get("lyric"):
        write_lyric_cache(
            guid,
            str(data.get("lyric") or ""),
            title=str(data.get("title") or ""),
            artist=str(data.get("artist") or ""),
        )
    return JSONResponse(content=build_metadata_payload(guid, data))


@app.get("/music/api/v1/search/playlist")
@app.get("/music/api/v1/search/playlist/{subpath:path}")
async def search_playlist(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    """/search/playlist：酷狗专题歌单搜索；空关键词走飞牛上游。

    飞牛请求形如 /music/api/v1/search/playlist?q=%E6%B5%8B%E8%AF%95&page=1&size=24。
    数据源：KuGouMusicApi /search?keywords=<kw>&page=&pagesize=&type=special。

    响应结构对齐飞牛原生 /search/playlist（{"code":0,"data":{"list":[...],"total":n}}），
    playlist 项字段：guid/name/coverId/createdAt/updatedAt/trackCount/score。
    本地与酷狗结果合并，官方在前、酷狗在后。
    """
    return await merged_search_meta(request, fetch_kugou_playlist_search, tag="playlist")

@app.api_route("/music/api/v1/static/cover", methods=["GET", "HEAD"])
@app.api_route("/music/api/v1/static/cover/{subpath:path}", methods=["GET", "HEAD"])
async def static_cover(request: Request, subpath: str = ""):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    # 代理上传换到的官方 coverId（track_<hash> / artist_<hash> / album_<hash> /
    # playlist_<hash>）已回写飞牛数据库，后续封面请求必须直接透传上游拿官方封面，
    # 不能用它当本地 guid 去搜酷狗（会拿假 guid 搜不到、白回源）。
    _probe_guid = extract_guid(request, subpath if is_online_guid(subpath) else None) or unquote(subpath or "")
    if _probe_guid and _probe_guid.startswith(("track_", "artist_", "album_", "playlist_")):
        logger.warning("[COVERDB] step0-official rid=%s coverId=%s -> upstream", uuid4().hex[:8], _probe_guid[:60])
        return await forward_to_upstream(request, get_upstream_client(request.app))
    # [COVCDB] 封面链路诊断日志。同一个 rid 串起一次请求的所有步骤。
    # 排查完后整体删除即可（搜索 COVCDB / COVERURL / COVERIMG / COVERFALLBACK）。
    rid = uuid4().hex[:8]
    t_all = time.monotonic()
    logger.warning("[COVCDB] step0-req rid=%s path=%s query=%s", rid, request.url.path, dict(request.query_params))
    guid, auth_resp = await _resolve_static_cover_guid(request, subpath)
    if auth_resp is not None:
        logger.warning("[COVCDB] step1-auth rid=%s AUTH_FAILED", rid)
        return auth_resp
    if not guid:
        logger.warning("[COVCDB] step1-guid rid=%s GUID_EMPTY -> upstream", rid)
        return await forward_to_upstream(request, get_upstream_client(request.app))
    logger.warning("[COVCDB] step1-guid rid=%s guid=%s is_online=%s is_kugou_playlist=%s",
                   rid, guid, is_online_guid(guid), is_kugou_playlist_guid(guid))

    # 本地实体封面（artist:<guid> / album:<guid>）必须先于 sidecar 与本地曲目
    # 分支判定：实体 guid 与本地曲目 guid 都是 32 位 hex，同构，一旦漏到
    # 下面会用音频文件目录去找歌手头像 / 专辑封面。前缀是确定性标记。
    if is_local_artist_cover_id(guid) or is_local_album_cover_id(guid):
        entity_guid = (local_artist_guid_from_cover_id(guid)
                       if is_local_artist_cover_id(guid)
                       else local_album_guid_from_cover_id(guid))
        entity_kind = "artist" if is_local_artist_cover_id(guid) else "album"
        logger.warning("[COVCDB] step2-entity rid=%s ENTITY_%s entity_guid=%s",
                       rid, entity_kind.upper(), entity_guid)
        entity_url = await _resolve_local_entity_cover_url(request, entity_guid, kind=entity_kind)
        if not entity_url:
            logger.warning("[COVCDB] step2-entity rid=%s ENTITY_%s NO_URL elapsed=%.2fs -> upstream",
                           rid, entity_kind.upper(), time.monotonic() - t_all)
            return await forward_to_upstream(request, get_upstream_client(request.app))
        # 返回未填尺寸的原图，缓存模板后再按本次请求的 size 填（歌手图
        # /uploadpic/<p>/<N>/ 与 album/{size} 两种模板都会同步替换）。
        remember_local_cover_url(guid, entity_url)
        entity_url = _fill_cover_size(entity_url, request)
        logger.warning("[COVCDB] step4-url rid=%s url=%s size=%s elapsed=%.2fs",
                       entity_url[:100], request.query_params.get("size"), time.monotonic() - t_all)
        response = await _fetch_cover_image_response(entity_url)
        if response is not None:
            logger.warning("[COVCDB] step7-ok rid=%s bytes=%d total=%.2fs",
                           rid, len(response.body), time.monotonic() - t_all)
            return response
        logger.warning("[COVCDB] step7-fail rid=%s NO_IMAGE url=%s total=%.2fs -> upstream",
                       rid, entity_url[:100], time.monotonic() - t_all)
        return await forward_to_upstream(request, get_upstream_client(request.app))

    # 本地曲目若同目录已有封面文件，直接同源返回，不必回源 metadata 与酷狗搜索
    if not is_online_guid(guid) and not is_kugou_playlist_guid(guid):
        sidecar = _local_sidecar_art_response(guid)
        if sidecar is not None:
            # 仅本地曲目触发上传换官方 coverId；函数内部已检查上游
            # track.coverId 是否为空，已有封面时不会产生任何写入。
            # sidecar 是本地音频同目录的静态文件，size 参数对它不生效；
            # 磁盘上存在 160px 这类小图，所以上传前先按 cover_upload_size 缩放。
            asyncio.ensure_future(_trigger_local_cover_upload(
                request, request.app, guid, None,
                fetch=lambda b=sidecar.body: _resize_cover_to_upload_size(b)))
            logger.warning("[COVCDB] step2-sidecar rid=%s HIT guid=%s bytes=%d",
                           rid, guid, len(sidecar.body))
            return sidecar

    # 本地曲库的 coverId 就是本地 guid（无 online: / playlist: 前缀）。
    # 之前这里一律透传上游，把本地封面兜底写成了死代码；改为先查进程内缓存再走兜底解析。
    cached_tpl = remembered_local_cover_url(guid)
    if cached_tpl is not None:
        cover = _fill_cover_size(cached_tpl, request) if cached_tpl else ""
        if cover:
            logger.warning("[COVCDB] step3-cache rid=%s HIT url=%s elapsed=%.2fs",
                           rid, cover[:90], time.monotonic() - t_all)
        else:
            logger.warning("[COVCDB] step3-cache rid=%s EMPTY_URL -> upstream", rid)
            return await forward_to_upstream(request, get_upstream_client(request.app))
    else:
        cover = await _fetch_cover_url_by_guid(request, guid)
        if cover and not is_online_guid(guid) and not is_kugou_playlist_guid(guid):
            remember_local_cover_url(guid, cover)
            cover = _fill_cover_size(cover, request)
    if not cover:
        logger.warning("[COVCDB] step5-nourl rid=%s NO_URL guid=%s is_online=%s is_kugou_playlist=%s elapsed=%.2fs",
                       rid, guid, is_online_guid(guid), is_kugou_playlist_guid(guid), time.monotonic() - t_all)
        if is_online_guid(guid) or is_kugou_playlist_guid(guid):
            logger.warning("[COVCDB] step6-fallback rid=%s -> fallback(online/kugou_playlist)", rid)
            return await _fetch_cover_fallback_response()
        # 本地封面解析失败则透传上游，尽量拿到飞牛自己扫描到的封面
        logger.warning("[COVCDB] step6-upstream rid=%s -> upstream(local)", rid)
        return await forward_to_upstream(request, get_upstream_client(request.app))
    # 歌单封面模板带 {size} 占位，回源前必须替换成具体尺寸
    if is_kugou_playlist_guid(guid):
        cover = _fill_cover_size(cover, request)
    logger.warning("[COVCDB] step4-url rid=%s url=%s size=%s elapsed=%.2fs",
                   rid, cover[:100], request.query_params.get("size"), time.monotonic() - t_all)
    response = await _fetch_cover_image_response(cover)
    if response is not None:
        # 只有本地曲目（无 online:/歌单前缀）才触发；实体封面走上面
        # step2-entity 分支，不会到这里。
        if not is_online_guid(guid) and not is_kugou_playlist_guid(guid):
            # 前端请求的 size 通常只有 160/240，拿那张小图去换 coverId
            # 会把缩略图写进曲库。这里把「1600 尺寸 URL 抓取器」交给后台任务，
            # 由它在上传前自己拉大图；本请求仍立刻返回已有的小图，不增首屏延迟。
            up_url = _fill_cover_size_for_upload(cover, request)
            if up_url != cover:
                async def _upload_bytes(u=up_url):
                    _r = await _fetch_cover_image_response(u)
                    return _r.body if _r is not None else b""
                asyncio.ensure_future(_trigger_local_cover_upload(
                    request, request.app, guid, None, fetch=_upload_bytes))
            else:
                asyncio.ensure_future(
                    _trigger_local_cover_upload(request, request.app, guid, response.body))
        logger.warning("[COVCDB] step7-ok rid=%s bytes=%d total=%.2fs",
                       rid, len(response.body), time.monotonic() - t_all)
        return response
    logger.warning("[COVCDB] step7-fail rid=%s NO_IMAGE url=%s total=%.2fs -> fallback/upstream",
                   rid, cover[:100], time.monotonic() - t_all)
    if is_online_guid(guid) or is_kugou_playlist_guid(guid):
        logger.warning("[COVCDB] step8-fallback rid=%s -> fallback(online/kugou_playlist)", rid)
        return await _fetch_cover_fallback_response()
    logger.warning("[COVCDB] step8-upstream rid=%s -> upstream(local)", rid)
    return await forward_to_upstream(request, get_upstream_client(request.app))


# === online favorites ===

_FAV_LOCK = asyncio.Lock()


def sanitize_user_guid(guid: str | None) -> str:
    """过滤文件名合法字符 [A-Za-z0-9-_]，非法字符替换为 _；为空则返回 'shared'。"""
    raw = str(guid or "").strip()
    safe = re.sub(r"[^A-Za-z0-9\-_]", "_", raw)
    return safe or "shared"


def user_fav_path(user_guid: str) -> str:
    fav_dir = CONF.get("fav_dir") or os.path.join(_HOME, "online_favorites")
    safe_name = sanitize_user_guid(user_guid)
    return os.path.join(fav_dir, f"{safe_name}.json")


def load_online_favorites(user_guid: str) -> list[dict]:
    path = user_fav_path(user_guid)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get("items"), list):
                return data["items"]
            if isinstance(data, list):
                return data
    except Exception as e:
        logger.warning("Failed to load online favorites for %s from %s: %s", user_guid, path, e)
    return []


def save_online_favorites(user_guid: str, items: list[dict]) -> bool:
    path = user_fav_path(user_guid)
    parent = os.path.dirname(path) or "."
    part_path = f"{path}.{uuid4().hex[:8]}.part"
    try:
        os.makedirs(parent, exist_ok=True)
        with open(part_path, "w", encoding="utf-8") as f:
            json.dump({"items": items}, f, ensure_ascii=False, indent=2)
        os.replace(part_path, path)
        return True
    except Exception as e:
        logger.warning("Failed to save online favorites for %s to %s: %s", user_guid, path, e)
        if os.path.exists(part_path):
            try:
                os.remove(part_path)
            except Exception:
                pass
        return False


def build_favorite_track_obj(guid: str, info: dict | None = None, created_at: int | None = None) -> dict:
    raw_info = dict(info or {})
    raw_info.setdefault("id", song_id_from_online_guid(guid))
    raw_info.setdefault("source", source_from_online_guid(guid))
    vo = build_online_track(raw_info)

    now = int(time.time())
    ts = created_at or now

    artist_name = vo.get("artist") or ""
    artists_list = [
        {
            "guid": f"{guid}:artist",
            "name": artist_name,
            "coverId": guid,
            "createdAt": ts,
            "updatedAt": ts,
        }
    ] if artist_name else []

    album_name = (vo.get("album", {}).get("name") if isinstance(vo.get("album"), dict) else "") or ""
    album_obj = {
        "guid": f"{guid}:album",
        "name": album_name,
        "artists": artists_list,
        "coverId": guid,
        "releaseDate": 0,
        "barcode": "",
        "createdAt": ts,
        "updatedAt": ts,
    }

    audio_spec = vo.get("audioSpec") or {}

    return {
        "guid": guid,
        "title": vo.get("title") or "",
        "duration": vo.get("duration") or 0,
        "isFavorite": True,
        "isCue": False,
        "genres": [],
        "artists": artists_list,
        "album": album_obj,
        "audioSpec": audio_spec,
        "accessStatus": 0,
        "coverId": guid,
        "year": 0,
        "discNo": 1,
        "trackNo": 1,
        "isrc": "",
        "createdAt": ts,
        "updatedAt": ts,
    }


async def _probe_upstream_auth(request: Request, client: httpx.AsyncClient) -> tuple[bool, str, Response | None]:
    """向上游探测用户是否已登录。复用当前请求 headers。
    返回 (is_authed, user_guid, error_response)。
    """
    headers = copy_incoming_headers(request)
    try:
        probe_req = client.build_request("GET", "/music/api/v1/user/me", headers=headers)
        probe_resp = await client.send(probe_req)
        resp_headers = filter_headers(probe_resp.headers, exclude_keys={"content-length", "content-encoding"})

        if probe_resp.status_code == 401:
            return False, "", Response(
                content=probe_resp.content,
                status_code=401,
                headers=resp_headers,
                media_type=probe_resp.headers.get("content-type"),
            )

        if probe_resp.status_code == 200:
            try:
                probe_json = probe_resp.json()
                if isinstance(probe_json, dict) and probe_json.get("code") == 99999:
                    return False, "", JSONResponse(
                        content=probe_json,
                        status_code=200,
                        headers=resp_headers,
                    )
                if isinstance(probe_json, dict) and probe_json.get("code") == 0:
                    data = probe_json.get("data")
                    if isinstance(data, dict) and data.get("guid"):
                        return True, str(data["guid"]), None
                    logger.warning("user/me response missing data.guid, falling back to 'shared': %s", probe_json)
                    return True, "shared", None
            except Exception as e:
                logger.warning("Failed to parse user/me json response: %s", e)
                return True, "shared", None
            return True, "shared", None

        # 其他非 200/401 状态码，上游异常
        return True, "shared", None
    except Exception as e:
        logger.warning("Upstream auth probe failed: %s", e)
        # 探测异常时保守放行
        return True, "shared", None


@app.post("/music/api/v1/favorite-track/create")
async def favorite_track_create(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    upstream_client = get_upstream_client(request.app)
    try:
        body = await request.json()
    except Exception:
        body = {}

    guid = ""
    if isinstance(body, dict):
        guid = str(body.get("trackGUID") or body.get("guid") or "").strip()

    if not is_online_guid(guid):
        return await forward_to_upstream(request, upstream_client)

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp

    now = int(time.time())
    info = await _online_info(request, guid)
    if not info:
        cached_lyric = read_lyric_cache(guid)
        title = ""
        artist = ""
        cached_media = find_cache_file(guid)
        if cached_media:
            base = os.path.splitext(os.path.basename(cached_media))[0]
            if " - " in base:
                artist, title = base.split(" - ", 1)
            else:
                title = base
        info = {
            "id": song_id_from_online_guid(guid),
            "source": source_from_online_guid(guid),
            "title": title,
            "artist": artist,
            "lyric": cached_lyric,
        }

    track_obj = build_favorite_track_obj(guid, info, created_at=now)

    async with _FAV_LOCK:
        try:
            items = load_online_favorites(user_guid)
            # 查重
            idx = next((i for i, it in enumerate(items) if it.get("guid") == guid), None)
            if idx is not None:
                # 幂等更新
                items[idx]["track"] = track_obj
            else:
                items.append({
                    "guid": guid,
                    "createdAt": now,
                    "track": track_obj,
                })
            save_online_favorites(user_guid, items)
        except Exception as e:
            logger.warning("Error updating online favorites for user %s: %s", user_guid, e)

    return JSONResponse(content={"code": 0, "msg": "", "data": None})


@app.post("/music/api/v1/favorite-track/delete")
async def favorite_track_delete(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    upstream_client = get_upstream_client(request.app)
    try:
        body = await request.json()
    except Exception:
        body = {}

    guid = ""
    if isinstance(body, dict):
        guid = str(body.get("trackGUID") or body.get("guid") or "").strip()

    if not is_online_guid(guid):
        return await forward_to_upstream(request, upstream_client)

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp

    async with _FAV_LOCK:
        try:
            items = load_online_favorites(user_guid)
            items = [it for it in items if it.get("guid") != guid]
            save_online_favorites(user_guid, items)
        except Exception as e:
            logger.warning("Error deleting from online favorites for user %s: %s", user_guid, e)

    return JSONResponse(content={"code": 0, "msg": "", "data": None})


@app.get("/music/api/v1/favorite-track/list")
async def favorite_track_list(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    upstream_client = get_upstream_client(request.app)
    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"
    headers = copy_incoming_headers(request)

    req = upstream_client.build_request("GET", url_path, headers=headers)
    upstream_resp = await upstream_client.send(req)
    resp_headers = filter_headers(upstream_resp.headers, exclude_keys={"content-length", "content-encoding"})

    if upstream_resp.status_code != 200:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    try:
        upstream_json = upstream_resp.json()
    except Exception:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    if not isinstance(upstream_json, dict) or upstream_json.get("code") != 0:
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    # 探测当前用户身份
    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp

    # 成功获取官方列表，合并本地在线收藏
    data = upstream_json.get("data")
    if not isinstance(data, dict):
        data = {"list": [], "total": 0}
        upstream_json["data"] = data

    official_list = data.get("list")
    if not isinstance(official_list, list):
        official_list = []
        data["list"] = official_list

    # 飞牛音乐前端收藏列表依赖 isFavorite=True 状态判断，遍历补齐官方列表中可能缺失的字段
    for item in official_list:
        if isinstance(item, dict):
            item["isFavorite"] = True

    official_total = data.get("total")
    if not isinstance(official_total, int):
        official_total = len(official_list)

    async with _FAV_LOCK:
        try:
            fav_items = load_online_favorites(user_guid)
        except Exception as e:
            logger.warning("Error reading online favorites for list for user %s: %s", user_guid, e)
            fav_items = []

    # 按 createdAt 倒序
    fav_items_sorted = sorted(fav_items, key=lambda x: x.get("createdAt", 0), reverse=True)
    online_tracks = []
    for it in fav_items_sorted:
        t = it.get("track")
        if isinstance(t, dict):
            # 确保关键属性为最新或格式完整
            t["isFavorite"] = True
            online_tracks.append(t)
        else:
            g = it.get("guid") or ""
            if g:
                online_tracks.append(build_favorite_track_obj(g, created_at=it.get("createdAt")))

    data["list"] = official_list + online_tracks
    data["total"] = official_total + len(online_tracks)

    return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)


# === daily recommend + play history ===


@app.get("/music/api/v1/album/artist-detail/list")
@app.get("/music/api/v1/album/artist-detail/list/{subpath:path}")
async def album_artist_detail_list(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    """/album/artist-detail/list：酷狗歌手作品聚合为飞牛专辑列表。"""
    artist_guid = str(request.query_params.get("artistGUID") or request.query_params.get("artistGuid") or request.query_params.get("artist_id") or request.query_params.get("artistId") or "").strip()
    try:
        page = max(1, int(request.query_params.get("page") or 1))
    except (TypeError, ValueError):
        page = 1
    try:
        size = min(500, max(1, int(request.query_params.get("size") or 60)))
    except (TypeError, ValueError):
        size = 60
    if not artist_guid:
        return JSONResponse(content={"code": 400, "msg": "artistGUID required", "data": None})
    kugou_payload = await fetch_kugou_artist_album_list(request.app, artist_guid, page=page, size=size)
    if isinstance(kugou_payload, dict):
        return JSONResponse(content=kugou_payload, status_code=200)
    return await forward_to_upstream(request, get_upstream_client(request.app))


@app.get("/music/api/v1/artist/detail")
@app.get("/music/api/v1/artist/detail/{subpath=path}")
async def artist_detail(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    """/artist/detail：酷狗歌手详情；非酷狗 GUID 走飞牛上游。

    非酷狗 GUID（飞牛原生本地歌手）转上游后补 coverId：上游对无头像的
    本地歌手表 coverId=null，前端歌手详情页头像位空白；用歌手自身 guid
    兜底，与 list 类接口补法一致。酷狗分支由 fetch_kugou_artist_detail
    自行填 coverId=guid，不需补。
    """
    artist_guid = str(request.query_params.get("guid") or request.query_params.get("GUID") or "").strip()
    if not artist_guid:
        return JSONResponse(content={"code": 400, "msg": "guid required", "data": None})
    parsed_artist_id, artist_kind = parse_kugou_artist_guid(artist_guid)
    if artist_kind != "kugou_artist":
        envelope = await fetch_upstream_envelope(request, get_upstream_client(request.app))
        if isinstance(envelope, Response):
            return envelope
        headers = envelope.pop("_ext_headers", {})
        if envelope.get("code") != 0:
            return JSONResponse(content=envelope, headers=headers)
        fill_detail_cover_id(envelope, kind="artist")
        return JSONResponse(content=envelope, headers=headers)
    kugou_payload = await fetch_kugou_artist_detail(request.app, artist_guid)
    if isinstance(kugou_payload, dict):
        return JSONResponse(content=kugou_payload, status_code=200)


@app.get("/music/api/v1/artist/list")
@app.get("/music/api/v1/artist/list/{subpath=path}")
async def artist_list(request: Request, subpath: str = ""):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    """/artist/list：本地歌手列表；转上游后补空 coverId 为 artist:<guid>。

    上游对无头像的本地歌手表 coverId=null，前端歌手列表卡片头像位空白。
    补成裸 guid 不够——歌手实体 guid 与曲目 guid 同构（32 位 hex），
    /static/cover 会按曲目链路解析，必须带 artist: 前缀才走 step2-entity。
    酷狗歌手列表走 /search/artist，不经本路由。
    """
    envelope = await fetch_upstream_envelope(request, get_upstream_client(request.app))
    if isinstance(envelope, Response):
        return envelope
    headers = envelope.pop("_ext_headers", {})
    if envelope.get("code") != 0:
        return JSONResponse(content=envelope, headers=headers)
    fill_artist_list_cover_ids(envelope)
    return JSONResponse(content=envelope, headers=headers)


@app.get("/music/api/v1/album/list")
@app.get("/music/api/v1/album/list/{subpath=path}")
async def album_list(request: Request, subpath: str = ""):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    """/album/list：本地专辑列表；转上游后补空 coverId 为 album:<guid>。

    与 artist/list 同构：上游对无封面的本地专辑标 coverId=null，前端专辑
    列表卡片封面位空白。必须带 album: 前缀，裸 guid 会走曲目链路。
    酷狗专辑列表走 /search/album，不经本路由。
    """
    envelope = await fetch_upstream_envelope(request, get_upstream_client(request.app))
    if isinstance(envelope, Response):
        return envelope
    headers = envelope.pop("_ext_headers", {})
    if envelope.get("code") != 0:
        return JSONResponse(content=envelope, headers=headers)
    fill_album_list_cover_ids(envelope)
    return JSONResponse(content=envelope, headers=headers)


@app.get("/music/api/v1/album/detail")
@app.get("/music/api/v1/album/detail/{subpath=path}")
async def album_detail(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    """/album/detail：酷狗专辑详情；非酷狗 GUID 走飞牛上游。

    飞牛请求形如
    /music/api/v1/album/detail?guid=online%3Akugou%3Aalbum%3A36547917
    （guid 里 %3A 即 ':'）。返回结构与飞牛原生 /album/detail 一致。
    """
    album_guid = str(
        request.query_params.get("guid")
        or request.query_params.get("GUID")
        or request.query_params.get("albumGuid")
        or ""
    ).strip()
    if not album_guid:
        return JSONResponse(content={"code": 400, "msg": "guid required", "data": None})
    kugou_payload = await fetch_kugou_album_detail(request.app, album_guid)
    if isinstance(kugou_payload, dict):
        return JSONResponse(content=kugou_payload, status_code=200)
    # 本地专辑：转上游后补 coverId。歌手详情同样处理（artist_detail）。
    # 本地专辑无封面时上游标 coverId=null，专辑详情页封面位空白；补为
    # album:<guid> 前缀格式，封面路由才能把它与同构的本地曲目 guid 区分开。
    envelope = await fetch_upstream_envelope(request, get_upstream_client(request.app))
    if isinstance(envelope, Response):
        return envelope
    headers = envelope.pop("_ext_headers", {})
    if envelope.get("code") != 0:
        return JSONResponse(content=envelope, headers=headers)
    fill_detail_cover_id(envelope, kind="album")
    return JSONResponse(content=envelope, headers=headers)


@app.get("/music/api/v1/track/album-detail/list")
@app.get("/music/api/v1/track/album-detail/list/{subpath=path}")
async def track_album_detail_list(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    """/track/album-detail/list：酷狗专辑歌曲列表；非酷狗 GUID 走飞牛上游。

    参数映射参考 /track/artist-detail/list 与 /track/playlist-detail/list：
    page/size + albumGUID，返回 {"code":0,"data":{"list":[...],"total":n}}，
    track 项结构与 build_online_track 一致。
    """
    album_guid = str(
        request.query_params.get("albumGUID")
        or request.query_params.get("albumGuid")
        or request.query_params.get("album_id")
        or request.query_params.get("albumId")
        or request.query_params.get("guid")
        or ""
    ).strip()
    if not album_guid:
        return JSONResponse(content={"code": 400, "msg": "albumGUID required", "data": None})
    if not album_guid.startswith("online:kugou:album:"):
        # 本地专辑：同上，转上游后把空的 coverId 兜底为歌曲自身 guid。
        envelope = await fetch_upstream_envelope(request, get_upstream_client(request.app))
        if isinstance(envelope, Response):
            return envelope
        headers = envelope.pop("_ext_headers", {})
        if envelope.get("code") != 0:
            return JSONResponse(content=envelope, headers=headers)
        fill_local_track_list_cover_ids(envelope)
        return JSONResponse(content=envelope, headers=headers)
    try:
        page = max(int(request.query_params.get("page") or 1), 1)
    except (TypeError, ValueError):
        page = 1
    try:
        # 酷狗 /album/songs pagesize 硬上限 50，超过返 errmsg="invalid param" 空结果。
        size = min(50, max(1, int(request.query_params.get("size") or 50)))
    except (TypeError, ValueError):
        size = 50
    payload = await fetch_kugou_album_tracks(request.app, album_guid, page=page, size=size)
    # get_album_songs 内部已按嵌套结构（base/audio_info/authors）归一化；
    # 不要再套 track_item_to_raw，否则会把 title 清空。
    raw_tracks = [it for it in (payload.get("items") or []) if isinstance(it, dict)]
    tracks = [build_online_track(x) for x in raw_tracks if x.get("hash") or x.get("id")]
    return JSONResponse(
        content={
            "code": 0,
            "msg": "",
            "data": {
                "list": tracks,
                "total": int(payload.get("total") or len(tracks)),
            },
        },
        status_code=200,
    )


@app.get("/music/api/v1/track/artist-detail/list")
@app.get("/music/api/v1/track/artist-detail/list/{subpath:path}")
async def track_artist_detail_list(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    """/track/artist-detail/list：酷狗歌手作品歌曲列表；非酷狗 GUID 走飞牛上游。

    参数映射参考 /track/playlist-detail/list：page/size + artistGUID，返回
    {"code":0,"data":{"list":[...],"total":n}}，track 项结构与 build_online_track 一致。

    非酷狗 GUID（飞牛原生本地歌手，如 32 位 hex GUID）转上游后补 coverId：
    上游对无专辑封面/无内嵌封面标签的本地曲目标 coverId=null，前端按
    track.coverId 取封面拿不到，封面位空白；用歌曲自身 guid 兜底，与
    /track/list、/track/play-history/list 的补法一致。酷狗分支由
    build_online_track 自行填 coverId，不需补。
    """
    artist_guid = str(
        request.query_params.get("artistGUID")
        or request.query_params.get("artistGuid")
        or request.query_params.get("artist_id")
        or request.query_params.get("artistId")
        or request.query_params.get("guid")
        or ""
    ).strip()
    if not artist_guid:
        return JSONResponse(content={"code": 400, "msg": "artistGUID required", "data": None})
    parsed_artist_id, artist_kind = parse_kugou_artist_guid(artist_guid)
    if artist_kind != "kugou_artist":
        # 本地歌手：转上游后补 coverId（fill_local_track_list_cover_ids 对线上
        # guid 也安全，酷狗项本身 coverId 非空不会被覆盖）。
        envelope = await fetch_upstream_envelope(request, get_upstream_client(request.app))
        if isinstance(envelope, Response):
            return envelope
        headers = envelope.pop("_ext_headers", {})
        if envelope.get("code") != 0:
            return JSONResponse(content=envelope, headers=headers)
        fill_local_track_list_cover_ids(envelope)
        return JSONResponse(content=envelope, headers=headers)
    try:
        page = max(int(request.query_params.get("page") or 1), 1)
    except (TypeError, ValueError):
        page = 1
    try:
        size = int(request.query_params.get("size") or 50)
    except (TypeError, ValueError):
        size = 50
    if size < 1:
        size = 50
    payload = await fetch_kugou_artist_tracks(request.app, artist_guid, page=page, size=size)
    # get_artist_audios 内部已调 search_item_to_raw 归一化；不要再套 track_item_to_raw，
    # 否则会把 title 清空（后者只认原始 KuGou 字段 hash/singerinfo/albuminfo）。
    raw_tracks = [it for it in (payload.get("items") or []) if isinstance(it, dict)]
    tracks = [build_online_track(x) for x in raw_tracks if x.get("hash") or x.get("id")]
    return JSONResponse(
        content={
            "code": 0,
            "msg": "ok",
            "data": {"list": tracks, "total": int(payload.get("total") or len(tracks))},
        }
    )


@app.get("/music/api/v1/playlist/list")
@app.get("/music/api/v1/playlist/list/{subpath:path}")
async def playlist_list(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    upstream_client = get_upstream_client(request.app)

    # 1. 先加载酷狗用户歌单（不依赖上游）
    kugou_playlists = []
    try:
        kugou_playlists = await fetch_kugou_user_playlist_bundles()
    except Exception as e:
        logger.warning("[KUGOU_PLAYLIST] list inject failed: %s", e)

    now_ts = int(time.time())
    injected = [
        {
            "guid": p.get("guid"),
            "name": p.get("name"),
            "coverId": p.get("coverId") or p.get("guid"),
            "createdAt": now_ts,
            "updatedAt": now_ts,
            # 与 detail / batch-detail 一致：trackCount 映射自酷狗 count
            "trackCount": int(p.get("trackCount") or 0),
        }
        for p in kugou_playlists
    ]

    # 2. 尝试获取官方歌单列表（可能失败）
    official = []
    resp_headers = {}
    envelope_code = 0
    envelope_msg = "ok"

    envelope = await fetch_upstream_envelope(request, upstream_client)
    if isinstance(envelope, dict):
        resp_headers = envelope.pop("_ext_headers", {})
        envelope_code = envelope.get("code", 0)
        envelope_msg = envelope.get("msg", "ok")
        data = envelope.get("data")
        if isinstance(data, dict):
            raw_list = data.get("list")
            if isinstance(raw_list, list):
                official = raw_list
    elif isinstance(envelope, Response):
        # 上游返回错误，但酷狗歌单仍然要返回
        pass

    # 3. 去重：移除官方列表中已存在的酷狗歌单
    official = [
        it for it in official
        if not (isinstance(it, dict) and is_kugou_playlist_guid(str(it.get("guid") or "")))
    ]

    # 4. 合并酷狗歌单 + 官方歌单
    merged_list = injected + official

    return JSONResponse(
        content={
            "code": envelope_code,
            "msg": envelope_msg,
            "data": {
                "list": merged_list,
                "total": len(merged_list),
            },
        },
        headers=resp_headers if resp_headers else None,
    )


@app.get("/music/api/v1/playlist/detail")
async def playlist_detail(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    guid = str(request.query_params.get("guid") or "").strip()
    if not is_kugou_playlist_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    # 酷狗歌单：从酷狗列表中找到对应条目
    try:
        bundles = await fetch_kugou_user_playlist_bundles()
    except Exception as e:
        logger.warning("[KUGOU_PLAYLIST] detail fetch failed: %s", e)
        return JSONResponse(content={"code": 1, "msg": str(e), "data": None})

    for b in bundles:
        if b.get("guid") == guid:
            now_ts = int(time.time())
            return JSONResponse(content={
                "code": 0, "msg": "ok",
                "data": {
                    "guid": guid,
                    "name": b.get("name"),
                    "coverId": b.get("coverId"),
                    "createdAt": now_ts,
                    "updatedAt": now_ts,
                    # 歌曲数：b 由 build_kugou_playlist_obj 生成，trackCount 已映射自酷狗 count
                    "trackCount": int(b.get("trackCount") or 0),
                }
            })

    # 未找到，返回空
    return JSONResponse(content={"code": 0, "msg": "ok", "data": None})


@app.get("/music/api/v1/playlist/batch-detail")
async def playlist_batch_detail(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    raw = request.query_params.get("guids") or request.query_params.get("guid") or ""
    guids = [g.strip() for g in raw.split(",") if g.strip()]
    kugou_ids = [g for g in guids if is_kugou_playlist_guid(g)]
    if not kugou_ids:
        return await forward_to_upstream(request, get_upstream_client(request.app))

    upstream_client = get_upstream_client(request.app)
    rest = [g for g in guids if not is_kugou_playlist_guid(g)]
    official_list: list = []
    if rest:
        headers = copy_incoming_headers(request)
        req = upstream_client.build_request(
            "GET",
            f"/music/api/v1/playlist/batch-detail?guids={quote(','.join(rest), safe=',')}",
            headers=headers,
        )
        resp = await upstream_client.send(req)
        if resp.status_code == 200:
            try:
                payload = resp.json()
                if isinstance(payload, dict) and payload.get("code") == 0:
                    data = payload.get("data") or {}
                    if isinstance(data, dict) and isinstance(data.get("list"), list):
                        official_list = data["list"]
                    elif isinstance(data, list):
                        official_list = data
            except Exception:
                official_list = []

    try:
        bundles = await fetch_kugou_user_playlist_bundles()
    except Exception as e:
        logger.warning("[KUGOU_PLAYLIST] batch-detail fetch failed: %s", e)
        bundles = []

    now_ts = int(time.time())
    kugou_details = []
    for b in bundles:
        if b.get("guid") in kugou_ids:
            kugou_details.append({
                "guid": b.get("guid"), "id": b.get("guid"),
                "name": b.get("name"), "title": b.get("name"),
                "coverId": b.get("coverId"), "coverUrl": b.get("coverUrl", ""),
                "creator": b.get("creator", ""),
                "createdAt": now_ts, "updatedAt": now_ts,
                "trackCount": int(b.get("trackCount") or 0),
                "source": "kugou", "isKugouPlaylist": True,
            })

    return JSONResponse(content={"code": 0, "msg": "ok", "data": {"list": kugou_details + official_list}})


@app.get("/music/api/v1/track/playlist-detail/list")
async def playlist_track_list(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    guid = str(
        request.query_params.get("playlistGUID")
        or request.query_params.get("playlistGuid")
        or request.query_params.get("guid")
        or ""
    ).strip()
    if not is_kugou_playlist_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    try:
        page = max(int(request.query_params.get("page") or 1), 1)
    except (TypeError, ValueError):
        page = 1
    try:
        size = int(request.query_params.get("size") or 50)
    except (TypeError, ValueError):
        size = 50
    if size < 1:
        size = 50
    payload = await fetch_kugou_playlist_tracks(request.app, guid, page=page, size=size)
    raw_tracks = [kugou_source.track_item_to_raw(it) for it in (payload.get("items") or []) if isinstance(it, dict)]
    raw_tracks = [x for x in raw_tracks if x]
    tracks = [build_online_track(x) for x in raw_tracks]
    return JSONResponse(
        content={
            "code": 0,
            "msg": "ok",
            "data": {"list": tracks, "total": int(payload.get("total") or len(tracks))},
        }
    )


@app.post("/music/api/v1/event/report")
async def event_report(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    upstream_client = get_upstream_client(request.app)
    raw = await request.body()
    try:
        body = json.loads(raw.decode("utf-8") or "{}") if raw else {}
    except Exception:
        body = {}
    events = body.get("events") if isinstance(body, dict) else None
    other_events: list = []
    if isinstance(events, list):
        for ev in events:
            if not isinstance(ev, dict):
                continue
            other_events.append(ev)
    else:
        return await forward_to_upstream(request, upstream_client)

    if other_events:
        headers = copy_incoming_headers(request)
        fwd = dict(body)
        fwd["events"] = other_events
        req = upstream_client.build_request(
            "POST",
            "/music/api/v1/event/report",
            headers=headers,
            content=json.dumps(fwd).encode("utf-8"),
        )
        resp = await upstream_client.send(req)
        resp_headers = filter_headers(resp.headers, exclude_keys={"content-length", "content-encoding"})
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    return JSONResponse(content={"code": 0, "msg": "ok", "data": None})


@app.get("/music/api/v1/play-history/list")
@app.get("/music/api/v1/play-history/list/{subpath:path}")
async def play_history_list(request: Request):
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    """播放历史列表：透传飞牛上游，补全本地歌曲的空 coverId。

    上游对无专辑封面/无内嵌封面标签的本地曲目标 coverId=null，
    前端按 track.coverId 取封面时拿不到，封面位空白；
    用歌曲自身 guid 兜底，与 /track/list、/track/metadata 的补法一致。
    """
    upstream_client = get_upstream_client(request.app)
    envelope = await fetch_upstream_envelope(request, upstream_client)
    if isinstance(envelope, Response):
        return envelope
    headers = envelope.pop("_ext_headers", {})
    if envelope.get("code") != 0:
        return JSONResponse(content=envelope, headers=headers)

    data = envelope.get("data")
    if not isinstance(data, dict):
        data = {"list": [], "total": 0}
        envelope["data"] = data

    # 本地歌曲 coverId=null -> 兜底为歌曲自身 guid
    fill_local_track_list_cover_ids(envelope)

    # 酷狗歌单里的在线播放历史暂时不注入
    return JSONResponse(content=envelope, headers=headers)


# ============================================================
# QR 登录 API（供 fnOS 设置页面调用）
# ============================================================

@app.get("/_ext/login/qr/create")
async def login_qr_create():
    """生成登录二维码。返回 { status, qrCode, key }"""
    if not CONF["kugou_enabled"]:
        return JSONResponse({"status": "disabled", "message": "KuGou source is disabled"}, status_code=400)
    key = await kugou_source.qr_key()
    if not key:
        return JSONResponse({"status": "error", "message": "Failed to get QR key"}, status_code=500)
    qr_base64 = await kugou_source.qr_create(key)
    if not qr_base64:
        return JSONResponse({"status": "error", "message": "Failed to create QR code"}, status_code=500)
    return JSONResponse({
        "status": "ok",
        "key": key,
        "qrCode": qr_base64,
    })


@app.get("/_ext/login/qr/check")
async def login_qr_check(key: str):
    """检查二维码扫描状态。
    
    返回:
    - status=0: 二维码过期
    - status=1: 等待扫码
    - status=2: 已扫码，等待确认
    - status=4: 登录成功
    """
    if not CONF["kugou_enabled"]:
        return JSONResponse({"status": "disabled", "message": "KuGou source is disabled"}, status_code=400)
    result = await kugou_source.qr_check(key)
    
    qr_status = result.get("status", -1)
    
    if qr_status == 4:
        # 登录成功，保存凭证
        credentials = kugou_source.save_credentials(result)
        
        # 更新 .env 文件持久化凭证
        env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
        try:
            env_updates = {
                "FNMUSIC_KUGOU_TOKEN": credentials["token"],
                "FNMUSIC_KUGOU_USERID": credentials["userid"],
                "FNMUSIC_KUGOU_DFID": credentials["dfid"],
                "FNMUSIC_KUGOU_/track/": credentials["t1"],
                "FNMUSIC_KUGOU_MID": credentials["mid"],
                "FNMUSIC_KUGOU_GUID": credentials["guid"],
                "FNMUSIC_KUGOU_DEV": credentials["dev"],
                "FNMUSIC_KUGOU_MAC": credentials["mac"],
            }
            # 读取现有 .env
            env_content = {}
            if os.path.exists(env_path):
                with open(env_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if "=" in line and not line.startswith("#"):
                            k, v = line.split("=", 1)
                            env_content[k.strip()] = v.strip()
            # 更新凭证
            env_content.update(env_updates)
            # 写回 .env
            with open(env_path, "w") as f:
                for k, v in env_content.items():
                    f.write(f"{k}={v}\n")
            logger.info("QR login success: credentials saved to %s", env_path)
        except Exception as e:
            logger.warning("Failed to save credentials to .env: %s", e)
        
        # 更新内存中的 CONF
        CONF["kugou_token"] = credentials["token"]
        CONF["kugou_userid"] = credentials["userid"]
        CONF["kugou_dfid"] = credentials["dfid"]
        CONF["kugou_t1"] = credentials["t1"]
        CONF["kugou_mid"] = credentials["mid"]
        CONF["kugou_guid"] = credentials["guid"]
        CONF["kugou_dev"] = credentials["dev"]
        CONF["kugou_mac"] = credentials["mac"]
        
        # 重新注入 kugou_source
        kugou_source.set_config({
            "kugou_token": CONF["kugou_token"],
            "kugou_userid": CONF["kugou_userid"],
            "kugou_dfid": CONF["kugou_dfid"],
            "kugou_t1": CONF["kugou_t1"],
            "kugou_mid": CONF["kugou_mid"],
            "kugou_guid": CONF["kugou_guid"],
            "kugou_dev": CONF["kugou_dev"],
            "kugou_mac": CONF["kugou_mac"],
        })
        
        return JSONResponse({
            "status": "authenticated",
            "nickname": result.get("nickname", ""),
            "userid": credentials["userid"],
        })
    elif qr_status == 2:
        return JSONResponse({"status": "scanned", "message": "QR scanned, waiting for confirmation"})
    elif qr_status == 0:
        return JSONResponse({"status": "expired", "message": "QR code expired, please regenerate"})
    elif qr_status == 1:
        return JSONResponse({"status": "pending", "message": "Waiting for scan"})
    else:
        return JSONResponse({"status": "error", "message": result.get("message", "Unknown error")})


@app.get("/_ext/login/status")
async def login_status():
    """查询当前登录状态"""
    if not CONF["kugou_token"]:
        return JSONResponse({"status": "not_logged_in"})
    return JSONResponse({
        "status": "logged_in",
        "userid": CONF["kugou_userid"],
        "has_token": bool(CONF["kugou_token"]),
        "has_userid": bool(CONF["kugou_userid"]),
        "has_dfid": bool(CONF["kugou_dfid"]),
    })


@app.post("/_ext/login/logout")
async def login_logout():
    """退出登录，清除凭证"""
    # 清除内存中的凭证
    CONF["kugou_token"] = ""
    CONF["kugou_userid"] = ""
    CONF["kugou_dfid"] = ""
    CONF["kugou_t1"] = ""
    CONF["kugou_mid"] = ""
    CONF["kugou_guid"] = ""
    CONF["kugou_dev"] = ""
    CONF["kugou_mac"] = ""
    
    # 重新注入 kugou_source
    kugou_source.set_config({
        "kugou_token": "",
        "kugou_userid": "",
        "kugou_dfid": "",
        "kugou_t1": "",
        "kugou_mid": "",
        "kugou_guid": "",
        "kugou_dev": "",
        "kugou_mac": "",
    })
    
    # 清除 .env 中的凭证
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(env_path):
        try:
            env_content = {}
            with open(env_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        k, v = line.split("=", 1)
                        env_content[k.strip()] = v.strip()
            # 清除凭证字段
            for key in ["FNMUSIC_KUGOU_TOKEN", "FNMUSIC_KUGOU_USERID", "FNMUSIC_KUGOU_DFID",
                       "FNMUSIC_KUGOU_/track/", "FNMUSIC_KUGOU_MID", "FNMUSIC_KUGOU_GUID",
                       "FNMUSIC_KUGOU_DEV", "FNMUSIC_KUGOU_MAC"]:
                env_content[key] = ""
            with open(env_path, "w") as f:
                for k, v in env_content.items():
                    f.write(f"{k}={v}\n")
            logger.info("Logout: credentials cleared from %s", env_path)
        except Exception as e:
            logger.warning("Failed to clear credentials from .env: %s", e)
    
    return JSONResponse({"status": "ok", "message": "Logged out"})


def _settings_page_path() -> Path:
    return Path(__file__).resolve().parents[1] / "web" / "settings.html"


@app.get("/app/fnmusic_ext_kugou")
@app.get("/app/fnmusic_ext_kugou/")
@app.get("/app/fnmusic_ext_kugou/settings")
@app.get("/app/fnmusic_ext_kugou/settings/")
async def settings_page():
    page = _settings_page_path()
    if not page.exists():
        return HTMLResponse("<h1>Settings page missing</h1>", status_code=500)
    return HTMLResponse(page.read_text(encoding="utf-8"), media_type="text/html; charset=utf-8")


@app.get("/app/fnmusic_ext_kugou/_ext/healthz")
async def gateway_healthz(request: Request):
    try:
        up = await ext_healthz(request)
        upstream_status = up.get("upstream")
        kugou_status = up.get("kugou")
    except Exception:
        upstream_status = "unknown"
        kugou_status = "unknown"
    return JSONResponse({"ok": True, "upstream": upstream_status, "kugou": kugou_status, "gateway": "ok"})


@app.get("/app/fnmusic_ext_kugou/_ext/login/qr/create")
async def login_qr_create_gateway():
    return await login_qr_create()


@app.get("/app/fnmusic_ext_kugou/_ext/login/qr/check")
async def login_qr_check_gateway(key: str):
    return await login_qr_check(key)


@app.get("/app/fnmusic_ext_kugou/_ext/login/status")
async def login_status_gateway():
    return await login_status()


@app.post("/app/fnmusic_ext_kugou/_ext/login/logout")
async def login_logout_gateway():
    return await login_logout()


@app.get("/app/fnmusic_ext_kugou/_ext/settings")
async def get_settings_gateway(request: Request):
    # 支持 GET 保存模式:?save=1&kugou_url=...&kugou_quality=...&kugou_enabled=...
    # 目的:绕开 nginx 对 POST 的 auth_request 间歇性拦截,保存请求直接写文件不转发。
    if (request.query_params.get("save") == "1"):
        form = {k: v for k, v in request.query_params.items() if k not in ("save",)}
        return await _save_settings_impl(form)
    return JSONResponse({
        "kugou_url": CONF["kugou_url"],
        "kugou_quality": CONF["kugou_quality"],
        "kugou_enabled": CONF["kugou_enabled"],
    })


async def _parse_form_dict(request: Request) -> dict:
    # 不用 request.form()，避开 python-multipart 依赖。
    # 兼容 x-www-form-urlencoded 与 JSON。
    ctype = (request.headers.get("content-type") or "").lower()
    raw = await request.body()
    if not raw:
        return {}
    if "application/json" in ctype:
        try:
            obj = json.loads(raw.decode("utf-8", errors="replace"))
            if isinstance(obj, dict):
                return {k: str(v) for k, v in obj.items()}
        except Exception:
            pass
    try:
        parsed = parse_qs(raw.decode("utf-8", errors="replace"), keep_blank_values=True)
        return {k: (v[0] if isinstance(v, list) and v else "") for k, v in parsed.items()}
    except Exception:
        return {}


@app.post("/app/fnmusic_ext_kugou/_ext/settings")
async def save_settings_gateway(request: Request):
    form = await _parse_form_dict(request)
    return await _save_settings_impl(form)


@app.get("/_ext/settings")
async def get_settings(request: Request):
    if (request.query_params.get("save") == "1"):
        form = {k: v for k, v in request.query_params.items() if k not in ("save",)}
        return await _save_settings_impl(form)
    return JSONResponse({
        "kugou_url": CONF["kugou_url"],
        "kugou_quality": CONF["kugou_quality"],
        "kugou_enabled": CONF["kugou_enabled"],
    })


@app.post("/_ext/settings")
async def save_settings(request: Request):
    form = await _parse_form_dict(request)
    return await _save_settings_impl(form)


async def _save_settings_impl(form):
    env_path = Path(__file__).resolve().parents[1] / ".env"
    env_content = {}
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env_content[k.strip()] = v.strip()

    if "kugou_url" in form:
        value = str(form["kugou_url"]).strip()
        if value:
            env_content["FNMUSIC_KUGOU_URL"] = value
            CONF["kugou_url"] = value
            kugou_source.set_config({"kugou_url": value})
    if "kugou_quality" in form:
        value = str(form["kugou_quality"]).strip()
        if value:
            env_content["FNMUSIC_KUGOU_QUALITY"] = value
            CONF["kugou_quality"] = value
            kugou_source.set_config({"kugou_quality": value})
    if "kugou_enabled" in form:
        value = str(form["kugou_enabled"]).strip()
        if value:
            enabled = value in ("1", "true", "True", "yes")
            env_content["FNMUSIC_KUGOU_ENABLED"] = "1" if enabled else "0"
            CONF["kugou_enabled"] = enabled
            kugou_source.set_config({"kugou_enabled": enabled})

    try:
        os.makedirs(env_path.parent, exist_ok=True)
    except Exception as e:
        return JSONResponse({"status": "error", "message": f"保存失败: 无法创建目录 {env_path.parent}: {e}"}, status_code=500)

    tmp = tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(env_path.parent))
    try:
        for k, v in env_content.items():
            tmp.write(f"{k}={v}\n")
        tmp.flush()
        tmp.close()
        os.replace(tmp.name, env_path)
        os.chmod(env_path, 0o600)
    except Exception as e:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass
        return JSONResponse({"status": "error", "message": f"保存失败: {e}"}, status_code=500)
    return JSONResponse({"status": "ok", "message": "配置已保存，请重启扩展应用生效"})


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def catch_all(request: Request, full_path: str):
    # 代理只接管 GET：其余方法（POST/PUT/DELETE/PATCH/HEAD/OPTIONS）
    # 一律透传上游，不进入本代理的拦截与字段兜底逻辑。
    if not _is_get_request(request):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    if full_path == "music/api/v1/track/list":
        upstream_client = get_upstream_client(request.app)
        payload_or_resp = await fetch_upstream_envelope(request, upstream_client)
        if isinstance(payload_or_resp, Response):
            return payload_or_resp
        payload = payload_or_resp
        headers = payload.pop("_ext_headers", {})
        fill_local_track_list_cover_ids(payload)
        return JSONResponse(content=payload, status_code=payload_or_resp_status(payload), headers=headers or None)

    # 风格详情曲目列表：结构与 track/list 一致（data.list[] 为 track），
    # 本地曲目无专辑封面时 coverId=null，走 catch-all 透传会保持空值，
    # 前端歌曲列表封面位空白。复用同一兜底，补为曲目自身 guid。
    if full_path == "music/api/v1/track/genre-detail/list":
        upstream_client = get_upstream_client(request.app)
        payload_or_resp = await fetch_upstream_envelope(request, upstream_client)
        if isinstance(payload_or_resp, Response):
            return payload_or_resp
        payload = payload_or_resp
        headers = payload.pop("_ext_headers", {})
        fill_local_track_list_cover_ids(payload)
        return JSONResponse(content=payload, status_code=payload_or_resp_status(payload), headers=headers or None)

    if _APP_MODE in {"gateway", "ui", "app"}:
        return JSONResponse({"status": "not_found", "path": full_path}, status_code=404)
    return await forward_to_upstream(request, get_upstream_client(request.app))
