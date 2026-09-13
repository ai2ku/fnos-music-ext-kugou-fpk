# SPDX-License-Identifier: GPL-2.0-or-later
# Copyright (C) 2026 ai2ku (https://github.com/ai2ku/)
#
# 本程序是自由软件：你可以按照 GNU 通用公共许可证（第 2 版或更高版本）
# 的条件重新发布和/或修改它。完整许可证见仓库根目录 LICENSE 文件。

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
import re
import time
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

# /album/detail?id=<id> 聚合结果缓存：detail 接口单张很快，但同一专辑详情页
# 会被封面/曲目/详情多次触发，缓存避免反复打两次酷狗接口。
_ALBUM_DETAIL_CACHE: dict[str, tuple[float, dict]] = {}
_ALBUM_DETAIL_TTL_S = 6 * 60 * 60


def _remember_song(item: dict) -> None:
    sid = str(item.get("id") or "").split(":", 1)[-1]
    if sid:
        _search_index[sid] = dict(item)


def get_search_index(song_id: str) -> dict | None:
    """读内存索引里的单曲元数据（album/artist 等）；未命中返回 None。

    本地曲目走封面回写时，若上游 track.album 为空则用此函数
    拿酷狗侧已搜过的专辑名强制覆盖。未命中时调用方应走
    /search 回源匹配。
    """
    if not song_id:
        return None
    sid = str(song_id).split(":", 1)[-1].strip()
    cached = _search_index.get(sid)
    return dict(cached) if cached else None




# ============================================================
# 搜索
# ============================================================

def _singers_from_any(it: dict) -> list[dict]:
    """统一歌手数组提取：优先 singerinfo，搜索格式优先 data.lists[].Singers。"""
    raw_singers = it.get("Singers") or it.get("singers") or it.get("singerinfo")
    if isinstance(raw_singers, str):
        name = raw_singers.strip()
        return [{"name": name, "id": ""}] if name else []
    if isinstance(it.get("singerinfo"), list):
        artists = []
        for singer in it.get("singerinfo"):
            if isinstance(singer, dict):
                name = str(singer.get("name") or "").strip()
                if name:
                    artists.append(singer)
        return artists
    if isinstance(raw_singers, list):
        artists = []
        for singer in raw_singers:
            if isinstance(singer, dict):
                name = str(singer.get("Name") or singer.get("name") or "").strip()
                singer_id = str(singer.get("ID") or singer.get("id") or singer.get("SingerID") or singer.get("singerId") or "").strip()
                if name:
                    artists.append({"name": name, "id": singer_id})
            else:
                name = str(singer or "").strip()
                if name:
                    artists.append({"name": name, "id": ""})
        return artists
    for key in ("author_name", "authorName", "AuthorName", "singername", "singerName", "SingerName", "singer", "SingName", "singName", "artist"):
        name = str(it.get(key) or "").strip()
        if name:
            raw_id = it.get("SingerID") or it.get("singerId") or it.get("authorID") or it.get("authorId")
            # key 不存在时 .get() 返回 None，直接 str() 会得到字符串 "None"，
            # 下游会拼出 online:kugou:artist:None 的脏 guid
            singer_id = str(raw_id).strip() if raw_id not in (None, "") else ""
            # /artist/audios 的 author_name 常是多人："安苏羽、橙大蕾蕾" / "IN-K、安苏羽、傅梦彤"
            parts = [p.strip() for p in re.split(r"[、,，/]", name) if p.strip()]
            return [{"name": p, "id": singer_id} for p in parts]
    return []








def _format_from_any(it: dict) -> str:
    """统一格式：优先 extname，兼容旧 ExtName/songType/format。"""
    for key in ("extname", "extName", "ExtName", "songType", "format", "ext"):
        value = it.get(key)
        if value not in (None, ""):
            return str(value).strip().lower() or "mp3"
    return "mp3"


def _cover_from_any(it: dict) -> str:
    """统一封面：保留 {size} 占位符，供封面接口动态替换。"""
    for key in ("cover", "Image", "image", "Img", "img", "AlbumImg", "albumImg", "AlbumImage", "albumImage", "PicUrl", "picUrl", "coverImg", "coverImgUrl", "AlbumImage", "albumImage", "albumImg", "albumimg", "album_img"):
        value = it.get(key)
        if value not in (None, ""):
            return str(value).strip()
    trans_param = it.get("trans_param")
    if isinstance(trans_param, dict):
        for key in ("union_cover", "UnionCover", "cover", "image"):
            value = trans_param.get(key)
            if value not in (None, ""):
                return str(value).strip()
    return ""


def _pick_ci(it: dict, *keys: str, default: str = "") -> str:
    """大小写/命名不敏感取字段：先看精确 key，再按小写归一化匹配。

    KuGouMusicApi 同一接口的回参在不同鉴权状态下字段风格不同
    （filehash / fileHash / FileHash；songname / songName / OriSongName 等），
    精确匹配会让整首歌被丢弃，所以映射统一走这里。
    """
    if not isinstance(it, dict):
        return default
    for key in keys:
        value = it.get(key)
        if value not in (None, ""):
            return str(value).strip()
    lowered = {str(k).lower(): v for k, v in it.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value not in (None, ""):
            return str(value).strip()
    return default




def search_item_to_raw(it: dict) -> dict:
    """把酷狗搜索/歌单歌曲对象都转成统一内部格式。"""
    if not isinstance(it, dict):
        return {}
    # 歌单 track/all 的真实字段完整，直接走精确路径。
    if isinstance(it.get("singerinfo"), list) or isinstance(it.get("albuminfo"), dict):
        return track_item_to_raw(it)

    # /artist/audios 的真实回参（必须先于 /privilege/lite 判断！两者都有 hash 键，
    # 先判 lite 会读不到字段导致标题/歌手/时长全空）。
    # 真实字段：hash / audio_name / author_name(可多人，顿号分隔) / album_id / album_name /
    # timelength(毫秒) / filesize / bitrate / extname / publish_date / songid / audio_id。
    # 封面不在顶层，在 trans_param.union_cover（带 {size} 占位符）。
    # 兼容另一种风格：filehash / songname / playTime(毫秒)。
    if "audio_name" in it or (it.get("author_name") not in (None, "") and "timelength" in it):
        sid = _pick_ci(it, "hash", "filehash", "hash_128", "FileHash")
        if not sid:
            return {}
        singers = _singers_from_any(it)
        # author_name 可能是多人："安苏羽、橙大蕾蕾" -> 拆成多个 artist
        if len(singers) <= 1:
            author_name = _pick_ci(it, "author_name", "authorName", "singername")
            if author_name and (not singers or not singers[0].get("name")):
                first_id = str(it.get("authorID") or it.get("authorId") or it.get("SingerID") or "").strip()
                singers = [
                    {"name": part.strip(), "id": first_id}
                    for part in re.split(r"[、,，/]", author_name)
                    if part.strip()
                ]
        singer_name = "、".join(
            str(x.get("name") or "").strip() for x in singers if isinstance(x, dict) and str(x.get("name") or "").strip()
        )
        album_name = _pick_ci(it, "album_name", "albumName", "albumname")
        raw_name = _pick_ci(it, "audio_name", "songname", "name", "OriSongName")
        # 注意：_strip_singer_prefix_from_name 期望 list[str]，不是 dict 列表
        title = _strip_singer_prefix_from_name(raw_name, [singer_name] if singer_name else [], album_name)
        # timelength / playTime 都是毫秒
        duration_ms = _to_int(it.get("timelength") or it.get("playTime") or it.get("timelength_128") or 0)
        if not duration_ms:
            duration_ms = _to_int(it.get("duration") or 0)
        duration = duration_ms / 1000.0 if duration_ms > 1000 else _to_float(duration_ms)
        ext = str(it.get("extname") or it.get("quality") or "mp3").strip().lower() or "mp3"
        file_size = _to_int(it.get("filesize") or it.get("filesize_128") or 0)
        bitrate = _to_int(it.get("bitrate") or 0)
        if not duration and file_size and bitrate:
            duration = file_size * 8 / (bitrate * 1000)
        cover = _cover_from_any(it)
        return {
            "id": f"kugou:{sid}",
            "source": "kugou",
            "hash": sid,
            "title": title,
            "artist": singer_name,
            "artists": singers,
            "album": album_name,
            "album_id": _pick_ci(it, "album_id", "albumId"),
            "duration_s": duration,
            "ext": ext,
            "cover_url": cover,
            "union_cover": cover,
            "file_size": file_size,
            "bitrate": bitrate,
            "lyric": "",
            "release_date": _pick_ci(it, "publish_date", "publishDate", "release_date"),
        }

    # /privilege/lite 的真实回参按小写字段精确映射。
    if "hash" in it or "singername" in it or "albumname" in it:
        sid = str(it.get("hash") or "").strip()
        if not sid:
            return {}
        raw_name = str(it.get("name") or "").strip()
        singername = str(it.get("singername") or "").strip()
        albumname = str(it.get("albumname") or "").strip()
        title = _strip_singer_prefix_from_name(raw_name, [singername] if singername else [], albumname)
        return {
            "id": f"kugou:{sid}",
            "source": "kugou",
            "hash": sid,
            "title": title,
            "artist": singername,
            "artists": [{"name": singername, "id": ""}] if singername else [],
            "album": albumname,
            "album_id": str(it.get("album_id") or "").strip(),
            "duration_s": _to_int(it.get("duration") or 0),
            "ext": str(it.get("extname") or it.get("quality") or "mp3").strip().lower() or "mp3",
            "cover_url": str(it.get("cover") or it.get("image") or "").strip(),
            "union_cover": str(it.get("cover") or it.get("image") or "").strip(),
            "file_size": _to_int(it.get("filesize") or 0),
            "bitrate": _to_int(it.get("bitrate") or 0),
            "lyric": "",
        }

    sid = str(it.get("FileHash") or "").strip()
    if not sid:
        return {}
    artists = [
        {
            "name": str(singer.get("name") or "").strip(),
            "id": str(singer.get("id") or "").strip(),
        }
        for singer in (it.get("Singers") or [])
        if isinstance(singer, dict) and str(singer.get("name") or "").strip()
    ]
    album_id = str(it.get("AlbumID") or "").strip()
    album_name = str(it.get("AlbumName") or "").strip()
    artist = "、".join(str(x.get("name") or "").strip() for x in artists if str(x.get("name") or "").strip())
    title = str(it.get("OriSongName") or "").strip()
    suffix = str(it.get("Suffix") or "").strip()
    if suffix:
        title = f"{title} {suffix}" if title else suffix
    ext = str(it.get("ExtName") or "mp3").strip().lower() or "mp3"
    cover = str(it.get("Image") or "").strip()
    file_size = _to_int(it.get("FileSize") or 0)
    bitrate = _to_int(it.get("Bitrate") or 0)
    duration = _to_int(it.get("Duration") or 0)
    if not duration and file_size and bitrate:
        duration = int(round(file_size * 8 / (bitrate * 1000)))
    return {
        "id": f"kugou:{sid}",
        "source": "kugou",
        "hash": sid,
        "title": title,
        "artist": artist,
        "artists": artists,
        "album": album_name,
        "album_id": album_id,
        "duration_s": duration,
        "ext": ext,
        "cover_url": cover,
        "union_cover": cover,
        "file_size": file_size,
        "bitrate": bitrate,
        "lyric": "",
    }


async def search(keyword: str, limit: int = 30, page: int = 1) -> dict:
    """调 KuGouMusicApi /search，返回统一结果对象，包含 items 和 total。"""
    if not keyword:
        return {"items": [], "total": 0, "page": page, "pagesize": limit}
    auth = _auth_header()
    logger.warning("[KUGOU] keyword=%r limit=%d auth_len=%d",
                   keyword, limit, len(auth or ""))
    try:
        async with _client() as c:
            lists: list[dict] = []
            raw: dict = {}
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
        raw_item = search_item_to_raw(it)
        if not raw_item:
            continue
        items.append(raw_item)
        _remember_song(raw_item)

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


async def get_artist_audios(artist_id: int | str, sort: str = "hot", page: int = 1, pagesize: int = 60) -> dict:
    """调 KuGouMusicApi /artist/audios，返回统一结果对象。

    接口：/artist/audios?id=3520&sort=hot&page=1&pagesize=60
    回参：{"total": n, "error_code": 0, "data": [...]}。
    """
    aid = str(artist_id or "").strip()
    if not aid:
        return {"items": [], "total": 0, "page": page, "pagesize": pagesize}
    try:
        async with _client() as c:
            r = await c.get(
                "/artist/audios",
                params={"id": aid, "sort": sort or "hot", "page": page, "pagesize": pagesize},
            )
            if r.status_code != 200:
                logger.warning("[KUGOU] artist/audios http=%s aid=%s body=%r", r.status_code, aid, r.text[:200])
                return {"items": [], "total": 0, "page": page, "pagesize": pagesize}
            body = r.json()
            if not isinstance(body, dict):
                logger.warning("[KUGOU] artist/audios body type=%s aid=%s body=%r", type(body).__name__, aid, str(body)[:200])
                return {"items": [], "total": 0, "page": page, "pagesize": pagesize}
            data = body.get("data") or []
            if isinstance(data, dict):
                data = data.get("list") or data.get("items") or []
            raw_items = data if isinstance(data, list) else []
            items: list[dict] = []
            for it in raw_items:
                if not isinstance(it, dict):
                    continue
                raw_item = search_item_to_raw(it)
                if not raw_item:
                    continue
                raw_item["release_date"] = str(it.get("publish_date") or it.get("publishDate") or "").strip()
                items.append(raw_item)
                _remember_song(raw_item)
            try:
                total = int(body.get("total") or 0)
            except (TypeError, ValueError):
                total = len(items)
            return {
                "items": items,
                "total": max(total, len(items)),
                "page": page,
                "pagesize": pagesize,
                "artist_id": aid,
            }
    except Exception as e:
        logger.warning("[KUGOU] artist/audios error aid=%s: %s", aid, e)
        return {"items": [], "total": 0, "page": page, "pagesize": pagesize}


async def search_albums_raw(keyword: str, limit: int = 30, page: int = 1) -> dict:
    """调 KuGouMusicApi /search?type=album，**原始透传** lists。

    与 search() 不同：不做 search_item_to_raw 映射（那是歌曲口径），也不
    写 _search_index——/album/detail 聚合要的是专辑原字段。

    回参 data.lists[] 真实字段：albumid / albumname / singers[{name,id}] /
    singerids[] / img(完整 240 URL) / songcount / publish_time / language /
    intro / company / title / quality / category / special_tag
    """
    kw = str(keyword or "").strip()
    if not kw:
        return {"lists": [], "total": 0, "page": page, "pagesize": limit, "keyword": ""}
    try:
        async with _client() as c:
            r = await c.get(
                "/search",
                params={"keywords": kw, "page": page, "pagesize": limit, "type": "album"},
            )
            if r.status_code != 200:
                logger.warning("[KUGOU] search/type=album http=%s kw=%r body=%r",
                               r.status_code, kw, r.text[:200])
                return {"lists": [], "total": 0, "page": page, "pagesize": limit, "keyword": kw}
            body = r.json()
            status = body.get("status", body.get("error_code"))
            if status not in (1, 0, 200, None):
                logger.warning("[KUGOU] search/type=album status=%s kw=%r errmsg=%r",
                               status, kw, body.get("error_msg"))
                return {"lists": [], "total": 0, "page": page, "pagesize": limit, "keyword": kw}
            data = body.get("data") or {}
            lists = data.get("lists") or [] if isinstance(data, dict) else []
            lists = [x for x in lists if isinstance(x, dict)] if isinstance(lists, list) else []
            try:
                total = int(data.get("total") or 0)
            except (TypeError, ValueError):
                total = 0
            logger.warning("[KUGOU] search/type=album kw=%r got=%d total=%d",
                           kw, len(lists), total)
            return {
                "lists": lists,
                "total": max(total, len(lists)),
                "page": page,
                "pagesize": limit,
                "keyword": kw,
            }
    except Exception as e:
        logger.warning("[KUGOU] search/type=album error kw=%r: %s", kw, e)
        return {"lists": [], "total": 0, "page": page, "pagesize": limit, "keyword": kw}


async def get_album_songs(album_id: int | str, page: int = 1, pagesize: int = 50) -> dict:
    """调 KuGouMusicApi /album/songs?id=<album_id>&page=&pagesize=。

    pagesize 上限 50：超过即返回 {"errmsg":"invalid param","status":0} 空结果，
    是硬上限不是截断。这里主动封顶，避免飞牛侧传 size=120 触发无效请求。

    回参 data 真实字段（实测 12799221 / 182443696 一致）：
      { "songs": [嵌套结构], "total": n }
    songs[] 元素是嵌套结构，不是扁平字段：
      base.audio_name / base.author_name / base.album_id / base.audio_id
      authors[]: {author_name, author_id}   <- 带真实歌手 ID
      audio_info.duration(ms) / .bitrate / .filesize / .extname / .hash
                / .hash_320 / .hash_128 / .hash_flac
      album_info.album_name / album_info.cover(带 {size} 占位符)
      trans_param.union_cover(带 {size} 占位符) / trans_param.songname_suffix(如 "(伴奏)")
      extend.disc / extend.sort

    不能复用 search_item_to_raw：它按扁平字段读（name/singername/timelength/albumid），
    嵌套结构全读空；字段名与口径也不同。这里自己映射成 /artist/audios 口径的统一
    内部格式，再交给 build_online_track。

    兼容其他可能形态：data.songList / data.list / data.lists / 扁平字段兜底。
    """
    aid = str(album_id or "").strip()
    if not aid:
        return {"items": [], "total": 0, "page": page, "pagesize": pagesize, "album_id": ""}
    if pagesize > 50:
        pagesize = 50
    empty = {"items": [], "total": 0, "page": page, "pagesize": pagesize, "album_id": aid}
    try:
        async with _client() as c:
            r = await c.get("/album/songs",
                            params={"id": aid, "page": page, "pagesize": pagesize})
            if r.status_code != 200:
                logger.warning("[KUGOU] album/songs http=%s album=%s body=%r",
                               r.status_code, aid, r.text[:200])
                return empty
            body = r.json()
            status = body.get("status", body.get("error_code"))
            errmsg = body.get("errmsg") or body.get("error_msg") or ""
            if status not in (1, 0, 200, None):
                logger.warning("[KUGOU] album/songs status=%s album=%s errmsg=%r",
                               status, aid, errmsg)
                return empty
            data = body.get("data") or {}
            # 嵌套结构：data.songs / data.songList / data.list / data.lists
            song_list = data.get("songs") or data.get("songList") or data.get("songlist") or []
            if not isinstance(song_list, list) or not song_list:
                song_list = data.get("list") or data.get("lists") or []
            song_list = [x for x in song_list if isinstance(x, dict)] if isinstance(song_list, list) else []
            total = _to_int(data.get("total") or 0)
            if total <= 0:
                total = len(song_list)
            # pagesize 超限时酷狗返 errmsg="invalid param" 并空结果。
            if not song_list and errmsg:
                logger.warning("[KUGOU] album/songs empty errmsg=%r album=%s pagesize=%d",
                               errmsg, aid, pagesize)
            logger.warning("[KUGOU] album/songs album=%s got=%d total=%d errmsg=%r",
                           aid, len(song_list), total, errmsg)
    except Exception as e:
        logger.warning("[KUGOU] album/songs error album=%s: %s", aid, e)
        return empty

    items: list[dict] = []
    for it in song_list:
        # 嵌套结构优先：base / audio_info / album_info / authors / trans_param / extend
        base = it.get("base") or {}
        audio_info = it.get("audio_info") or {}
        album_info = it.get("album_info") or {}
        tp = it.get("trans_param") or {}
        sid = str(audio_info.get("hash") or audio_info.get("hash_320")
                  or audio_info.get("hash_128") or base.get("audio_id")
                  or it.get("hash") or it.get("filehash") or "").strip()
        if not sid:
            continue

        # 歌手：authors[] 带真实 author_id，可直接生成 artist guid。
        authors_raw = it.get("authors") or []
        singers: list[dict] = []
        if isinstance(authors_raw, list) and authors_raw:
            for a in authors_raw:
                if not isinstance(a, dict):
                    continue
                nm = str(a.get("author_name") or a.get("name") or "").strip()
                if not nm:
                    continue
                ai_s = str(a.get("author_id") or a.get("id") or "").strip()
                singers.append({"name": nm, "id": ai_s})
        if not singers:
            singer_name = str(it.get("singername") or base.get("author_name") or "").strip()
            singers = [
                {"name": part.strip(), "id": ""}
                for part in re.split(r"[、,，/]", singer_name) if part.strip()
            ]
        singer_name = "、".join(str(s.get("name") or "").strip() for s in singers
                                if str(s.get("name") or "").strip())

        raw_name = str(base.get("audio_name") or it.get("name") or "").strip()
        album_name = str(album_info.get("album_name") or it.get("albumname")
                         or it.get("album_name") or "").strip()
        title = _strip_singer_prefix_from_name(raw_name, [singer_name] if singer_name else [], album_name)
        # 曲名后缀（如 "(伴奏)"）拼在曲名后，供伴奏版本区分。
        suffix = str(tp.get("songname_suffix") or "").strip()
        if suffix and suffix not in title:
            title = f"{title} {suffix}"

        # audio_info.duration 是毫秒；兜底扁平 timelength 时 >10000 才当毫秒。
        duration_ms = _to_int(audio_info.get("duration") or audio_info.get("duration_320")
                              or audio_info.get("duration_128") or 0)
        if duration_ms <= 0:
            raw_tl = _to_int(it.get("timelength") or 0)
            duration_ms = raw_tl if raw_tl > 1000 else int(raw_tl * 1000)
        duration = duration_ms / 1000.0

        bitrate = _to_int(audio_info.get("bitrate") or it.get("bitrate") or 0)
        file_size = _to_int(audio_info.get("filesize_320") or audio_info.get("filesize")
                            or it.get("size") or it.get("filesize") or 0)
        if not duration and file_size and bitrate:
            duration = file_size * 8 / (bitrate * 1000)
        ext = str(audio_info.get("extname") or it.get("extname") or it.get("filetype")
                  or "mp3").strip().lower() or "mp3"
        cover = str(album_info.get("cover") or tp.get("union_cover")
                    or it.get("image") or "").strip()

        items.append({
            "id": f"kugou:{sid}",
            "source": "kugou",
            "hash": sid,
            "title": title,
            "artist": singer_name,
            "artists": singers,
            "album": album_name,
            "album_id": str(base.get("album_id") or it.get("albumid") or aid).strip(),
            "duration_s": duration,
            "ext": ext,
            "cover_url": cover,
            "union_cover": cover,
            "file_size": file_size,
            "bitrate": bitrate,
            "lyric": str(it.get("lyric") or "").strip(),
            "release_date": str(it.get("publish_time") or it.get("publish_date") or "").strip(),
            "disc_no": _to_int((it.get("extend") or {}).get("disc") or 0) or None,
            "track_no": _to_int((it.get("extend") or {}).get("sort") or 0) or None,
        })
    return {"items": items, "total": total, "page": page, "pagesize": pagesize, "album_id": aid}

async def get_album_detail_bundle(album_id: int | str) -> dict:
    """/album/detail 聚合：detail 拿歌手名+专辑名 → search?type=album → 按 albumid 匹配。

    先取 detail 是因为 /search 的 keywords 必须有歌手名和专辑名，而这两个
    字段 /album/detail?id=<id> 已经给全（author_name + album_name），不需要外部传入。

    返回 {"detail": <酷狗 detail 原样>, "match": <search 命中的原始条目或 None>}。
    detail 与 search 串行依赖（keywords 来自 detail），无法并行。
    """
    aid = str(album_id or "").strip()
    if not aid:
        return {"detail": {}, "match": None, "album_id": ""}

    now = time.monotonic()
    cached = _ALBUM_DETAIL_CACHE.get(aid)
    if cached and now - cached[0] < _ALBUM_DETAIL_TTL_S:
        return cached[1]

    detail = await get_album_detail(aid)
    author = str(detail.get("author_name") or detail.get("authorName") or "").strip()
    name = str(detail.get("album_name") or detail.get("albumName") or detail.get("album") or "").strip()
    if not (author or name):
        logger.warning("[KUGOU_ALBUM_DETAIL] empty name album_id=%s detail=%r", aid, str(detail)[:150])
        return {"detail": detail, "match": None, "album_id": aid}

    # 关键词：优先「歌手+专辑名」拼接。实测只搜专辑名会撞同名专辑
    # （keywords=小心思 → total=500 且首位不是本专辑）；带上歌手名 total=1 精准命中。
    candidates = []
    kw = author + name
    if kw:
        candidates.append(kw)
    if f"{author} {name}" and f"{author} {name}" not in candidates:
        candidates.append(f"{author} {name}")
    if name and name not in candidates:
        candidates.append(name)

    match = None
    for kw in candidates:
        res = await search_albums_raw(kw, limit=30)
        lists = res.get("lists") or []
        hit = next((x for x in lists if str(x.get("albumid")) == aid), None)
        if hit:
            match = hit
            break
        # 只有单一候选且未命中时才继续尝试；避免多候选串行拖慢失败路径。
        if len(candidates) <= 1:
            break

    out = {"detail": detail, "match": match, "album_id": aid}
    _ALBUM_DETAIL_CACHE[aid] = (now, out)
    return out


async def get_album_detail(album_id: int | str) -> dict:
    """调 KuGouMusicApi /album/detail，返回专辑详情。

    接口：/album/detail?id=180668003
    回参：{"status": 1, "error_code": 0, "errmsg": "", "data": [ {...} ]}

    data[] 单条字段（酷狗原始输出）：
      album_id / album_name / sizable_cover(带 {size} 占位符)
      cover(仅文件名，不能用) / author_name / authors[]
      publish_date / language / type / publish_company / heat / category / intro
    """
    aid = str(album_id or "").strip()
    if not aid:
        return {}
    try:
        async with _client() as c:
            r = await c.get("/album/detail", params={"id": aid})
            if r.status_code != 200:
                logger.warning("[KUGOU] album/detail http=%s album_id=%s body=%r",
                               r.status_code, aid, r.text[:200])
                return {}
            body = r.json()
            if not isinstance(body, dict):
                logger.warning("[KUGOU] album/detail body type=%s album_id=%s body=%r",
                               type(body).__name__, aid, str(body)[:200])
                return {}
            status = body.get("status", body.get("error_code"))
            if status not in (1, 0, 200, None):
                logger.warning("[KUGOU] album/detail status=%s album_id=%s errmsg=%r",
                               status, aid, body.get("errmsg"))
                return {}
            data = body.get("data") or []
            if isinstance(data, dict):
                data = data.get("list") or data.get("items") or []
            if not isinstance(data, list) or not data:
                logger.warning("[KUGOU] album/detail empty data album_id=%s body=%r", aid, str(body)[:200])
                return {}
            first = data[0]
            return first if isinstance(first, dict) else {}
    except Exception as e:
        logger.warning("[KUGOU] album/detail error album_id=%s: %s", aid, e)
        return {}


async def get_artist_albums(artist_id: int | str, page: int = 1, pagesize: int = 60) -> dict:
    """调 KuGouMusicApi /artist/albums，返回该歌手真实专辑列表。

    接口：/artist/albums?id=6539&page=1&pagesize=60
    回参：{"total": n, "error_code": 0, "status": 1, "data": [...],
           "extra": {"page_total": n}, "errmsg": "success"}

    data[] 真实字段（酷狗原始输出，与 /artist/audios 不同）：
      album_id / album_name / author_name(可能多人，顿号分隔)
      authors: [{"author_name": 名, "author_id": 数字ID}]  ← 带真实歌手 ID
      sizable_cover(带 {size} 占位符) / cover(文件名)
      publish_date(YYYY-MM-DD) / language / type(单曲专辑|原声带|录音室专辑|...)
      heat / category / grade / quality / is_publish / publish_company / intro
    """
    aid = str(artist_id or "").strip()
    if not aid:
        return {"items": [], "total": 0, "page": page, "pagesize": pagesize}
    try:
        async with _client() as c:
            r = await c.get(
                "/artist/albums",
                params={"id": aid, "page": page, "pagesize": pagesize},
            )
            if r.status_code != 200:
                logger.warning("[KUGOU] artist/albums http=%s aid=%s body=%r", r.status_code, aid, r.text[:200])
                return {"items": [], "total": 0, "page": page, "pagesize": pagesize}
            body = r.json()
            if not isinstance(body, dict):
                logger.warning("[KUGOU] artist/albums body type=%s aid=%s body=%r", type(body).__name__, aid, str(body)[:200])
                return {"items": [], "total": 0, "page": page, "pagesize": pagesize}
            status = body.get("status", body.get("error_code"))
            if status not in (1, 0, 200, None):
                logger.warning("[KUGOU] artist/albums status=%s aid=%s errmsg=%r", status, aid, body.get("errmsg"))
                return {"items": [], "total": 0, "page": page, "pagesize": pagesize}
            data = body.get("data") or []
            if isinstance(data, dict):
                data = data.get("list") or data.get("items") or []
            raw_items = data if isinstance(data, list) else []
            items: list[dict] = []
            for it in raw_items:
                if not isinstance(it, dict):
                    continue
                item = album_item_to_raw(it)
                if not item:
                    continue
                items.append(item)
            try:
                total = int(body.get("total") or 0)
            except (TypeError, ValueError):
                total = 0
            if not total:
                extra = body.get("extra") or {}
                if isinstance(extra, dict):
                    try:
                        total = int(extra.get("page_total") or 0)
                    except (TypeError, ValueError):
                        total = 0
            return {
                "items": items,
                "total": max(total, len(items)),
                "page": page,
                "pagesize": pagesize,
                "artist_id": aid,
            }
    except Exception as e:
        logger.warning("[KUGOU] artist/albums error aid=%s: %s", aid, e)
        return {"items": [], "total": 0, "page": page, "pagesize": pagesize}


def album_item_to_raw(it: dict) -> dict:
    """把 /artist/albums 单条专辑映射为内部统一格式。

    与 track_item_to_raw 不同：这里 authors 是数组且带真实 author_id，
    歌手 ID 不用再反查。
    """
    if not isinstance(it, dict):
        return {}
    album_id = _pick_ci(it, "album_id", "albumId", "AlbumID", "albumID")
    album_name = _pick_ci(it, "album_name", "albumName", "albumname", "AlbumName")
    if not album_id and not album_name:
        return {}

    # authors 数组优先（带 author_id）；回退到 author_name 字符串拆分。
    artists: list[dict] = []
    seen_names: set[str] = set()
    authors = it.get("authors")
    if isinstance(authors, list):
        for au in authors:
            if not isinstance(au, dict):
                continue
            name = str(au.get("author_name") or au.get("authorName") or au.get("name") or "").strip()
            if not name or name in seen_names:
                continue
            author_id = str(au.get("author_id") or au.get("authorId") or au.get("AuthorID") or "").strip()
            artists.append({"name": name, "id": author_id})
            seen_names.add(name)
    if not artists:
        singer_text = _pick_ci(it, "author_name", "authorName", "singername", "singername",
                               "SingerName", "artist", "author")
        for name in _split_singers(singer_text):
            if name and name not in seen_names:
                artists.append({"name": name, "id": ""})
                seen_names.add(name)

    # 封面：sizable_cover 带 {size} 占位符；顶层 cover 只是文件名，不能用。
    cover_url = _pick_ci(it, "sizable_cover", "sizableCover", "SizableCover",
                         "sizable_cover_url", "coverUrl", "Image", "Image_300x300")
    if not cover_url:
        cover_url = _pick_ci(it, "cover", "pic", "avatar")

    return {
        "id": f"kugou:album:{album_id}" if album_id else "",
        "source": "kugou",
        "album_id": album_id,
        "album": album_name,
        "album_name": album_name,
        "artists": artists,
        "artist": "、".join(a["name"] for a in artists if a["name"]),
        "cover_url": cover_url,
        "union_cover": cover_url,
        "publish_date": _pick_ci(it, "publish_date", "publishDate", "PublishDate", "pub_date"),
        "language": _pick_ci(it, "language", "Language"),
        "album_type": _pick_ci(it, "type", "album_type", "albumType"),
        "publish_company": _pick_ci(it, "publish_company", "publishCompany", "company"),
        "heat": _to_int(it.get("heat") or 0),
        "category": _to_int(it.get("category") or 0),
        "intro": _pick_ci(it, "intro", "album_intro", "desc", "description"),
        "is_publish": _to_int(it.get("is_publish") if it.get("is_publish") is not None else 1),
    }


def _split_singers(text: str) -> list[str]:
    """拆分酷狗顿号/逗号分隔的多歌手字符串。"""
    out: list[str] = []
    for part in re.split(r"[、,，/]+", str(text or "")):
        name = part.strip()
        if name:
            out.append(name)
    return out


def _field(it: dict, keys: tuple[str, ...], default: Any = "") -> Any:
    """按 KuGouMusicApi 常见大小写/命名兼容取字段。"""
    for k in keys:
        if it.get(k) not in (None, ""):
            return it.get(k)
    return default


def _to_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0






def _album_info_id(albuminfo: Any) -> str:
    """酷狗歌单 track/all 正确格式：albuminfo.id。"""
    if isinstance(albuminfo, dict):
        return str(albuminfo.get("id") or albuminfo.get("albumid") or albuminfo.get("albumId") or "").strip()
    return ""


def _album_name_from_albuminfo(albuminfo: Any) -> str:
    """酷狗歌单 track/all 正确格式：albuminfo.name。"""
    if isinstance(albuminfo, dict):
        return str(albuminfo.get("name") or "").strip()
    return ""


def _timelen_to_seconds(value: Any) -> float:
    """酷狗 timelen 是毫秒；兼容已是秒数的旧数据。"""
    ms = _to_int(value)
    if ms <= 0:
        return 0.0
    # 若数值明显是毫秒（大于 1000），按毫秒转秒；否则按秒处理。
    return ms / 1000 if ms > 1000 else float(ms)




def _strip_singer_prefix_from_name(name: str, singer_names: list[str], album_name: str) -> str:
    """去掉酷狗 song name 里的歌手前缀：例如 "球球、白小白 - 宠爱吖" -> "宠爱吖"。"""
    title = str(name or "").strip()
    if not title:
        return title
    # 酷狗常见格式：歌手、歌手 - 歌名；先按最后一个 " - " 切分，右侧通常就是歌名。
    if " - " in title:
        right = title.rsplit(" - ", 1)[1].strip()
        left = title.rsplit(" - ", 1)[0].strip()
        if right and (not left or left == left):  # 只要右侧非空，优先采用右侧歌名
            return right
    names = [str(x or "").strip() for x in singer_names if str(x or "").strip()]
    if not names:
        return title
    joined = "、".join(names)
    prefixes = {
        joined,
        "/".join(names),
        ",".join(names),
        "，".join(names),
        " - ".join(names),
        f"{joined} - {album_name}",
        f"{joined}-{album_name}",
    }
    for sep in (" - ", "-", "_", "–", "—", "|", ";", "；"):
        for prefix in prefixes:
            if title.startswith(prefix + sep) or title.startswith(prefix):
                cleaned = title[len(prefix):].lstrip(sep).lstrip("-_|;； ").strip()
                if cleaned:
                    return cleaned
    return title


def track_item_to_raw(it: dict) -> dict:
    """按酷狗 /playlist/track/all 的真实返回格式精确映射。

    真实字段：hash/name/singerinfo[]/albuminfo/timelen/extname/size/cover/bitrate。
    """
    if not isinstance(it, dict):
        return {}

    sid = str(it.get("hash") or "").strip()
    if not sid:
        return {}

    artists = []
    if isinstance(it.get("singerinfo"), list):
        for singer in it.get("singerinfo"):
            if isinstance(singer, dict):
                singer_name = str(singer.get("name") or "").strip()
                if singer_name:
                    artists.append(singer)
    singer_names = [str(x.get("name") or "").strip() for x in artists]
    album = _album_name_from_albuminfo(it.get("albuminfo"))
    artist = "、".join(singer_names)
    title = _strip_singer_prefix_from_name(str(it.get("name") or ""), singer_names, album)
    duration_s = _timelen_to_seconds(it.get("timelen"))
    ext = _format_from_any(it)
    cover = str(it.get("cover") or "").strip()
    if not cover:
        trans_param = it.get("trans_param")
        if isinstance(trans_param, dict):
            cover = str(trans_param.get("union_cover") or "").strip()
    file_size = _to_int(it.get("size"))
    bitrate = _to_int(it.get("bitrate"))

    return {
        "id": f"kugou:{sid}",
        "source": "kugou",
        "hash": sid,
        "title": title,
        "artist": artist,
        "artists": artists,
        "album": album,
        "album_id": _album_info_id(it.get("albuminfo")),
        "duration_s": duration_s,
        "ext": ext,
        "cover_url": cover,
        "union_cover": cover,
        "file_size": file_size,
        "bitrate": bitrate,
        "lyric": "",
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
    """从内存索引拿元数据。搜过的歌直接命中；未搜过则用 /privilege/lite?hash= 回源。"""
    if not song_id:
        return None
    cached = _search_index.get(song_id)
    if cached:
        return dict(cached)
    # 未搜过（直接进入详情页），用 /privilege/lite?hash= 拉元数据
    lite_info = await fetch_privilege_lite_info(song_id)
    if lite_info:
        return lite_info
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


async def fetch_privilege_lite_info(song_id: str) -> dict | None:
    """从 /privilege/lite?hash= 拉一首歌的元数据并映射到统一格式。"""
    if not song_id:
        return None
    try:
        async with _client() as c:
            r = await c.get(
                "/privilege/lite",
                params={"hash": song_id},
            )
            if r.status_code == 200:
                body = r.json()
                status = body.get("status", body.get("code"))
                if status not in (1, 200, 0):
                    logger.warning("[KUGOU] privilege_lite status=%s hash=%s body=%r", status, song_id, str(body)[:300])
                    return None
                data = body.get("data") or body
                if isinstance(data, list) and data and isinstance(data[0], dict):
                    data = data[0]
                if not isinstance(data, dict):
                    return None
                cover_url = ""
                info_obj = data.get("info")
                if isinstance(info_obj, dict):
                    cover_url = str(info_obj.get("image") or "").strip()
                tp = data.get("trans_param")
                if isinstance(tp, dict) and not cover_url:
                    cover_url = str(tp.get("union_cover") or "").strip()
                raw_item = search_item_to_raw(data)
                if raw_item:
                    if cover_url and not raw_item.get("cover_url"):
                        raw_item["cover_url"] = cover_url
                        raw_item["union_cover"] = cover_url
                    _remember_song(raw_item)
                    return raw_item
    except Exception as e:
        logger.warning("privilege_lite %s err=%s", song_id, e)
    return None


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
