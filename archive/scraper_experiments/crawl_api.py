#!/usr/bin/env python3
"""
小红书 API 爬虫（mnsv2签名 + 增量保存 + 断点续爬）

用法：
    python scraper/crawl_api.py --test 3        # 测试3个博主
    python scraper/crawl_api.py                  # 全量（断点续跑）
    python scraper/crawl_api.py --status         # 进度
    python scraper/crawl_api.py --phase detail   # 只跑详情阶段
    python scraper/crawl_api.py --phase comments # 只跑评论阶段
"""

import argparse
import asyncio
import csv
import ctypes
import hashlib
import json
import os
import random
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import quote

import httpx
from playwright.async_api import async_playwright

from spider_xhs_adapter import import_creator_from_spider_xhs

sys.stdout.reconfigure(line_buffering=True)

BASE_DIR = Path(__file__).parent.parent
NOTES_DB = BASE_DIR / "data" / "notes.db"
PROGRESS_DB = BASE_DIR / "data" / "crawl_progress.db"
CREATOR_IDS_FILE = BASE_DIR / "creator_ids.txt"
COOKIE_FILE = BASE_DIR / "data" / "xhs_cookie.txt"

PROXY = "http://127.0.0.1:7890"
COOKIE_STR = "a1=19b59fabb6bw93jydbj5sf9kdgrbbg27yx5f4hszh30000101682; web_session=040069b522744d19141ec9def63b4bdacdfdb9; webId=c4977872fab544a22e28fe3af3e3b242; xsecappid=xhs-pc-web"
API_HOST = "https://edith.xiaohongshu.com"
DEFAULT_COOKIE_REFRESH_EVERY = 50
DEFAULT_FALLBACK_CONSEC_ERRORS = 10

# === Inline sign functions (from test_new_sign.py, proven working) ===
BASE64_CHARS = list("ZmserbBoHQtNP+wOcza/LpngG8yJq42KWYj0DSfdikx3VT16IlUAFM97hECvuRX5")
CRC32_TABLE = [0,1996959894,3993919788,2567524794,124634137,1886057615,3915621685,2657392035,249268274,2044508324,3772115230,2547177864,162941995,2125561021,3887607047,2428444049,498536548,1789927666,4089016648,2227061214,450548861,1843258603,4107580753,2211677639,325883990,1684777152,4251122042,2321926636,335633487,1661365465,4195302755,2366115317,997073096,1281953886,3579855332,2724688242,1006888145,1258607687,3524101629,2768942443,901097722,1119000684,3686517206,2898065728,853044451,1172266101,3705015759,2882616665,651767980,1373503546,3369554304,3218104598,565507253,1454621731,3485111705,3099436303,671266974,1594198024,3322730930,2970347812,795835527,1483230225,3244367275,3060149565,1994146192,31158534,2563907772,4023717930,1907459465,112637215,2680153253,3904427059,2013776290,251722036,2517215374,3775830040,2137656763,141376813,2439277719,3865271297,1802195444,476864866,2238001368,4066508878,1812370925,453092731,2181625025,4111451223,1706088902,314042704,2344532202,4240017532,1658658271,366619977,2362670323,4224994405,1303535960,984961486,2747007092,3569037538,1256170817,1037604311,2765210733,3554079995,1131014506,879679996,2909243462,3663771856,1141124467,855842277,2852801631,3708648649,1342533948,654459306,3188396048,3373015174,1466479909,544179635,3110523913,3462522015,1591671054,702138776,2966460450,3352799412,1504918807,783551873,3082640443,3233442989,3988292384,2596254646,62317068,1957810842,3939845945,2647816111,81470997,1943803523,3814918930,2489596804,225274430,2053790376,3826175755,2466906013,167816743,2097651377,4027552580,2265490386,503444072,1762050814,4150417245,2154129355,426522225,1852507879,4275313526,2312317920,282753626,1742555852,4189708143,2394877945,397917763,1622183637,3604390888,2714866558,953729732,1340076626,3518719985,2797360999,1068828381,1219638859,3624741850,2936675148,906185462,1090812512,3747672003,2825379669,829329135,1181335161,3412177804,3160834842,628085408,1382605366,3423369109,3138078467,570562233,1426400815,3317316542,2998733608,733239954,1555261956,3268935591,3050360625,752459403,1541320221,2607071920,3965973030,1969922972,40735498,2617837225,3943577151,1913087877,83908371,2512341634,3803740692,2075208622,213261112,2463272603,3855990285,2094854071,198958881,2262029012,4057260610,1759359992,534414190,2176718541,4139329115,1873836001,414664567,2282248934,4279200368,1711684554,285281116,2405801727,4167216745,1634467795,376229701,2685067896,3608007406,1308918612,956543938,2808555105,3495958263,1231636301,1047427035,2932959818,3654703836,1088359270,936918000,2847714899,3736837829,1202900863,817233897,3183342108,3401237130,1404277552,615818150,3134207493,3453421203,1423857449,601450431,3009837614,3294710456,1567103746,711928724,3020668471,3272380065,1510334235,755167117]

def _rsu(num, bit=0):
    return ctypes.c_uint32(num).value >> bit

def mrc(e):
    o = -1
    for n in range(min(57, len(e))):
        o = CRC32_TABLE[(o & 255) ^ ord(e[n])] ^ _rsu(o, 8)
    return o ^ -1 ^ 3988292384

def _t2b64(e):
    return BASE64_CHARS[(e>>18)&63]+BASE64_CHARS[(e>>12)&63]+BASE64_CHARS[(e>>6)&63]+BASE64_CHARS[e&63]

def _encode_chunk(data, start, end):
    r = []
    for i in range(start, end, 3):
        c = ((data[i]<<16)&0xFF0000)+((data[i+1]<<8)&0xFF00)+(data[i+2]&0xFF)
        r.append(_t2b64(c))
    return "".join(r)

def encode_utf8(s):
    enc = quote(s, safe="~()*!.'")
    r, i = [], 0
    while i < len(enc):
        if enc[i] == "%":
            r.append(int(enc[i+1:i+3], 16)); i += 3
        else:
            r.append(ord(enc[i])); i += 1
    return r

def b64_encode(data):
    ln = len(data); rem = ln % 3; chunks = []
    ml = ln - rem
    for i in range(0, ml, 16383):
        chunks.append(_encode_chunk(data, i, min(i+16383, ml)))
    if rem == 1:
        a = data[-1]; chunks.append(BASE64_CHARS[a>>2]+BASE64_CHARS[(a<<4)&63]+"==")
    elif rem == 2:
        a = (data[-2]<<8)+data[-1]; chunks.append(BASE64_CHARS[a>>10]+BASE64_CHARS[(a>>4)&63]+BASE64_CHARS[(a<<2)&63]+"=")
    return "".join(chunks)

def get_trace_id():
    return "".join(random.choice("abcdef0123456789") for _ in range(16))

def build_sign_string(uri, data=None, method="POST"):
    if method.upper() == "POST":
        c = uri
        if data is not None:
            c += json.dumps(data, separators=(",",":"), ensure_ascii=False) if isinstance(data, dict) else str(data)
        return c
    else:
        if not data: return uri
        if isinstance(data, dict):
            parts = []
            for k in data:
                v = data[k]
                vs = ",".join(str(x) for x in v) if isinstance(v, list) else str(v) if v is not None else ""
                parts.append(f"{k}={quote(vs, safe='')}")
            return f"{uri}?{'&'.join(parts)}"
        return f"{uri}?{data}"

def build_xs(x3, dtype="object"):
    s = {"x0":"4.2.1","x1":"xhs-pc-web","x2":"Mac OS","x3":x3,"x4":dtype}
    return "XYS_"+b64_encode(encode_utf8(json.dumps(s, separators=(",",":"))))

def build_xs_common(a1, b1, x_s, x_t):
    p = {"s0":3,"s1":"","x0":"1","x1":"4.2.2","x2":"Mac OS","x3":"xhs-pc-web","x4":"4.74.0","x5":a1,"x6":x_t,"x7":x_s,"x8":b1,"x9":mrc(x_t+x_s+b1),"x10":154,"x11":"normal"}
    return b64_encode(encode_utf8(json.dumps(p, separators=(",",":"))))

async def sign_with_playwright(page, uri, data=None, a1="", method="POST"):
    try:
        ls = await page.evaluate("() => window.localStorage")
        b1 = ls.get("b1", "") if ls else ""
    except:
        b1 = ""
    sign_str = build_sign_string(uri, data, method)
    md5_str = hashlib.md5(sign_str.encode()).hexdigest()
    sign_str_esc = sign_str.replace("\\","\\\\").replace("'","\\'").replace("\n","\\n")
    md5_esc = md5_str.replace("\\","\\\\").replace("'","\\'")
    try:
        x3 = await page.evaluate(f"window.mnsv2('{sign_str_esc}', '{md5_esc}')")
        x3 = x3 or ""
    except:
        x3 = ""
    dtype = "object" if isinstance(data, (dict, list)) else "string"
    x_s = build_xs(x3, dtype)
    x_t = str(int(time.time()*1000))
    return {
        "x-s": x_s, "x-t": x_t,
        "x-s-common": build_xs_common(a1, b1, x_s, x_t),
        "x-b3-traceid": get_trace_id(),
    }
# === End inline sign functions ===


def parse_cookie(s):
    d = {}
    for item in s.split(";"):
        item = item.strip()
        if "=" in item:
            k, v = item.split("=", 1)
            d[k.strip()] = v.strip()
    return d


def load_cookie_str():
    env_cookie = os.environ.get("XHS_COOKIE", "").strip()
    if env_cookie:
        return env_cookie

    if COOKIE_FILE.exists():
        file_cookie = COOKIE_FILE.read_text(encoding="utf-8").strip()
        if file_cookie:
            return file_cookie

    return COOKIE_STR


def load_cookie_state():
    cookie_str = load_cookie_str()
    cookie_dict = parse_cookie(cookie_str)
    return cookie_str, cookie_dict, cookie_dict.get("a1", "")

BASE_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json;charset=UTF-8",
    "origin": "https://www.xiaohongshu.com",
    "referer": "https://www.xiaohongshu.com/",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
}


# === DB init ===
def load_creator_ids():
    """Load creator IDs from creator_ids.txt."""
    if not CREATOR_IDS_FILE.exists():
        raise FileNotFoundError(f"creator_ids.txt 不存在: {CREATOR_IDS_FILE}")

    ids = []
    with open(CREATOR_IDS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            user_id = line.strip()
            if user_id:
                ids.append(user_id)
    return ids


def init_db():
    # notes.db
    nc = sqlite3.connect(NOTES_DB)
    nc.execute("""
        CREATE TABLE IF NOT EXISTS notes (
            note_id TEXT PRIMARY KEY, creator_id TEXT, creator_name TEXT,
            title TEXT, content TEXT, note_type TEXT,
            liked_count TEXT, collected_count TEXT, comment_count TEXT, share_count TEXT,
            tags TEXT, ip_location TEXT, time INTEGER, last_update_time INTEGER, crawled_at TEXT
        )
    """)
    nc.execute("CREATE INDEX IF NOT EXISTS idx_creator ON notes(creator_id)")
    nc.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT PRIMARY KEY, note_id TEXT, user_name TEXT, user_id TEXT,
            content TEXT, like_count TEXT, sub_comment_count TEXT,
            create_time TEXT, ip_location TEXT, crawled_at TEXT
        )
    """)
    nc.execute("CREATE INDEX IF NOT EXISTS idx_comment_note ON comments(note_id)")
    nc.execute("""
        CREATE TABLE IF NOT EXISTS note_progress (
            note_id TEXT PRIMARY KEY, creator_id TEXT, xsec_token TEXT,
            detail_done INTEGER DEFAULT 0, comments_done INTEGER DEFAULT 0
        )
    """)
    nc.execute("CREATE INDEX IF NOT EXISTS idx_np_creator ON note_progress(creator_id)")
    nc.commit()

    # crawl_progress.db — add columns if missing
    pc = sqlite3.connect(PROGRESS_DB)
    cols = {r[1] for r in pc.execute("PRAGMA table_info(creator_progress)").fetchall()}
    if "notes_cursor" not in cols:
        pc.execute("ALTER TABLE creator_progress ADD COLUMN notes_cursor TEXT DEFAULT ''")
    if "api_phase" not in cols:
        pc.execute("ALTER TABLE creator_progress ADD COLUMN api_phase TEXT DEFAULT 'list'")
    pc.commit()
    return nc, pc


def sync_creator_progress(pc):
    """Sync creator_progress with creator_ids.txt and recover resumable states."""
    pc.executemany(
        """
        INSERT OR IGNORE INTO creator_progress
        (user_id, name, expected_notes, status, actual_notes, notes_cursor, api_phase)
        VALUES (?, '', 0, 'pending', 0, '', 'list')
        """,
        [(user_id,) for user_id in load_creator_ids()],
    )
    pc.execute(
        """
        UPDATE creator_progress
        SET status='pending'
        WHERE status='done' AND COALESCE(api_phase, 'list') != 'done'
        """
    )
    pc.execute("UPDATE creator_progress SET status='pending' WHERE status='running'")
    pc.execute(
        """
        UPDATE creator_progress
        SET status='pending', error=NULL
        WHERE status='error' AND COALESCE(api_phase, 'list') != 'done'
        """
    )

    # Recover legacy "done" records that have notes but were never tracked at note/comment level.
    nc = sqlite3.connect(NOTES_DB)
    try:
        legacy_done_ids = [
            row[0]
            for row in pc.execute(
                "SELECT user_id FROM creator_progress WHERE status='done' AND api_phase='done'"
            ).fetchall()
        ]
        for user_id in legacy_done_ids:
            note_progress_count = nc.execute(
                "SELECT COUNT(*) FROM note_progress WHERE creator_id=?",
                (user_id,),
            ).fetchone()[0]
            comment_count = nc.execute(
                """
                SELECT COUNT(*)
                FROM comments
                WHERE note_id IN (SELECT note_id FROM notes WHERE creator_id=?)
                """,
                (user_id,),
            ).fetchone()[0]
            if note_progress_count == 0 and comment_count == 0:
                pc.execute(
                    """
                    UPDATE creator_progress
                    SET status='pending', api_phase='list', notes_cursor='', finished_at=NULL
                    WHERE user_id=?
                    """,
                    (user_id,),
                )
    finally:
        nc.close()
    pc.commit()


def export_csv():
    """Export notes and comments tables to CSV files."""
    if not NOTES_DB.exists():
        print("notes.db 不存在")
        return

    conn = sqlite3.connect(NOTES_DB)
    try:
        for table_name, out_name in (
            ("notes", "notes_export.csv"),
            ("comments", "comments_export.csv"),
        ):
            cursor = conn.execute(f"SELECT * FROM {table_name}")
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            out_path = BASE_DIR / "data" / out_name
            with open(out_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)
            print(f"已导出 {len(rows)} 条 {table_name} 到 {out_path}")
    finally:
        conn.close()


# === API functions ===
async def api_request(client, page, method, uri, params_or_data, cookie_dict, a1, retries=2):
    """Make signed API request with retry."""
    for attempt in range(retries + 1):
        signs = await sign_with_playwright(page, uri, params_or_data, a1, method)
        headers = {**BASE_HEADERS, "X-S": signs["x-s"], "X-T": signs["x-t"],
                   "x-S-Common": signs["x-s-common"], "X-B3-Traceid": signs["x-b3-traceid"]}
        try:
            if method == "GET":
                resp = await client.get(f"{API_HOST}{uri}", params=params_or_data, headers=headers, cookies=cookie_dict)
            else:
                resp = await client.post(f"{API_HOST}{uri}", json=params_or_data, headers=headers, cookies=cookie_dict)
            data = resp.json()
            code = data.get("code")

            if data.get("success"):
                return data["data"]

            if code in (-1, -100) and attempt < retries:
                wait = 5 * (attempt + 1)
                print(f"    code:{code}, retry in {wait}s...")
                await asyncio.sleep(wait)
                continue
            if code == 300011 and attempt < retries:
                print(f"    300011 签名失效, reload page...")
                await page.reload(wait_until="domcontentloaded")
                await page.wait_for_timeout(3000)
                continue
            if not data.get("success"):
                print(f"    API error: code={code}")
                return None
        except Exception as e:
            if attempt < retries:
                await asyncio.sleep(5)
            else:
                print(f"    Request error: {e}")
                return None
    return None


async def fetch_user_notes_page(client, page, user_id, cursor, cookie_dict, a1):
    """Fetch one page of user's notes."""
    uri = "/api/sns/web/v1/user_posted"
    params = {"num": "30", "cursor": cursor, "user_id": user_id, "image_scenes": "FD_WM_WEBP"}
    return await api_request(client, page, "GET", uri, params, cookie_dict, a1)


async def fetch_note_detail(client, page, note_id, xsec_token, cookie_dict, a1):
    """Fetch note detail via feed API."""
    uri = "/api/sns/web/v1/feed"
    data = {
        "source_note_id": note_id,
        "image_formats": ["jpg", "webp", "avif"],
        "extra": {"need_body_topic": 1},
        "xsec_source": "pc_user",
        "xsec_token": xsec_token,
    }
    return await api_request(client, page, "POST", uri, data, cookie_dict, a1)


async def fetch_comments_page(client, page, note_id, xsec_token, cursor, cookie_dict, a1):
    """Fetch one page of comments."""
    uri = "/api/sns/web/v2/comment/page"
    params = {"note_id": note_id, "cursor": cursor, "top_comment_id": "",
              "image_formats": "jpg,webp,avif", "xsec_token": xsec_token}
    return await api_request(client, page, "GET", uri, params, cookie_dict, a1)


# === Phase functions ===
async def phase_list(client, page, user_id, nc, pc, cookie_dict, a1):
    """Phase 1: Paginate note list, save note_ids to note_progress."""
    # Get saved cursor
    row = pc.execute("SELECT notes_cursor FROM creator_progress WHERE user_id=?", (user_id,)).fetchone()
    cursor = row[0] if row and row[0] else ""
    total_new = 0
    had_successful_page = False

    page_num = 0
    while True:
        page_num += 1
        result = await fetch_user_notes_page(client, page, user_id, cursor, cookie_dict, a1)
        if not result:
            print(f"    列表第{page_num}页失败，停止翻页")
            break

        had_successful_page = True
        notes = result.get("notes", [])
        print(f"    第{page_num}页: {len(notes)}篇, has_more={result.get('has_more')}")
        for n in notes:
            nid = n.get("note_id", "")
            xsec = n.get("xsec_token", "")
            if nid:
                nc.execute("INSERT OR IGNORE INTO note_progress (note_id, creator_id, xsec_token) VALUES (?,?,?)",
                           (nid, user_id, xsec))
                total_new += 1
        nc.commit()

        cursor = result.get("cursor", "")
        has_more = result.get("has_more", False)

        # Save cursor for resume
        pc.execute("UPDATE creator_progress SET notes_cursor=? WHERE user_id=?", (cursor, user_id))
        pc.commit()

        if not has_more or not cursor:
            break
        await asyncio.sleep(3)

    # Cooldown after list phase (rate limit recovery)
    if total_new > 60:
        print(f"    列表{total_new}篇，冷却60s...")
        await asyncio.sleep(60)
    elif total_new > 30:
        print(f"    列表{total_new}篇，冷却30s...")
        await asyncio.sleep(30)
    elif total_new > 0:
        await asyncio.sleep(15)

    if had_successful_page:
        pc.execute("UPDATE creator_progress SET api_phase='detail' WHERE user_id=?", (user_id,))
        pc.commit()
    return had_successful_page, total_new


async def phase_detail(client, page, user_id, nc, pc, cookie_dict, a1):
    """Phase 2: Fetch detail for each note missing detail."""
    pending = nc.execute(
        "SELECT note_id, xsec_token FROM note_progress WHERE creator_id=? AND detail_done=0",
        (user_id,)
    ).fetchall()

    success = 0
    consec_errors = 0
    completed = True

    # Probe: test if rate limit has lifted
    if pending:
        probe_id, probe_xsec = pending[0]
        probe = await fetch_note_detail(client, page, probe_id, probe_xsec, cookie_dict, a1)
        if probe and probe.get("items"):
            card = probe["items"][0].get("note_card", {})
            if card:
                # Probe succeeded, save it
                interact = card.get("interact_info", {})
                tags = [t.get("name", "") for t in card.get("tag_list", []) if isinstance(t, dict)]
                creator_name = card.get("user", {}).get("nickname", "")
                now = time.strftime("%Y-%m-%d %H:%M:%S")
                nc.execute("""
                    INSERT OR REPLACE INTO notes
                    (note_id, creator_id, creator_name, title, content, note_type,
                     liked_count, collected_count, comment_count, share_count,
                     tags, ip_location, time, last_update_time, crawled_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    probe_id, user_id, creator_name,
                    card.get("title", ""), card.get("desc", ""), card.get("type", ""),
                    str(interact.get("liked_count", "")), str(interact.get("collected_count", "")),
                    str(interact.get("comment_count", "")), str(interact.get("share_count", "")),
                    json.dumps(tags, ensure_ascii=False), card.get("ip_location", ""),
                    card.get("time", 0), card.get("last_update_time", 0), now,
                ))
                nc.execute("UPDATE note_progress SET detail_done=1 WHERE note_id=?", (probe_id,))
                nc.commit()
                success += 1
                pending = pending[1:]  # skip probe in main loop
        else:
            # Still rate limited, wait longer
            print(f"    限流中，等待60s恢复...")
            await asyncio.sleep(60)

    for note_id, xsec_token in pending:
        # Skip if already in notes table with content
        existing = nc.execute("SELECT content FROM notes WHERE note_id=?", (note_id,)).fetchone()
        if existing and existing[0]:
            nc.execute("UPDATE note_progress SET detail_done=1 WHERE note_id=?", (note_id,))
            nc.commit()
            success += 1
            continue

        # Progressive backoff on consecutive errors
        if consec_errors >= 3:
            wait = min(60, consec_errors * 10)
            print(f"    连续{consec_errors}次失败, 等待{wait}s...")
            await asyncio.sleep(wait)

        result = await fetch_note_detail(client, page, note_id, xsec_token, cookie_dict, a1)
        if result and result.get("items"):
            card = result["items"][0].get("note_card", {})
            if card:
                interact = card.get("interact_info", {})
                tags = [t.get("name", "") for t in card.get("tag_list", []) if isinstance(t, dict)]
                creator_name = card.get("user", {}).get("nickname", "")
                now = time.strftime("%Y-%m-%d %H:%M:%S")
                nc.execute("""
                    INSERT OR REPLACE INTO notes
                    (note_id, creator_id, creator_name, title, content, note_type,
                     liked_count, collected_count, comment_count, share_count,
                     tags, ip_location, time, last_update_time, crawled_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    note_id, user_id, creator_name,
                    card.get("title", ""), card.get("desc", ""), card.get("type", ""),
                    str(interact.get("liked_count", "")), str(interact.get("collected_count", "")),
                    str(interact.get("comment_count", "")), str(interact.get("share_count", "")),
                    json.dumps(tags, ensure_ascii=False), card.get("ip_location", ""),
                    card.get("time", 0), card.get("last_update_time", 0), now,
                ))
                nc.execute("UPDATE note_progress SET detail_done=1 WHERE note_id=?", (note_id,))
                nc.commit()
                success += 1
                consec_errors = 0
        else:
            consec_errors += 1
            if consec_errors >= 10:
                print(f"    连续10次失败，跳过剩余详情")
                completed = False
                break
        await asyncio.sleep(1.5)

    if completed:
        pc.execute("UPDATE creator_progress SET api_phase='comments' WHERE user_id=?", (user_id,))
        pc.commit()
    return completed, success, len(pending)


async def phase_comments(client, page, user_id, nc, pc, cookie_dict, a1):
    """Phase 3: Fetch all comments for each note."""
    pending = nc.execute(
        "SELECT np.note_id, np.xsec_token FROM note_progress np WHERE np.creator_id=? AND np.comments_done=0",
        (user_id,)
    ).fetchall()

    total_comments = 0
    for note_id, xsec_token in pending:
        cursor = ""
        note_comments = 0
        complete = True
        while True:
            result = await fetch_comments_page(client, page, note_id, xsec_token, cursor, cookie_dict, a1)
            if not result:
                complete = False
                break

            comments = result.get("comments", [])
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            for c in comments:
                cid = c.get("id", "")
                if not cid:
                    continue
                user_info = c.get("user_info", {})
                nc.execute("""
                    INSERT OR IGNORE INTO comments
                    (comment_id, note_id, user_name, user_id, content, like_count,
                     sub_comment_count, create_time, ip_location, crawled_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (
                    cid, note_id,
                    user_info.get("nickname", ""), user_info.get("user_id", ""),
                    c.get("content", ""), str(c.get("like_count", "")),
                    str(c.get("sub_comment_count", "")),
                    c.get("create_time", ""), c.get("ip_location", ""), now,
                ))
                note_comments += 1
            nc.commit()

            cursor = result.get("cursor", "")
            if not result.get("has_more", False) or not cursor:
                break
            await asyncio.sleep(0.5)

        if complete:
            nc.execute("UPDATE note_progress SET comments_done=1 WHERE note_id=?", (note_id,))
            nc.commit()
        total_comments += note_comments
        await asyncio.sleep(0.5)

    remaining = nc.execute(
        "SELECT COUNT(*) FROM note_progress WHERE creator_id=? AND comments_done=0",
        (user_id,),
    ).fetchone()[0]
    if remaining == 0:
        pc.execute(
            "UPDATE creator_progress SET api_phase='done', status='done', finished_at=? WHERE user_id=?",
            (time.strftime("%Y-%m-%d %H:%M:%S"), user_id),
        )
    else:
        pc.execute(
            "UPDATE creator_progress SET api_phase='comments', status='pending' WHERE user_id=?",
            (user_id,),
        )
    pc.commit()
    return total_comments


async def reload_cookie_context(context):
    """Reload cookies into the existing browser context."""
    cookie_str, cookie_dict, a1 = load_cookie_state()
    await context.clear_cookies()
    await context.add_cookies([
        {"name": k, "value": v, "domain": ".xiaohongshu.com", "path": "/"}
        for k, v in cookie_dict.items()
    ])
    return cookie_str, cookie_dict, a1


def run_spider_fallback(user_id, notes_db, progress_db, spider_root=None, timeout=3600):
    return import_creator_from_spider_xhs(
        creator_id=user_id,
        notes_db_path=notes_db,
        progress_db_path=progress_db,
        cookie_str=load_cookie_str(),
        spider_root=spider_root,
        timeout=timeout,
        proxy_url=PROXY,
    )


async def try_spider_fallback(user_id, args, pc):
    print("  尝试 Spider_XHS fallback...")
    fallback = await asyncio.to_thread(
        run_spider_fallback,
        user_id,
        NOTES_DB,
        PROGRESS_DB,
        args.fallback_spider_root,
        args.fallback_timeout,
    )
    if fallback.success:
        print(f"  Spider_XHS fallback 成功: notes+{fallback.imported_notes}, comments+{fallback.imported_comments}")
        return "done" if fallback.imported_comments > 0 else "comments"

    print(f"  Spider_XHS fallback 未成功: {fallback.message}")
    pc.execute("UPDATE creator_progress SET status='pending', error=? WHERE user_id=?", (fallback.message, user_id))
    pc.commit()
    return None


# === Status ===
def show_status():
    if not PROGRESS_DB.exists():
        print("crawl_progress.db 不存在"); return

    pc = sqlite3.connect(PROGRESS_DB)
    # Check if api_phase column exists
    cols = {r[1] for r in pc.execute("PRAGMA table_info(creator_progress)").fetchall()}
    has_phase = "api_phase" in cols

    stats = pc.execute("SELECT status, COUNT(*) FROM creator_progress GROUP BY status").fetchall()
    total = pc.execute("SELECT COUNT(*) FROM creator_progress").fetchone()[0]

    if has_phase:
        phase_stats = pc.execute("SELECT api_phase, COUNT(*) FROM creator_progress GROUP BY api_phase").fetchall()
    pc.close()

    nc = sqlite3.connect(NOTES_DB)
    notes_count = nc.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
    np_count = nc.execute("SELECT COUNT(*) FROM note_progress").fetchone()[0] if 'note_progress' in {r[0] for r in nc.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()} else 0
    comments_count = nc.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
    nc.close()

    print(f"\n{'='*50}")
    print(f"采集进度 (共 {total} 个博主)")
    print(f"{'='*50}")
    for status, count in stats:
        print(f"  {status:10s}: {count}")
    if has_phase:
        print(f"\nAPI阶段:")
        for phase, count in phase_stats:
            print(f"  {phase or 'N/A':10s}: {count}")
    print(f"\n数据量:")
    print(f"  note_progress: {np_count}")
    print(f"  notes: {notes_count}")
    print(f"  comments: {comments_count}")
    print()


# === Main ===
async def main():
    parser = argparse.ArgumentParser(description="小红书 API 爬虫")
    parser.add_argument("--test", type=int, default=0, help="测试前N个博主")
    parser.add_argument("--status", action="store_true", help="查看进度")
    parser.add_argument("--phase", choices=["detail", "comments"], help="只跑指定阶段")
    parser.add_argument("--export", action="store_true", help="导出 notes/comments CSV")
    parser.add_argument("--cookie-refresh-every", type=int, default=DEFAULT_COOKIE_REFRESH_EVERY, help="每处理 N 个博主后重读一次 cookie 文件")
    parser.add_argument("--fallback-spider-root", default=str(BASE_DIR / "Spider_XHS"), help="Spider_XHS 仓库路径")
    parser.add_argument("--fallback-timeout", type=int, default=3600, help="Spider_XHS fallback 超时时间(秒)")
    parser.add_argument("--fallback-after-errors", type=int, default=DEFAULT_FALLBACK_CONSEC_ERRORS, help="详情阶段连续失败达到 N 次后尝试 Spider_XHS fallback")
    parser.add_argument("--fallback-on-list-failure", action="store_true", help="列表阶段失败时也尝试 Spider_XHS fallback")
    args = parser.parse_args()

    if args.status:
        show_status()
        return
    if args.export:
        export_csv()
        return

    nc, pc = init_db()
    sync_creator_progress(pc)
    cookie_str, cookie_dict, a1 = load_cookie_state()

    # Get creators to process
    if args.phase == "detail":
        rows = pc.execute(
            "SELECT user_id, name FROM creator_progress WHERE status IN ('pending','running') AND api_phase='detail'" +
            (f" LIMIT {args.test}" if args.test else "")
        ).fetchall()
    elif args.phase == "comments":
        rows = pc.execute(
            "SELECT user_id, name FROM creator_progress WHERE status IN ('pending','running') AND api_phase='comments'" +
            (f" LIMIT {args.test}" if args.test else "")
        ).fetchall()
    else:
        # Full pipeline: pending or partially done
        rows = pc.execute(
            "SELECT user_id, name FROM creator_progress WHERE status IN ('pending','running')" +
            (f" LIMIT {args.test}" if args.test else "")
        ).fetchall()

    if not rows:
        print("没有待处理的博主")
        show_status()
        return

    print(f"待处理: {len(rows)} 个博主")

    async with async_playwright() as p:
        print("启动 Playwright...")
        browser = await p.chromium.launch(headless=True, proxy={"server": PROXY})
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        )
        await context.add_cookies([
            {"name": k, "value": v, "domain": ".xiaohongshu.com", "path": "/"}
            for k, v in cookie_dict.items()
        ])
        page = await context.new_page()

        print("加载小红书首页...")
        await page.goto("https://www.xiaohongshu.com/explore", wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        has_mnsv2 = await page.evaluate("typeof window.mnsv2")
        if has_mnsv2 == "undefined":
            await page.wait_for_timeout(5000)
            has_mnsv2 = await page.evaluate("typeof window.mnsv2")
            if has_mnsv2 == "undefined":
                print("❌ mnsv2 未加载"); await browser.close(); return
        print("✓ mnsv2 就绪")

        async with httpx.AsyncClient(proxy=PROXY, timeout=15) as client:
            try:
                for idx, (user_id, name) in enumerate(rows, 1):
                    if args.cookie_refresh_every > 0 and idx > 1 and (idx - 1) % args.cookie_refresh_every == 0:
                        print("重载 cookie...")
                        cookie_str, cookie_dict, a1 = await reload_cookie_context(context)
                        await page.goto("https://www.xiaohongshu.com/explore", wait_until="domcontentloaded")
                        await page.wait_for_timeout(3000)

                    print(f"\n[{idx}/{len(rows)}] {name or user_id}")

                    # Get current phase
                    phase_row = pc.execute("SELECT api_phase FROM creator_progress WHERE user_id=?", (user_id,)).fetchone()
                    current_phase = phase_row[0] if phase_row and phase_row[0] else "list"

                    pc.execute("UPDATE creator_progress SET status='running', started_at=? WHERE user_id=?",
                               (time.strftime("%Y-%m-%d %H:%M:%S"), user_id))
                    pc.commit()

                    # Phase 1: List
                    if current_phase == "list" and args.phase is None:
                        list_done, note_count = await phase_list(client, page, user_id, nc, pc, cookie_dict, a1)
                        print(f"  列表: {note_count} 篇笔记")
                        if not list_done:
                            if args.fallback_on_list_failure:
                                fallback_phase = await try_spider_fallback(user_id, args, pc)
                                if fallback_phase:
                                    current_phase = fallback_phase
                                    await asyncio.sleep(3)
                                    if current_phase == "done":
                                        await asyncio.sleep(2)
                                        continue
                                    if current_phase == "comments":
                                        comment_count = await phase_comments(client, page, user_id, nc, pc, cookie_dict, a1)
                                        print(f"  评论: {comment_count} 条")
                                        await asyncio.sleep(2)
                                        continue
                            pc.execute("UPDATE creator_progress SET status='pending' WHERE user_id=?", (user_id,))
                            pc.commit()
                            await asyncio.sleep(2)
                            continue
                        current_phase = "detail"
                        await asyncio.sleep(5)  # cooldown between phases

                    # Phase 2: Detail
                    if current_phase == "detail" and args.phase in (None, "detail"):
                        detail_done, success, total = await phase_detail(client, page, user_id, nc, pc, cookie_dict, a1)
                        print(f"  详情: {success}/{total}")
                        if not detail_done:
                            fallback_needed = args.phase is None and total > 0 and (total - success) >= args.fallback_after_errors
                            if fallback_needed:
                                print("  详情阶段连续失败较多，切到 Spider_XHS fallback...")
                                fallback_phase = await try_spider_fallback(user_id, args, pc)
                                if fallback_phase:
                                    current_phase = fallback_phase
                                else:
                                    await asyncio.sleep(2)
                                    continue
                            else:
                                pc.execute("UPDATE creator_progress SET status='pending' WHERE user_id=?", (user_id,))
                                pc.commit()
                                await asyncio.sleep(2)
                                continue
                        elif current_phase != "done":
                            current_phase = "comments"
                        await asyncio.sleep(3)

                    # Phase 3: Comments
                    if current_phase == "comments" and args.phase in (None, "comments"):
                        comment_count = await phase_comments(client, page, user_id, nc, pc, cookie_dict, a1)
                        print(f"  评论: {comment_count} 条")

                    await asyncio.sleep(2)

            except (KeyboardInterrupt, asyncio.CancelledError):
                # Reset running to pending (but keep phase progress)
                pc.execute("UPDATE creator_progress SET status='pending' WHERE status='running'")
                pc.commit()
                print("\n\n中断，已保存进度。")

        await browser.close()

    show_status()
    nc.close()
    pc.close()


if __name__ == "__main__":
    asyncio.run(main())
