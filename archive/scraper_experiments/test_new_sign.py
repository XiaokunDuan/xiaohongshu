#!/usr/bin/env python3
"""
测试 MediaCrawler 新签名方案（window.mnsv2 + x-s-common）是否能绕过300011。
直接内联签名函数，不依赖 MediaCrawler 完整包。
"""

import asyncio
import ctypes
import hashlib
import json
import random
import time
from urllib.parse import quote

import httpx
from playwright.async_api import async_playwright

PROXY = "http://127.0.0.1:7890"
COOKIE_STR = "a1=19b59fabb6bw93jydbj5sf9kdgrbbg27yx5f4hszh30000101682; web_session=040069b522744d19141ec9def63b4bdacdfdb9; webId=c4977872fab544a22e28fe3af3e3b242; xsecappid=xhs-pc-web"
API_HOST = "https://edith.xiaohongshu.com"
TEST_USER_ID = "5a73c5fa4eacab4c4ccc9778"

# === Inline sign functions from MediaCrawler ===
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
    # Get b1 from localStorage
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
        "x-s": x_s,
        "x-t": x_t,
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


async def main():
    cookie_dict = parse_cookie(COOKIE_STR)

    async with async_playwright() as p:
        print("1. 启动 Playwright + 代理...")
        browser = await p.chromium.launch(headless=True, proxy={"server": PROXY})
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        )
        await context.add_cookies([
            {"name": k, "value": v, "domain": ".xiaohongshu.com", "path": "/"}
            for k, v in cookie_dict.items()
        ])

        page = await context.new_page()
        print("2. 加载小红书首页...")
        await page.goto("https://www.xiaohongshu.com/explore", wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        has_mnsv2 = await page.evaluate("typeof window.mnsv2")
        print(f"   window.mnsv2: {has_mnsv2}")
        if has_mnsv2 == "undefined":
            await page.wait_for_timeout(5000)
            has_mnsv2 = await page.evaluate("typeof window.mnsv2")
            print(f"   retry: {has_mnsv2}")
            if has_mnsv2 == "undefined":
                print("   ❌ mnsv2 未加载"); await browser.close(); return

        # Test 1: Get user notes
        print("\n3. 测试 GET user_posted (新签名 mnsv2)...")
        uri = "/api/sns/web/v1/user_posted"
        params = {"num": "30", "cursor": "", "user_id": TEST_USER_ID, "image_scenes": "FD_WM_WEBP"}

        signs = await sign_with_playwright(page, uri, params, cookie_dict.get("a1",""), "GET")
        print(f"   x-s: {signs['x-s'][:40]}...")
        print(f"   x-s-common: {signs['x-s-common'][:40]}...")

        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://www.xiaohongshu.com",
            "referer": "https://www.xiaohongshu.com/",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            "X-S": signs["x-s"],
            "X-T": signs["x-t"],
            "x-S-Common": signs["x-s-common"],
            "X-B3-Traceid": signs["x-b3-traceid"],
        }

        first_note_id = None
        xsec_token = ""
        async with httpx.AsyncClient(proxy=PROXY, timeout=15) as client:
            resp = await client.get(f"{API_HOST}{uri}", params=params, headers=headers, cookies=cookie_dict)
            print(f"   HTTP {resp.status_code}")
            data = resp.json()
            code = data.get("code")
            print(f"   code: {code}, success: {data.get('success')}")
            if data.get("success"):
                notes = data["data"]["notes"]
                print(f"   ✅ 笔记数: {len(notes)}, has_more: {data['data']['has_more']}")
                if notes:
                    first_note_id = notes[0]["note_id"]
                    xsec_token = notes[0].get("xsec_token", "")
                    print(f"   第一篇: {notes[0].get('display_title','?')}")
            else:
                print(f"   ❌ {json.dumps(data, ensure_ascii=False)[:300]}")

        # Test 2: Get comments
        if first_note_id:
            print(f"\n4. 测试 GET comment/page (note: {first_note_id})...")
            await asyncio.sleep(1)
            curi = "/api/sns/web/v2/comment/page"
            cparams = {"note_id": first_note_id, "cursor": "", "top_comment_id": "", "image_formats": "jpg,webp,avif", "xsec_token": xsec_token}
            signs2 = await sign_with_playwright(page, curi, cparams, cookie_dict.get("a1",""), "GET")
            h2 = {**headers, "X-S": signs2["x-s"], "X-T": signs2["x-t"], "x-S-Common": signs2["x-s-common"], "X-B3-Traceid": signs2["x-b3-traceid"]}
            async with httpx.AsyncClient(proxy=PROXY, timeout=15) as client:
                resp2 = await client.get(f"{API_HOST}{curi}", params=cparams, headers=h2, cookies=cookie_dict)
                print(f"   HTTP {resp2.status_code}")
                d2 = resp2.json()
                print(f"   code: {d2.get('code')}, success: {d2.get('success')}")
                if d2.get("success"):
                    comments = d2["data"].get("comments", [])
                    print(f"   ✅ 评论数: {len(comments)}, has_more: {d2['data'].get('has_more')}")
                    if comments:
                        print(f"   第一条: {comments[0].get('content','?')[:80]}")
                else:
                    print(f"   ❌ {json.dumps(d2, ensure_ascii=False)[:300]}")

        print("\n" + "="*50)
        print("测试完成！")
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
