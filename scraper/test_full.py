#!/usr/bin/env python3
"""
全流程测试：Playwright签名 + API调用
验证：用户笔记翻页、笔记详情、评论获取
"""

import asyncio
import json
import time

import requests as req
from playwright.async_api import async_playwright

PROXY = "http://127.0.0.1:7890"
COOKIE_STR = "a1=19b59fabb6bw93jydbj5sf9kdgrbbg27yx5f4hszh30000101682; web_session=040069b522744d19141ef23bf63b4bd5e8b770; webId=c4977872fab544a22e28fe3af3e3b242; xsecappid=xhs-pc-web"
API_HOST = "https://edith.xiaohongshu.com"
TEST_USER_ID = "5a73c5fa4eacab4c4ccc9778"  # 赵露思


def parse_cookie_str(cookie_str):
    cookies = {}
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            k, v = item.split("=", 1)
            cookies[k.strip()] = v.strip()
    return cookies


async def main():
    cookie_dict = parse_cookie_str(COOKIE_STR)

    async with async_playwright() as p:
        print("1. 启动 Playwright Chromium + 代理...")
        browser = await p.chromium.launch(
            headless=True,
            proxy={"server": PROXY},
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        )

        # Set cookies
        await context.add_cookies([
            {"name": k, "value": v, "domain": ".xiaohongshu.com", "path": "/"}
            for k, v in cookie_dict.items()
        ])

        page = await context.new_page()

        print("2. 加载小红书首页（加载签名函数）...")
        await page.goto("https://www.xiaohongshu.com/explore", wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)

        # Check if _webmsxyw exists
        has_sign = await page.evaluate("typeof window._webmsxyw")
        print(f"   _webmsxyw type: {has_sign}")
        if has_sign == "undefined":
            print("   ❌ 签名函数未加载！尝试等待更久...")
            await page.wait_for_timeout(5000)
            has_sign = await page.evaluate("typeof window._webmsxyw")
            print(f"   _webmsxyw type (retry): {has_sign}")
            if has_sign == "undefined":
                print("   ❌ 签名函数仍未加载，退出")
                await browser.close()
                return

        # ========== Test 1: Get user notes (with pagination) ==========
        print("\n3. 测试获取用户笔记列表（翻页）...")
        uri = "/api/sns/web/v1/user_posted"
        params = {"num": "30", "cursor": "", "user_id": TEST_USER_ID, "image_scenes": "FD_WM_WEBP"}
        query_str = "&".join(f"{k}={v}" for k, v in params.items())
        sign_uri = f"{uri}?{query_str}"

        sign_result = await page.evaluate(
            "([uri, data]) => window._webmsxyw(uri, data)",
            [sign_uri, None]
        )
        print(f"   签名结果: x-s={sign_result.get('X-s', sign_result.get('x-s', '?'))[:20]}...")

        # Make API call with signature
        api_headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "content-type": "application/json",
            "referer": "https://www.xiaohongshu.com/",
            "origin": "https://www.xiaohongshu.com",
        }
        # Add signature headers (handle both cases)
        for k, v in sign_result.items():
            api_headers[k.lower()] = str(v)

        resp = req.get(
            f"{API_HOST}{uri}",
            params=params,
            headers=api_headers,
            cookies=cookie_dict,
            proxies={"http": PROXY, "https": PROXY},
            timeout=15,
        )
        print(f"   API 状态码: {resp.status_code}")

        if resp.status_code == 200:
            data = resp.json()
            if data.get("success"):
                notes = data["data"]["notes"]
                has_more = data["data"]["has_more"]
                cursor = data["data"]["cursor"]
                print(f"   ✅ 第一页笔记数: {len(notes)}, has_more: {has_more}")
                if notes:
                    print(f"   第一篇: {notes[0].get('display_title', '?')}")
                    first_note_id = notes[0]["note_id"]

                    # Try second page
                    if has_more:
                        time.sleep(1)
                        params2 = {**params, "cursor": cursor}
                        query_str2 = "&".join(f"{k}={v}" for k, v in params2.items())
                        sign2 = await page.evaluate(
                            "([uri, data]) => window._webmsxyw(uri, data)",
                            [f"{uri}?{query_str2}", None]
                        )
                        api_headers2 = {**api_headers}
                        for k, v in sign2.items():
                            api_headers2[k.lower()] = str(v)

                        resp2 = req.get(
                            f"{API_HOST}{uri}", params=params2, headers=api_headers2,
                            cookies=cookie_dict, proxies={"http": PROXY, "https": PROXY}, timeout=15,
                        )
                        if resp2.status_code == 200 and resp2.json().get("success"):
                            notes2 = resp2.json()["data"]["notes"]
                            print(f"   ✅ 第二页笔记数: {len(notes2)}")
                        else:
                            print(f"   ❌ 第二页失败: {resp2.status_code} {resp2.text[:200]}")
                else:
                    first_note_id = None
            else:
                print(f"   ❌ API 返回失败: {data}")
                first_note_id = None
        else:
            print(f"   ❌ HTTP 错误: {resp.status_code} {resp.text[:200]}")
            first_note_id = None

        # ========== Test 2: Get note detail ==========
        if first_note_id:
            print(f"\n4. 测试获取笔记详情 (note_id: {first_note_id})...")
            time.sleep(1)
            feed_uri = "/api/sns/web/v1/feed"
            feed_data = {"source_note_id": first_note_id, "image_scenes": ["CRD_WM_WEBP"]}

            sign3 = await page.evaluate(
                "([uri, data]) => window._webmsxyw(uri, data)",
                [feed_uri, json.dumps(feed_data, separators=(",", ":"))]
            )
            api_headers3 = {**api_headers}
            for k, v in sign3.items():
                api_headers3[k.lower()] = str(v)

            resp3 = req.post(
                f"{API_HOST}{feed_uri}",
                json=feed_data,
                headers=api_headers3,
                cookies=cookie_dict,
                proxies={"http": PROXY, "https": PROXY},
                timeout=15,
            )
            if resp3.status_code == 200 and resp3.json().get("success"):
                note_card = resp3.json()["data"]["items"][0]["note_card"]
                print(f"   ✅ 标题: {note_card.get('title', '?')}")
                print(f"   内容: {note_card.get('desc', '')[:150]}")
                interact = note_card.get("interact_info", {})
                print(f"   点赞: {interact.get('liked_count')}, 评论: {interact.get('comment_count')}")
            else:
                print(f"   ❌ 笔记详情失败: {resp3.status_code} {resp3.text[:200]}")

        # ========== Test 3: Get comments ==========
        if first_note_id:
            print(f"\n5. 测试获取评论 (note_id: {first_note_id})...")
            time.sleep(1)
            comment_uri = "/api/sns/web/v2/comment/page"
            comment_params = {"note_id": first_note_id, "cursor": ""}
            comment_qs = "&".join(f"{k}={v}" for k, v in comment_params.items())

            sign4 = await page.evaluate(
                "([uri, data]) => window._webmsxyw(uri, data)",
                [f"{comment_uri}?{comment_qs}", None]
            )
            api_headers4 = {**api_headers}
            for k, v in sign4.items():
                api_headers4[k.lower()] = str(v)

            resp4 = req.get(
                f"{API_HOST}{comment_uri}",
                params=comment_params,
                headers=api_headers4,
                cookies=cookie_dict,
                proxies={"http": PROXY, "https": PROXY},
                timeout=15,
            )
            if resp4.status_code == 200 and resp4.json().get("success"):
                comments = resp4.json()["data"]["comments"]
                has_more_c = resp4.json()["data"]["has_more"]
                print(f"   ✅ 评论数: {len(comments)}, has_more: {has_more_c}")
                if comments:
                    print(f"   第一条: {comments[0].get('content', '?')[:80]}")
            else:
                print(f"   ❌ 评论获取失败: {resp4.status_code} {resp4.text[:200]}")

        print("\n" + "=" * 50)
        print("🎉 全流程测试完成！")
        print("=" * 50)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
