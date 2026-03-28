#!/usr/bin/env python3
"""
AdsPower-backed Xiaohongshu smoke test.

Flow:
1. Call AdsPower Local API to start or attach to a browser profile.
2. Connect to the returned CDP endpoint with Playwright.
3. Open a Xiaohongshu creator page and report whether the profile can access it.
"""

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from playwright.async_api import async_playwright

sys.stdout.reconfigure(line_buffering=True)

ADSP_DEFAULT_HOST = "127.0.0.1"
ADSP_DEFAULT_PORT = 50325
DEFAULT_TEST_URL = "https://www.xiaohongshu.com/user/profile/5a73c5fa4eacab4c4ccc9778"
ADSP_LOCAL_API_FILE = Path("/Users/dxk/Library/Application Support/adspower_global/cwd_global/source/local_api")


@dataclass
class AdsPowerBrowserSession:
    ws_endpoint: str
    debug_port: int | None = None
    profile_id: str | None = None
    raw: dict[str, Any] | None = None


def infer_local_api_base(host: str | None, port: int | None):
    if host and port:
        return f"http://{host}:{port}"

    if ADSP_LOCAL_API_FILE.exists():
        value = ADSP_LOCAL_API_FILE.read_text(encoding="utf-8").strip().rstrip("/")
        if value.startswith("http://local.adspower.com:"):
            actual_port = value.rsplit(":", 1)[-1]
            return f"http://127.0.0.1:{actual_port}"
        if value.startswith("http://"):
            return value

    return f"http://{ADSP_DEFAULT_HOST}:{port or ADSP_DEFAULT_PORT}"


def _extract_session_data(payload: dict[str, Any], profile_id: str):
    data = payload.get("data", payload)
    ws_endpoint = (
        data.get("ws", {}).get("puppeteer")
        if isinstance(data.get("ws"), dict)
        else None
    ) or data.get("websocket") or data.get("wsEndpoint") or data.get("ws")
    debug_port = data.get("debug_port") or data.get("debugPort") or data.get("port")

    if not ws_endpoint and debug_port:
        ws_endpoint = f"http://127.0.0.1:{debug_port}"

    if not ws_endpoint:
        raise RuntimeError(f"AdsPower 响应里没有 ws/debug 端点: {json.dumps(payload, ensure_ascii=False)[:400]}")

    return AdsPowerBrowserSession(
        ws_endpoint=str(ws_endpoint),
        debug_port=int(debug_port) if debug_port else None,
        profile_id=profile_id,
        raw=payload,
    )


def start_adspower_browser(base_url: str, profile_id: str):
    candidates = [
        ("GET", f"{base_url}/api/v1/browser/start", {"user_id": profile_id}),
        ("GET", f"{base_url}/api/v1/browser/start", {"serial_number": profile_id}),
        ("POST", f"{base_url}/api/v1/browser/start", {"user_id": profile_id}),
        ("POST", f"{base_url}/api/v1/browser/start", {"serial_number": profile_id}),
        ("GET", f"{base_url}/api/v1/browser/active", {"user_id": profile_id}),
        ("GET", f"{base_url}/api/v1/browser/active", {"serial_number": profile_id}),
    ]

    errors = []
    for method, url, payload in candidates:
        try:
            if method == "GET":
                resp = requests.get(url, params=payload, timeout=20)
            else:
                resp = requests.post(url, json=payload, timeout=20)
            body = resp.json()
        except Exception as exc:
            errors.append(f"{method} {url} {payload}: {exc}")
            continue

        if resp.status_code != 200:
            errors.append(f"{method} {url} {payload}: HTTP {resp.status_code}")
            continue

        if isinstance(body, dict) and body.get("code") not in (0, "0", None):
            errors.append(f"{method} {url} {payload}: code={body.get('code')} msg={body.get('msg')}")
            continue

        try:
            return _extract_session_data(body, profile_id)
        except Exception as exc:
            errors.append(f"{method} {url} {payload}: {exc}")

    raise RuntimeError("AdsPower Local API 调用失败:\n" + "\n".join(errors))


async def smoke_test_creator_page(session: AdsPowerBrowserSession, url: str):
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(session.ws_endpoint)
        try:
            context = browser.contexts[0] if browser.contexts else await browser.new_context()
            page = context.pages[0] if context.pages else await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)
            html = await page.content()
            title = await page.title()

            blocked_tokens = ["登录", "login", "验证码", "访问频率", "IP_BLOCK", "安全验证"]
            blocked = [token for token in blocked_tokens if token in html]

            note_titles = await page.evaluate(
                """
                () => Array.from(document.querySelectorAll('section.note-item, a[href*="/explore/"], [class*="note-item"]'))
                    .slice(0, 5)
                    .map(el => (el.textContent || '').trim().replace(/\\s+/g, ' ').slice(0, 80))
                    .filter(Boolean)
                """
            )

            return {
                "title": title,
                "blocked_tokens": blocked,
                "note_titles": note_titles,
                "url": page.url,
            }
        finally:
            await browser.close()


async def main():
    parser = argparse.ArgumentParser(description="AdsPower Xiaohongshu smoke test")
    parser.add_argument("--profile-id", default="k19io81y", help="AdsPower profile id / serial number")
    parser.add_argument("--api-host", help="AdsPower Local API host, default inferred from local_api file")
    parser.add_argument("--api-port", type=int, help="AdsPower Local API port, default inferred from local_api file")
    parser.add_argument("--url", default=DEFAULT_TEST_URL, help="Xiaohongshu page to open")
    args = parser.parse_args()

    base_url = infer_local_api_base(args.api_host, args.api_port)
    print(f"AdsPower Local API: {base_url}")
    print(f"Profile: {args.profile_id}")

    session = start_adspower_browser(base_url, args.profile_id)
    print(f"CDP endpoint: {session.ws_endpoint}")

    result = await smoke_test_creator_page(session, args.url)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
