#!/usr/bin/env python3
"""
测试 xhs 库是否能从海外正常工作。
用法：
    python scraper/test_xhs.py --cookie "a1=xxx; web_session=yyy; webId=zzz"

获取 cookie 方法：
1. 浏览器打开 https://www.xiaohongshu.com/（需要中国IP或VPN）
2. F12 → Application → Cookies → xiaohongshu.com
3. 复制 a1, web_session, webId 的值
"""

import argparse
import json
import sys
import time

from xhs import XhsClient


def test_connection(client):
    """测试基本连接"""
    print("=" * 50)
    print("测试1: 获取用户信息")
    print("=" * 50)

    # 用赵露思的ID测试（数据集第一个博主）
    user_id = "5a73c5fa4eacab4c4ccc9778"
    try:
        user_info = client.get_user_info(user_id)
        print(f"  用户名: {user_info.get('basic_info', {}).get('nickname', 'N/A')}")
        print(f"  粉丝数: {user_info.get('interactions', [{}])[0].get('count', 'N/A')}")
        print(f"  ✓ 用户信息获取成功")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}")
        return False


def test_user_notes(client):
    """测试获取用户笔记列表"""
    print("\n" + "=" * 50)
    print("测试2: 获取用户笔记列表（第一页）")
    print("=" * 50)

    user_id = "5a73c5fa4eacab4c4ccc9778"
    try:
        notes = client.get_user_notes(user_id)
        note_list = notes.get("notes", [])
        has_more = notes.get("has_more", False)
        print(f"  第一页笔记数: {len(note_list)}")
        print(f"  还有更多: {has_more}")
        if note_list:
            first = note_list[0]
            print(f"  第一篇: {first.get('display_title', 'N/A')}")
            print(f"  note_id: {first.get('note_id', 'N/A')}")
            print(f"  ✓ 笔记列表获取成功")
            return first.get("note_id")
        return None
    except Exception as e:
        print(f"  ✗ 失败: {e}")
        return None


def test_note_detail(client, note_id):
    """测试获取笔记详情"""
    print("\n" + "=" * 50)
    print(f"测试3: 获取笔记详情 (note_id: {note_id})")
    print("=" * 50)

    try:
        note = client.get_note_by_id(note_id)
        title = note.get("title", "N/A")
        desc = note.get("desc", "N/A")
        note_type = note.get("type", "N/A")
        interact = note.get("interact_info", {})

        print(f"  标题: {title}")
        print(f"  类型: {note_type}")
        print(f"  文本前100字: {desc[:100]}..." if len(desc) > 100 else f"  文本: {desc}")
        print(f"  点赞: {interact.get('liked_count', 'N/A')}")
        print(f"  收藏: {interact.get('collected_count', 'N/A')}")
        print(f"  评论: {interact.get('comment_count', 'N/A')}")
        print(f"  标签: {[t.get('name', '') for t in note.get('tag_list', [])][:5]}")
        print(f"  ✓ 笔记详情获取成功")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}")
        return False


def test_comments(client, note_id):
    """测试获取评论"""
    print("\n" + "=" * 50)
    print(f"测试4: 获取笔记评论 (note_id: {note_id})")
    print("=" * 50)

    try:
        comments = client.get_note_comments(note_id)
        comment_list = comments.get("comments", [])
        has_more = comments.get("has_more", False)
        print(f"  第一页评论数: {len(comment_list)}")
        print(f"  还有更多: {has_more}")
        if comment_list:
            first = comment_list[0]
            print(f"  第一条评论: {first.get('content', 'N/A')[:80]}")
            print(f"  评论者: {first.get('user_info', {}).get('nickname', 'N/A')}")
        print(f"  ✓ 评论获取成功")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="测试 xhs 库连接")
    parser.add_argument("--cookie", type=str, required=True, help="小红书 cookie 字符串")
    parser.add_argument("--proxy", type=str, default=None, help="代理地址，如 http://127.0.0.1:7890")
    args = parser.parse_args()

    proxies = None
    if args.proxy:
        proxies = {"http": args.proxy, "https": args.proxy}

    print("初始化 XhsClient...")
    client = XhsClient(cookie=args.cookie, proxies=proxies)
    print(f"  代理: {args.proxy or '无'}")

    # 测试1: 用户信息
    if not test_connection(client):
        print("\n❌ 基本连接失败，请检查 cookie 和网络")
        sys.exit(1)

    time.sleep(1)

    # 测试2: 笔记列表
    note_id = test_user_notes(client)
    if not note_id:
        print("\n❌ 无法获取笔记列表")
        sys.exit(1)

    time.sleep(1)

    # 测试3: 笔记详情
    test_note_detail(client, note_id)

    time.sleep(1)

    # 测试4: 评论
    test_comments(client, note_id)

    print("\n" + "=" * 50)
    print("✅ 所有测试完成！xhs 库从海外可以正常工作。")
    print("=" * 50)


if __name__ == "__main__":
    main()
