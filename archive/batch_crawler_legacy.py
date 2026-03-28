#!/usr/bin/env python3
"""
小红书3400博主笔记文本批量采集脚本（断点续传）

使用方式：
    python batch_crawler.py                  # 从上次中断处继续
    python batch_crawler.py --batch-size 20  # 每批20个博主
    python batch_crawler.py --test 10        # 测试模式，只跑前10个
    python batch_crawler.py --status         # 查看进度
"""

import argparse
import csv
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).parent
MEDIACRAWLER_DIR = BASE_DIR / "MediaCrawler"
CREATOR_IDS_FILE = BASE_DIR / "creator_ids.txt"
DAREN_CSV = BASE_DIR / "data" / "daren_clusters.csv"
PROGRESS_DB = BASE_DIR / "data" / "crawl_progress.db"

# MediaCrawler xhs config path
XHS_CONFIG = MEDIACRAWLER_DIR / "config" / "xhs_config.py"


def init_progress_db():
    """初始化进度追踪数据库"""
    conn = sqlite3.connect(PROGRESS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS creator_progress (
            user_id TEXT PRIMARY KEY,
            name TEXT,
            expected_notes INTEGER,
            status TEXT DEFAULT 'pending',
            actual_notes INTEGER DEFAULT 0,
            started_at TEXT,
            finished_at TEXT,
            error TEXT
        )
    """)
    conn.commit()
    return conn


def load_creators(conn):
    """从CSV加载博主信息到进度数据库（如果还没加载）"""
    count = conn.execute("SELECT COUNT(*) FROM creator_progress").fetchone()[0]
    if count > 0:
        print(f"进度数据库已有 {count} 个博主记录")
        return

    print("从CSV加载博主数据...")
    with open(DAREN_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            url = row.get("达人官方地址", "")
            m = re.search(r"profile/([a-f0-9]+)", url)
            if m:
                user_id = m.group(1)
                name = row.get("达人名称", "")
                notes = int(float(row.get("笔记总数", 0)))
                rows.append((user_id, name, notes))

    conn.executemany(
        "INSERT OR IGNORE INTO creator_progress (user_id, name, expected_notes) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()
    print(f"已加载 {len(rows)} 个博主")


def get_pending_creators(conn, limit=None):
    """获取待采集的博主列表"""
    query = "SELECT user_id, name, expected_notes FROM creator_progress WHERE status = 'pending' ORDER BY ROWID"
    if limit:
        query += f" LIMIT {limit}"
    return conn.execute(query).fetchall()


def update_status(conn, user_id, status, error=None):
    """更新博主采集状态"""
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    if status == "running":
        conn.execute(
            "UPDATE creator_progress SET status=?, started_at=? WHERE user_id=?",
            (status, now, user_id),
        )
    elif status == "done":
        conn.execute(
            "UPDATE creator_progress SET status=?, finished_at=? WHERE user_id=?",
            (status, now, user_id),
        )
    elif status == "error":
        conn.execute(
            "UPDATE creator_progress SET status=?, finished_at=?, error=? WHERE user_id=?",
            (status, now, error, user_id),
        )
    conn.commit()


def write_xhs_config(creator_ids):
    """动态写入当前批次的博主ID到xhs_config.py"""
    # 读取原始配置
    with open(XHS_CONFIG, "r", encoding="utf-8") as f:
        content = f.read()

    # 替换 XHS_CREATOR_ID_LIST
    id_list_str = json.dumps(creator_ids, ensure_ascii=False, indent=4)
    # 把 JSON 格式转成 Python list 格式
    new_list = "XHS_CREATOR_ID_LIST = " + id_list_str + "\n"

    # 用正则替换
    pattern = r"XHS_CREATOR_ID_LIST\s*=\s*\[.*?\]"
    content = re.sub(pattern, new_list.strip(), content, flags=re.DOTALL)

    with open(XHS_CONFIG, "w", encoding="utf-8") as f:
        f.write(content)


def run_mediacrawler():
    """运行 MediaCrawler"""
    result = subprocess.run(
        ["uv", "run", "python", "main.py"],
        cwd=MEDIACRAWLER_DIR,
        capture_output=False,
        timeout=3600,  # 单批最多1小时超时
    )
    return result.returncode


def show_status(conn):
    """显示采集进度"""
    stats = conn.execute("""
        SELECT status, COUNT(*), SUM(expected_notes)
        FROM creator_progress
        GROUP BY status
    """).fetchall()

    total = conn.execute("SELECT COUNT(*) FROM creator_progress").fetchone()[0]
    total_notes = conn.execute("SELECT SUM(expected_notes) FROM creator_progress").fetchone()[0]

    print(f"\n{'='*50}")
    print(f"采集进度总览 (共 {total} 个博主, {total_notes:,} 篇笔记)")
    print(f"{'='*50}")
    for status, count, notes in stats:
        pct = count / total * 100
        print(f"  {status:10s}: {count:5d} 个博主 ({pct:5.1f}%) | {int(notes or 0):>8,} 篇笔记")

    # 最近完成的
    recent = conn.execute("""
        SELECT name, finished_at FROM creator_progress
        WHERE status='done' ORDER BY finished_at DESC LIMIT 5
    """).fetchall()
    if recent:
        print(f"\n最近完成:")
        for name, t in recent:
            print(f"  ✓ {name} ({t})")

    # 最近失败的
    errors = conn.execute("""
        SELECT name, error FROM creator_progress
        WHERE status='error' ORDER BY finished_at DESC LIMIT 5
    """).fetchall()
    if errors:
        print(f"\n最近失败:")
        for name, err in errors:
            print(f"  ✗ {name}: {err}")
    print()


def main():
    parser = argparse.ArgumentParser(description="小红书博主笔记批量采集")
    parser.add_argument("--batch-size", type=int, default=10, help="每批采集的博主数量 (默认10)")
    parser.add_argument("--test", type=int, default=0, help="测试模式：只采集前N个博主")
    parser.add_argument("--status", action="store_true", help="查看采集进度")
    parser.add_argument("--reset-errors", action="store_true", help="重置失败的博主为pending")
    args = parser.parse_args()

    conn = init_progress_db()
    load_creators(conn)

    if args.status:
        show_status(conn)
        return

    if args.reset_errors:
        n = conn.execute("UPDATE creator_progress SET status='pending', error=NULL WHERE status='error'").rowcount
        conn.commit()
        print(f"已重置 {n} 个失败博主为 pending")
        return

    # 获取待采集列表
    limit = args.test if args.test > 0 else None
    pending = get_pending_creators(conn, limit=limit)

    if not pending:
        print("所有博主已采集完成！")
        show_status(conn)
        return

    total_pending = len(pending)
    print(f"待采集: {total_pending} 个博主")
    print(f"批次大小: {args.batch_size}")
    print(f"预计批次: {(total_pending + args.batch_size - 1) // args.batch_size}")
    print()

    # 分批处理
    batch_num = 0
    for i in range(0, total_pending, args.batch_size):
        batch = pending[i : i + args.batch_size]
        batch_num += 1
        batch_ids = [row[0] for row in batch]
        batch_names = [row[1] for row in batch]

        print(f"\n{'='*50}")
        print(f"批次 {batch_num}: {len(batch)} 个博主")
        print(f"博主: {', '.join(batch_names[:5])}{'...' if len(batch_names) > 5 else ''}")
        print(f"{'='*50}")

        # 更新状态为 running
        for user_id, name, _ in batch:
            update_status(conn, user_id, "running")

        # 写入配置
        write_xhs_config(batch_ids)

        # 运行爬虫
        try:
            returncode = run_mediacrawler()
            if returncode == 0:
                for user_id, name, _ in batch:
                    update_status(conn, user_id, "done")
                print(f"批次 {batch_num} 完成 ✓")
            else:
                for user_id, name, _ in batch:
                    update_status(conn, user_id, "error", f"exit code {returncode}")
                print(f"批次 {batch_num} 失败 (exit code {returncode})")
        except subprocess.TimeoutExpired:
            for user_id, name, _ in batch:
                update_status(conn, user_id, "error", "timeout")
            print(f"批次 {batch_num} 超时")
        except KeyboardInterrupt:
            # Ctrl+C 时把 running 的重置为 pending
            conn.execute("UPDATE creator_progress SET status='pending' WHERE status='running'")
            conn.commit()
            print("\n\n用户中断，已保存进度。下次运行将从中断处继续。")
            show_status(conn)
            return

        # 批次间休息
        if i + args.batch_size < total_pending:
            sleep_sec = 10
            print(f"休息 {sleep_sec} 秒...")
            time.sleep(sleep_sec)

    show_status(conn)
    print("全部完成！")


if __name__ == "__main__":
    main()
