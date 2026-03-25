"""
5_time_and_format_analysis.py
分析两个未挖掘的维度：
  A. 粉丝活跃时间规律（高峰时段 + 高峰日）
  B. 图文 vs 视频 内容形式对比（报价、CPE）
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import re
import os

# ── 字体 ──────────────────────────────────────────────────────────────
def get_font():
    for p in ['/System/Library/Fonts/PingFang.ttc', '/System/Library/Fonts/STHeiti Light.ttc']:
        if os.path.exists(p):
            return fm.FontProperties(fname=p)
    return None

FONT = get_font()

def set_chinese(ax, title='', xlabel='', ylabel=''):
    if FONT:
        ax.set_title(title, fontproperties=FONT, fontsize=14)
        ax.set_xlabel(xlabel, fontproperties=FONT, fontsize=11)
        ax.set_ylabel(ylabel, fontproperties=FONT, fontsize=11)
        ax.set_xticklabels(ax.get_xticklabels(), fontproperties=FONT)
        ax.set_yticklabels(ax.get_yticklabels(), fontproperties=FONT)

OUT = os.path.join('..', 'reports')

# ══════════════════════════════════════════════════════════════════════
# A. 粉丝活跃时间分析
# ══════════════════════════════════════════════════════════════════════
def parse_active_time(series):
    """从 '20:00|17.5% 周五|32.2%' 提取高峰小时和高峰星期"""
    hours, weekdays = [], []
    for val in series.dropna():
        # 小时
        h = re.search(r'(\d{1,2}):\d{2}\|', str(val))
        if h:
            hours.append(int(h.group(1)))
        # 星期
        w = re.search(r'(周[一二三四五六日])\|', str(val))
        if w:
            weekdays.append(w.group(1))
    return hours, weekdays

def analyze_active_time(df):
    print("\n====== A. 粉丝活跃时间分析 ======")

    hours, weekdays = parse_active_time(df['粉丝活跃时间'])

    hour_counts = pd.Series(hours).value_counts().sort_index()
    weekday_counts = pd.Series(weekdays).value_counts()
    weekday_order = ['周一','周二','周三','周四','周五','周六','周日']
    weekday_counts = weekday_counts.reindex(weekday_order).fillna(0)

    print(f"有效样本数: {len(hours)}")
    print("\n高峰时段TOP5（小时）:")
    print(pd.Series(hours).value_counts().head(5).to_string())
    print("\n高峰星期分布:")
    print(weekday_counts.to_string())

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # 时段分布
    axes[0].bar(hour_counts.index, hour_counts.values, color='steelblue', alpha=0.8)
    set_chinese(axes[0], title='达人粉丝高峰活跃时段分布', xlabel='小时', ylabel='达人数量')

    # 星期分布
    axes[1].bar(weekday_counts.index, weekday_counts.values, color='coral', alpha=0.8)
    set_chinese(axes[1], title='达人粉丝高峰活跃星期分布', xlabel='', ylabel='达人数量')
    if FONT:
        axes[1].set_xticklabels(weekday_order, fontproperties=FONT)

    plt.tight_layout()
    out = os.path.join(OUT, 'fan_active_time.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n图表已保存: {out}")

    # 按聚类群体分析（如果有cluster列）
    if 'cluster' in df.columns:
        df2 = df.copy()
        df2['peak_hour'] = df2['粉丝活跃时间'].apply(
            lambda x: int(re.search(r'(\d{1,2}):\d{2}\|', str(x)).group(1))
            if re.search(r'(\d{1,2}):\d{2}\|', str(x)) else np.nan
        )
        print("\n各聚类群体平均高峰时段:")
        print(df2.groupby('cluster')['peak_hour'].mean().round(1).to_string())


# ══════════════════════════════════════════════════════════════════════
# B. 图文 vs 视频 内容形式对比
# ══════════════════════════════════════════════════════════════════════
def analyze_format_comparison(df):
    print("\n====== B. 图文 vs 视频 内容形式对比 ======")

    both = df[df['图文笔记报价'].notna() & df['视频笔记报价'].notna()].copy()
    print(f"两者均有报价的达人数: {len(both)}")

    # 去除极端异常值（top 1%）
    for col in ['图文笔记报价','视频笔记报价','图文CPE','视频CPE']:
        q99 = both[col].quantile(0.99)
        both = both[both[col] <= q99]
    print(f"去除top1%异常值后样本数: {len(both)}")

    # 汇总对比
    summary = pd.DataFrame({
        '图文': [both['图文笔记报价'].median(), both['图文CPE'].median(), both['图文CPM'].median()],
        '视频': [both['视频笔记报价'].median(), both['视频CPE'].median(), both['视频CPM'].median()],
    }, index=['报价中位数(元)', 'CPE中位数(元)', 'CPM中位数(元)'])
    print("\n图文 vs 视频 核心指标对比（中位数）:")
    print(summary.round(2).to_string())

    # 各认证类型下的对比
    print("\n按认证类型分组的CPE对比（中位数）:")
    group = both.groupby('认证类型')[['图文CPE','视频CPE']].median().round(2)
    print(group.to_string())

    # 可视化
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))

    # 报价分布对比（箱线图）
    axes[0].boxplot(
        [both['图文笔记报价'].dropna(), both['视频笔记报价'].dropna()],
        labels=['图文', '视频'], patch_artist=True,
        boxprops=dict(facecolor='steelblue', alpha=0.6)
    )
    set_chinese(axes[0], title='图文 vs 视频 报价分布对比', xlabel='内容形式', ylabel='报价（元）')
    if FONT:
        axes[0].set_xticklabels(['图文', '视频'], fontproperties=FONT)

    # CPE对比（箱线图）
    axes[1].boxplot(
        [both['图文CPE'].dropna(), both['视频CPE'].dropna()],
        labels=['图文', '视频'], patch_artist=True,
        boxprops=dict(facecolor='coral', alpha=0.6)
    )
    set_chinese(axes[1], title='图文 vs 视频 CPE（互动成本）对比', xlabel='内容形式', ylabel='CPE（元）')
    if FONT:
        axes[1].set_xticklabels(['图文', '视频'], fontproperties=FONT)

    plt.tight_layout()
    out = os.path.join(OUT, 'format_comparison.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n图表已保存: {out}")

    # 谁更适合哪种形式
    both['更划算形式'] = both.apply(
        lambda r: '图文' if r['图文CPE'] < r['视频CPE'] else '视频', axis=1
    )
    print("\n各认证类型中更划算的内容形式占比:")
    print(both.groupby('认证类型')['更划算形式'].value_counts(normalize=True).round(3).to_string())


# ══════════════════════════════════════════════════════════════════════
# 主程序
# ══════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    input_file = os.path.join('..', 'data', 'combined_cleaned_final.csv')
    if not os.path.exists(input_file):
        input_file = os.path.join('..', 'data', 'combined.csv')

    df = pd.read_csv(input_file)
    print(f"载入数据: {len(df)} 条")

    analyze_active_time(df)
    analyze_format_comparison(df)

    print("\n全部分析完成。")
