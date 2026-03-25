"""
6_deep_mining.py
四个维度的深度挖掘：
  A. 粉丝活跃时间 × 聚类群体交叉分析
  B. 图文 vs 视频 × 聚类群体交叉分析
  C. 粉丝关注焦点分布 + 与达人标签的匹配度
  D. 认证信息（具体职业）× 商业与互动表现
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import ast, re, os
from collections import Counter

# ── 字体 ──────────────────────────────────────────────────────────────
def get_font():
    for p in ['/System/Library/Fonts/PingFang.ttc',
              '/System/Library/Fonts/STHeiti Light.ttc']:
        if os.path.exists(p):
            return fm.FontProperties(fname=p)
    return None

FP = get_font()
OUT = os.path.join('..', 'reports')

def xtick_font(ax, labels=None):
    """统一设置中文刻度标签"""
    if FP is None:
        return
    ticks = ax.get_xticks()
    if labels is None:
        labels = [t.get_text() for t in ax.get_xticklabels()]
    ax.set_xticks(ticks[:len(labels)])
    ax.set_xticklabels(labels, fontproperties=FP, fontsize=10)

def title_font(ax, title, xlabel='', ylabel=''):
    if FP:
        ax.set_title(title, fontproperties=FP, fontsize=13)
        if xlabel: ax.set_xlabel(xlabel, fontproperties=FP, fontsize=11)
        if ylabel: ax.set_ylabel(ylabel, fontproperties=FP, fontsize=11)

CLUSTER_NAMES = {0: '大众生活家', 1: '话题引爆者', 2: 'Z世代潮流捕手', 3: '垂类干货王'}

# ══════════════════════════════════════════════════════════════════════
# A. 粉丝活跃时间 × 聚类群体
# ══════════════════════════════════════════════════════════════════════
def analyze_active_time(df):
    print("\n" + "="*60)
    print("A. 粉丝活跃时间分析")
    print("="*60)

    # 整体分布
    hour_counts = df['最活跃小时'].value_counts().sort_index()
    weekday_order = ['周一','周二','周三','周四','周五','周六','周日']
    weekday_counts = df['最活跃星期'].value_counts().reindex(weekday_order).fillna(0)

    print(f"\n有效样本: {df['最活跃小时'].notna().sum()} 条")
    print("\n【高峰时段 TOP5】")
    print(df['最活跃小时'].value_counts().head(5).to_string())
    print("\n【高峰星期分布】")
    print(weekday_counts.to_string())

    # 按群体交叉
    print("\n【各聚类群体 平均高峰时段 & 最常见高峰星期】")
    cross = df.groupby('群体标签').agg(
        平均高峰时段=('最活跃小时', 'mean'),
        样本量=('最活跃小时', 'count')
    ).round(1)
    cross['群体名称'] = cross.index.map(CLUSTER_NAMES)
    print(cross.to_string())

    weekday_by_cluster = df.groupby('群体标签')['最活跃星期'].agg(
        lambda x: x.value_counts().idxmax() if x.notna().any() else '--'
    )
    print("\n各群体最常见高峰星期:")
    for k, v in weekday_by_cluster.items():
        print(f"  {CLUSTER_NAMES.get(k, k)}: {v}")

    # 可视化
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    # 1. 时段柱图
    axes[0].bar(hour_counts.index, hour_counts.values, color='steelblue', alpha=0.8)
    title_font(axes[0], '粉丝高峰活跃时段分布（全体）', '小时', '达人数量')

    # 2. 星期柱图
    axes[1].bar(range(7), weekday_counts.values, color='coral', alpha=0.8)
    title_font(axes[1], '粉丝高峰活跃星期分布（全体）', '', '达人数量')
    axes[1].set_xticks(range(7))
    xtick_font(axes[1], weekday_order)

    # 3. 各群体平均高峰时段
    cluster_hours = df.groupby('群体标签')['最活跃小时'].mean()
    bar_labels = [CLUSTER_NAMES.get(i, str(i)) for i in cluster_hours.index]
    axes[2].bar(range(len(cluster_hours)), cluster_hours.values, color='mediumseagreen', alpha=0.8)
    title_font(axes[2], '各达人群体粉丝平均高峰时段', '群体', '平均小时')
    axes[2].set_xticks(range(len(cluster_hours)))
    xtick_font(axes[2], bar_labels)
    for i, v in enumerate(cluster_hours.values):
        axes[2].text(i, v + 0.1, f'{v:.1f}时', ha='center', fontsize=9,
                     fontproperties=FP if FP else None)

    plt.tight_layout()
    out = os.path.join(OUT, 'A_active_time.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n图已保存: {out}")


# ══════════════════════════════════════════════════════════════════════
# B. 图文 vs 视频 × 聚类群体
# ══════════════════════════════════════════════════════════════════════
def analyze_format(df):
    print("\n" + "="*60)
    print("B. 图文 vs 视频 内容形式对比")
    print("="*60)

    d = df[df['图文笔记报价'].notna() & df['视频笔记报价'].notna()].copy()
    # 去 top1% 极端值
    for c in ['图文笔记报价','视频笔记报价','图文CPE','视频CPE']:
        q = d[c].quantile(0.99)
        d = d[d[c] <= q]
    print(f"有效样本（双报价且去极值后）: {len(d)}")

    # 整体中位数对比
    summary = pd.DataFrame({
        '图文中位数': [d['图文笔记报价'].median(), d['图文CPE'].median(), d['图文CPM'].median()],
        '视频中位数': [d['视频笔记报价'].median(), d['视频CPE'].median(), d['视频CPM'].median()],
    }, index=['报价(元)', 'CPE(元)', 'CPM(元)'])
    print("\n【整体对比（中位数）】")
    print(summary.round(2).to_string())

    # 按认证类型
    print("\n【按认证类型 CPE中位数对比】")
    grp = d.groupby('认证类型')[['图文CPE','视频CPE']].median().round(2)
    print(grp.to_string())

    # 按聚类群体
    print("\n【按聚类群体 CPE中位数对比】")
    d['群体名称'] = d['群体标签'].map(CLUSTER_NAMES)
    grp2 = d.groupby('群体名称')[['图文CPE','视频CPE','图文笔记报价','视频笔记报价']].median().round(1)
    print(grp2.to_string())

    # 哪种更划算
    d['更划算'] = d.apply(lambda r: '图文' if r['图文CPE'] < r['视频CPE'] else '视频', axis=1)
    print("\n【各群体中更划算的内容形式占比】")
    piv = d.groupby('群体名称')['更划算'].value_counts(normalize=True).round(3).unstack(fill_value=0)
    print(piv.to_string())

    # 可视化
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    # 1. 整体报价对比
    axes[0].bar(['图文','视频'],
                [d['图文笔记报价'].median(), d['视频笔记报价'].median()],
                color=['steelblue','coral'], alpha=0.8)
    title_font(axes[0], '图文 vs 视频 报价中位数', '内容形式', '报价（元）')
    xtick_font(axes[0], ['图文','视频'])
    for i, v in enumerate([d['图文笔记报价'].median(), d['视频笔记报价'].median()]):
        axes[0].text(i, v + 200, f'{v:,.0f}', ha='center', fontsize=10)

    # 2. 各群体CPE对比（分组柱图）
    cluster_names = list(CLUSTER_NAMES.values())
    x = np.arange(len(cluster_names))
    w = 0.35
    img_cpe = [d[d['群体标签']==k]['图文CPE'].median() for k in CLUSTER_NAMES]
    vid_cpe = [d[d['群体标签']==k]['视频CPE'].median() for k in CLUSTER_NAMES]
    axes[1].bar(x - w/2, img_cpe, w, label='图文CPE', color='steelblue', alpha=0.8)
    axes[1].bar(x + w/2, vid_cpe, w, label='视频CPE', color='coral', alpha=0.8)
    title_font(axes[1], '各群体 图文/视频 CPE中位数对比', '', 'CPE（元）')
    axes[1].set_xticks(x)
    xtick_font(axes[1], cluster_names)
    if FP:
        axes[1].legend(prop=FP)
    else:
        axes[1].legend()

    # 3. 各群体"更划算"占比堆叠图
    piv2 = d.groupby('群体标签')['更划算'].value_counts(normalize=True).unstack(fill_value=0)
    bottom = np.zeros(len(piv2))
    colors = {'图文': 'steelblue', '视频': 'coral'}
    for col in piv2.columns:
        axes[2].bar(range(len(piv2)), piv2[col].values, bottom=bottom,
                    label=col, color=colors.get(col, 'grey'), alpha=0.8)
        bottom += piv2[col].values
    title_font(axes[2], '各群体更划算内容形式占比', '群体', '占比')
    axes[2].set_xticks(range(len(piv2)))
    xtick_font(axes[2], [CLUSTER_NAMES.get(i, str(i)) for i in piv2.index])
    if FP:
        axes[2].legend(prop=FP)
    else:
        axes[2].legend()

    plt.tight_layout()
    out = os.path.join(OUT, 'B_format_comparison.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n图已保存: {out}")


# ══════════════════════════════════════════════════════════════════════
# C. 粉丝关注焦点 × 达人标签匹配
# ══════════════════════════════════════════════════════════════════════
def analyze_fan_interest(df):
    print("\n" + "="*60)
    print("C. 粉丝关注焦点分析")
    print("="*60)

    # 聚合Top1/2/3关注焦点
    all_interests = []
    for col in ['Top1关注焦点','Top2关注焦点','Top3关注焦点']:
        all_interests.extend(df[col].dropna().tolist())
    interest_counts = Counter(all_interests)
    top_interests = pd.Series(interest_counts).sort_values(ascending=False).head(20)
    print("\n【粉丝关注焦点 TOP20（合并Top1-3）】")
    print(top_interests.to_string())

    # Top1关注焦点分布
    print("\n【Top1关注焦点分布 TOP10】")
    print(df['Top1关注焦点'].value_counts().head(10).to_string())

    # 按群体的Top1关注焦点
    print("\n【各聚类群体 最常见的Top1粉丝关注焦点】")
    for k, name in CLUSTER_NAMES.items():
        sub = df[df['群体标签']==k]['Top1关注焦点'].value_counts().head(3)
        print(f"  {name}: {', '.join([f'{i}({v})' for i, v in sub.items()])}")

    # 达人标签 vs 粉丝关注焦点 匹配分析
    print("\n【达人标签 vs 粉丝Top1关注焦点 匹配度分析】")

    # 解析达人标签
    def parse_tags(x):
        try:
            tags = ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else []
            return [t.strip() for t in tags]
        except:
            return []

    df['标签列表'] = df['达人标签'].apply(parse_tags)
    df['主标签'] = df['标签列表'].apply(lambda x: x[0] if x else None)

    cross = df.groupby('主标签')['Top1关注焦点'].agg(
        lambda x: x.value_counts().idxmax() if x.notna().any() else '--'
    ).reset_index()
    cross.columns = ['达人主标签', '粉丝Top1关注焦点']

    tag_counts = df['主标签'].value_counts()
    cross['达人数量'] = cross['达人主标签'].map(tag_counts)
    cross = cross.sort_values('达人数量', ascending=False).head(15)
    print(cross.to_string(index=False))

    # 可视化
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # 1. 整体Top10关注焦点
    top10 = top_interests.head(10)
    axes[0].barh(range(len(top10)), top10.values[::-1], color='mediumseagreen', alpha=0.8)
    title_font(axes[0], '粉丝关注焦点TOP10（合并Top1-3）', '出现次数', '')
    axes[0].set_yticks(range(len(top10)))
    if FP:
        axes[0].set_yticklabels(top10.index[::-1].tolist(), fontproperties=FP, fontsize=10)
    else:
        axes[0].set_yticklabels(top10.index[::-1].tolist(), fontsize=10)

    # 2. 各群体Top1粉丝关注焦点分布
    cluster_interest_data = {}
    for k, name in CLUSTER_NAMES.items():
        sub = df[df['群体标签']==k]['Top1关注焦点'].value_counts(normalize=True).head(5)
        cluster_interest_data[name] = sub

    colors_list = ['#2196F3','#FF9800','#4CAF50','#E91E63']
    for idx, (name, data) in enumerate(cluster_interest_data.items()):
        y_pos = np.arange(len(data)) + idx * (len(data) + 1)
        axes[1].barh(y_pos, data.values, color=colors_list[idx], alpha=0.75, label=name)
        if FP:
            axes[1].set_yticks(list(axes[1].get_yticks()) + list(y_pos))

    title_font(axes[1], '各群体粉丝关注焦点分布（占比）', '占比', '')
    if FP:
        axes[1].legend(prop=FP)
    else:
        axes[1].legend()

    plt.tight_layout()
    out = os.path.join(OUT, 'C_fan_interest.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n图已保存: {out}")


# ══════════════════════════════════════════════════════════════════════
# D. 认证信息（职业）× 商业与互动表现
# ══════════════════════════════════════════════════════════════════════
def analyze_certification(df):
    print("\n" + "="*60)
    print("D. 认证信息（具体职业）分析")
    print("="*60)

    # 职业归类
    entertainment = ['演员','歌手','艺人','主持人','音乐人','导演','综艺节目嘉宾','公众人物']
    creator = ['时尚博主','vlog博主','美妆博主','搞笑博主','美食博主','博主','自媒体人','旅行博主']
    professional = ['运动员','律师','艺术家','作家','医生','设计师','摄影师']
    commercial = ['服饰鞋帽','粮油调味旗舰店','美妆护肤旗舰店','休闲零食','官方账号','新闻媒体']

    def classify(cert):
        if cert == '--' or pd.isna(cert):
            return '未认证'
        if cert in entertainment:
            return '娱乐明星类'
        if cert in creator:
            return '内容创作者类'
        if cert in professional:
            return '专业人士类'
        if cert in commercial:
            return '商业机构类'
        return '其他'

    df = df.copy()
    df['职业大类'] = df['认证信息'].apply(classify)

    print("\n【职业大类分布】")
    print(df['职业大类'].value_counts().to_string())

    # 有认证信息的达人单独分析
    certified = df[df['职业大类'] != '未认证'].copy()
    print(f"\n有具体职业认证的达人: {len(certified)} 位")

    print("\n【各职业大类 核心指标对比（中位数）】")
    metrics = ['粉丝数','视频笔记报价','视频CPE','活跃粉丝占比','商业笔记占比','藏赞比','评赞比']
    grp = certified.groupby('职业大类')[metrics].median().round(3)
    print(grp.to_string())

    print("\n【具体职业 TOP15 详细数据】")
    top_certs = df[df['认证信息'] != '--']['认证信息'].value_counts().head(15).index
    detail = df[df['认证信息'].isin(top_certs)].groupby('认证信息')[
        ['粉丝数','视频笔记报价','视频CPE','活跃粉丝占比','商业笔记占比']
    ].median().round(2)
    detail['人数'] = df[df['认证信息'].isin(top_certs)].groupby('认证信息').size()
    print(detail.sort_values('人数', ascending=False).to_string())

    # 可视化
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    # 1. 职业大类人数（去掉未认证让图清晰）
    cert_counts = certified['职业大类'].value_counts()
    axes[0].bar(range(len(cert_counts)), cert_counts.values, color='slateblue', alpha=0.8)
    title_font(axes[0], '有认证达人职业大类分布', '职业类别', '人数')
    axes[0].set_xticks(range(len(cert_counts)))
    xtick_font(axes[0], cert_counts.index.tolist())

    # 2. 各职业大类 平均视频报价
    price_by_type = certified.groupby('职业大类')['视频笔记报价'].median().sort_values(ascending=False)
    axes[1].bar(range(len(price_by_type)), price_by_type.values, color='coral', alpha=0.8)
    title_font(axes[1], '各职业大类 视频报价中位数', '职业类别', '报价（元）')
    axes[1].set_xticks(range(len(price_by_type)))
    xtick_font(axes[1], price_by_type.index.tolist())
    for i, v in enumerate(price_by_type.values):
        axes[1].text(i, v + 500, f'{v:,.0f}', ha='center', fontsize=8,
                     fontproperties=FP if FP else None)

    # 3. 各职业大类 CPE对比
    cpe_by_type = certified.groupby('职业大类')['视频CPE'].median().sort_values()
    axes[2].bar(range(len(cpe_by_type)), cpe_by_type.values, color='mediumseagreen', alpha=0.8)
    title_font(axes[2], '各职业大类 视频CPE中位数（越低越好）', '职业类别', 'CPE（元）')
    axes[2].set_xticks(range(len(cpe_by_type)))
    xtick_font(axes[2], cpe_by_type.index.tolist())
    for i, v in enumerate(cpe_by_type.values):
        axes[2].text(i, v + 0.05, f'{v:.2f}', ha='center', fontsize=9)

    plt.tight_layout()
    out = os.path.join(OUT, 'D_certification.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n图已保存: {out}")


# ══════════════════════════════════════════════════════════════════════
# 主程序
# ══════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    df = pd.read_csv(os.path.join('..', 'data', 'daren_clusters.csv'))
    print(f"载入数据: {len(df)} 条，共 {len(df.columns)} 列")

    # 修复最活跃小时列：把 "18:00" 转成数字 18
    def parse_hour(x):
        if pd.isna(x):
            return np.nan
        m = re.search(r'(\d{1,2}):\d{2}', str(x))
        return int(m.group(1)) if m else np.nan
    df['最活跃小时'] = df['最活跃小时'].apply(parse_hour)

    analyze_active_time(df)
    analyze_format(df)
    analyze_fan_interest(df)
    analyze_certification(df)

    print("\n\n全部分析完成，图表已保存至 reports/ 目录。")
