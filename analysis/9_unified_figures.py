"""
9_unified_figures.py
统一色系重绘论文所有图表
色板: #1e3a8a / #3b82f6 / #93c5fd / #9bc5db
输出目录: ../reports/final/
"""

import os, re, ast, itertools
from collections import Counter

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.colors as mcolors
import networkx as nx
from networkx.algorithms import community
import jieba
from wordcloud import WordCloud
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# ── 路径 ──────────────────────────────────────────────────────────────
BASE   = os.path.dirname(os.path.abspath(__file__))
DATA   = os.path.join(BASE, '..', 'data')
OUT    = os.path.join(BASE, '..', 'reports', 'final')
os.makedirs(OUT, exist_ok=True)

# ── 字体 ──────────────────────────────────────────────────────────────
def get_font():
    for p in ['/System/Library/Fonts/PingFang.ttc',
              '/System/Library/Fonts/STHeiti Light.ttc']:
        if os.path.exists(p):
            return fm.FontProperties(fname=p), p
    return None, None

FP, FONT_PATH = get_font()

def fp(size=12):
    return fm.FontProperties(fname=FONT_PATH, size=size) if FONT_PATH else None

def set_ax(ax, title='', xlabel='', ylabel='', title_size=14):
    if FP:
        ax.set_title(title, fontproperties=fp(title_size), pad=10)
        if xlabel: ax.set_xlabel(xlabel, fontproperties=fp(11))
        if ylabel: ax.set_ylabel(ylabel, fontproperties=fp(11))
        ax.set_xticklabels(
            [t.get_text() for t in ax.get_xticklabels()],
            fontproperties=fp(10)
        )
        ax.set_yticklabels(
            [t.get_text() for t in ax.get_yticklabels()],
            fontproperties=fp(10)
        )
    else:
        ax.set_title(title, fontsize=title_size, pad=10)
        if xlabel: ax.set_xlabel(xlabel)
        if ylabel: ax.set_ylabel(ylabel)

def set_xticklabels(ax, labels, size=10):
    ax.set_xticks(range(len(labels)))
    if FP:
        ax.set_xticklabels(labels, fontproperties=fp(size))
    else:
        ax.set_xticklabels(labels, fontsize=size)

# ── 色板 ──────────────────────────────────────────────────────────────
C = {
    'navy':      '#1e3a8a',
    'blue':      '#3b82f6',
    'lightblue': '#93c5fd',
    'skyblue':   '#9bc5db',
}
PALETTE   = [C['navy'], C['blue'], C['lightblue'], C['skyblue']]
CMAP_NAME = 'thesis_blue'

# 注册自定义colormap（深→浅蓝）
_cmap = mcolors.LinearSegmentedColormap.from_list(
    CMAP_NAME, [C['navy'], C['blue'], C['lightblue']]
)
import matplotlib
matplotlib.colormaps.register(_cmap, name=CMAP_NAME, force=True)

CLUSTER_NAMES = {0: '主流生活型', 1: '年轻受众型', 2: '干货变现型'}

# ── 全局样式 ──────────────────────────────────────────────────────────
plt.rcParams.update({
    'figure.facecolor': 'white',
    'axes.facecolor':   'white',
    'axes.edgecolor':   '#cccccc',
    'axes.grid':        True,
    'grid.color':       '#e5e7eb',
    'grid.linewidth':   0.6,
    'axes.spines.top':  False,
    'axes.spines.right':False,
})

# ══════════════════════════════════════════════════════════════════════
# 图1  标签共现词网络
# ══════════════════════════════════════════════════════════════════════
def fig1_tag_network(df):
    print("生成 图1 标签共现网络...")
    df = df.copy()
    df['达人标签_list'] = df['达人标签'].apply(
        lambda x: ast.literal_eval(x)
        if isinstance(x, str) and x.startswith('[') else []
    )
    filtered = df['达人标签_list'].apply(
        lambda tags: [t for t in tags if not t.endswith('其他')]
    )
    tag_lists = filtered[filtered.apply(len) > 1]
    pairs     = [tuple(sorted(p))
                 for tags in tag_lists
                 for p in itertools.combinations(tags, 2)]
    pair_counts = Counter(pairs)

    G_full = nx.Graph()
    for pair, w in pair_counts.most_common(100):
        G_full.add_edge(pair[0], pair[1], weight=w)
    if '接地气生活' in G_full:
        G_full.remove_node('接地气生活')

    comps = list(nx.connected_components(G_full))
    if not comps:
        print("  无数据，跳过。"); return
    G = G_full.subgraph(max(comps, key=len)).copy()

    communities_list = community.louvain_communities(G, seed=42)
    n_comm = len(communities_list)
    comm_colors = [PALETTE[i % len(PALETTE)] for i in range(n_comm)]
    node_color_map = {}
    for i, comm in enumerate(communities_list):
        for node in comm:
            node_color_map[node] = comm_colors[i]
    node_colors = [node_color_map.get(n, C['skyblue']) for n in G.nodes()]

    fig, ax = plt.subplots(figsize=(18, 18), facecolor='white')
    pos = nx.spring_layout(G, k=2.2, iterations=120, seed=42)
    node_sizes  = [G.degree(n) * 320 + 200 for n in G.nodes()]
    edge_widths = [G[u][v]['weight'] * 0.45 for u, v in G.edges()]

    nx.draw_networkx_nodes(G, pos, node_size=node_sizes,
                           node_color=node_colors, alpha=0.88, ax=ax)
    nx.draw_networkx_edges(G, pos, width=edge_widths,
                           edge_color='#d1d5db', alpha=0.65, ax=ax)
    if FONT_PATH:
        for node, (x, y) in pos.items():
            ax.text(x, y, node, fontproperties=fp(12),
                    ha='center', va='center',
                    bbox=dict(facecolor='white', alpha=0.6,
                              edgecolor='none', boxstyle='round,pad=0.2'))
        ax.set_title('图1  达人标签共现词网络', fontproperties=fp(20), pad=14)
    else:
        nx.draw_networkx_labels(G, pos, ax=ax)
        ax.set_title('图1  达人标签共现词网络', fontsize=20)
    ax.axis('off')

    out = os.path.join(OUT, 'fig1_tag_network.png')
    fig.savefig(out, dpi=180, bbox_inches='tight')
    plt.close(fig)
    print(f"  ✅ {out}")


# ══════════════════════════════════════════════════════════════════════
# 图2  粉丝关注焦点分布
# ══════════════════════════════════════════════════════════════════════
def fig2_fan_interest(df):
    print("生成 图2 粉丝关注焦点分布...")
    counter = Counter()
    for val in df['Top1关注焦点'].dropna():
        counter[str(val).strip()] += 1
    top = pd.Series(dict(counter.most_common(12)))

    fig, ax = plt.subplots(figsize=(11, 5))
    colors = [PALETTE[i % len(PALETTE)] for i in range(len(top))]
    bars = ax.bar(range(len(top)), top.values, color=colors, alpha=0.90, zorder=3)
    set_xticklabels(ax, top.index.tolist())
    set_ax(ax, title='图2  高影响力达人粉丝关注焦点分布（Top12）',
           xlabel='', ylabel='达人数量')
    for bar in bars:
        ax.text(bar.get_x() + bar.get_width()/2,
                bar.get_height() + 15,
                f'{int(bar.get_height())}',
                ha='center', va='bottom', fontsize=9,
                fontproperties=fp(9) if FP else None)
    ax.set_xlim(-0.6, len(top) - 0.4)
    fig.tight_layout()
    out = os.path.join(OUT, 'fig2_fan_interest.png')
    fig.savefig(out, dpi=180, bbox_inches='tight')
    plt.close(fig)
    print(f"  ✅ {out}")


# ══════════════════════════════════════════════════════════════════════
# 图3  达人简介高频词词云
# ══════════════════════════════════════════════════════════════════════
def fig3_wordcloud(df):
    print("生成 图3 达人简介词云...")
    if not FONT_PATH:
        print("  未找到中文字体，跳过词云。"); return

    text = ''.join(df['简介'].dropna().astype(str))
    text = re.sub(r'[a-zA-Z0-9\.\/:_@\s]+', '', text)
    stopwords = {
        '的','是','我','你','了','都','在','也','我们','一个','不','就','会',
        '请','看','VX','vx','合作','联系','邮箱','商务','工作','v','这里',
        '主页','小红','书','哦','啊','吧','嗯','呢'
    }
    words = [w for w in jieba.cut(text)
             if w not in stopwords and len(w) > 1]

    # 自定义颜色函数：在色板内随机选色
    def color_func(word, font_size, position, orientation, random_state=None, **kwargs):
        return PALETTE[random_state.randint(0, len(PALETTE) - 1)]

    wc = WordCloud(
        font_path=FONT_PATH,
        width=1000, height=700,
        background_color='white',
        max_words=100,
        color_func=color_func,
        prefer_horizontal=0.85,
        margin=4,
    ).generate(' '.join(words))

    fig, ax = plt.subplots(figsize=(12, 7))
    ax.imshow(wc, interpolation='bilinear')
    ax.axis('off')
    ax.set_title('图3  高影响力达人简介高频词',
                 fontproperties=fp(18), pad=12)
    fig.tight_layout()
    out = os.path.join(OUT, 'fig3_bio_wordcloud.png')
    fig.savefig(out, dpi=180, bbox_inches='tight')
    plt.close(fig)
    print(f"  ✅ {out}")


# ══════════════════════════════════════════════════════════════════════
# 图4  粉丝活跃时段 × 星期分布
# ══════════════════════════════════════════════════════════════════════
def fig4_active_time(df):
    print("生成 图4 粉丝活跃时段与星期...")
    hour_counts    = df['最活跃小时'].value_counts().sort_index()
    weekday_order  = ['周一','周二','周三','周四','周五','周六','周日']
    weekday_counts = df['最活跃星期'].value_counts().reindex(weekday_order).fillna(0)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # 时段
    axes[0].bar(hour_counts.index, hour_counts.values,
                color=C['blue'], alpha=0.88, zorder=3)
    peak_h = hour_counts.idxmax()
    axes[0].bar([peak_h], [hour_counts[peak_h]],
                color=C['navy'], alpha=1.0, zorder=4)
    # 只显示关键时刻刻度，避免拥挤
    shown_hours = [0, 6, 9, 12, 15, 17, 18, 20, 21, 23]
    axes[0].set_xticks(shown_hours)
    axes[0].set_xticklabels(
        [f'{h}:00' for h in shown_hours],
        fontproperties=fp(10) if FP else None,
        rotation=0
    )
    set_ax(axes[0], title='粉丝高峰活跃时段分布',
           xlabel='时段（小时）', ylabel='达人数量')

    # 星期
    bar_colors = [C['navy'] if v == weekday_counts.max() else C['lightblue']
                  for v in weekday_counts.values]
    axes[1].bar(range(7), weekday_counts.values,
                color=bar_colors, alpha=0.90, zorder=3)
    set_xticklabels(axes[1], weekday_order)
    set_ax(axes[1], title='粉丝高峰活跃星期分布', ylabel='达人数量')

    fig.suptitle('图4  高影响力达人粉丝高峰活跃时段与星期分布',
                 fontproperties=fp(15) if FP else None, fontsize=15, y=1.02)
    fig.tight_layout()
    out = os.path.join(OUT, 'fig4_active_time.png')
    fig.savefig(out, dpi=180, bbox_inches='tight')
    plt.close(fig)
    print(f"  ✅ {out}")


# ══════════════════════════════════════════════════════════════════════
# 图5  图文 vs 视频 内容形式效能对比
# ══════════════════════════════════════════════════════════════════════
def fig5_format_comparison(df):
    print("生成 图5 内容形式效能对比...")
    d = df[df['图文笔记报价'].notna() & df['视频笔记报价'].notna()].copy()
    for c in ['图文笔记报价','视频笔记报价','图文CPE','视频CPE']:
        q = d[c].quantile(0.99)
        d = d[d[c] <= q]

    # 群体名称映射
    if '群体标签_k3' in d.columns:
        d['群体名称'] = d['群体标签_k3'].map(CLUSTER_NAMES)
    elif '群体标签' in d.columns:
        d['群体名称'] = d['群体标签'].map(CLUSTER_NAMES)

    fig, axes = plt.subplots(1, 3, figsize=(17, 5))

    # 1. 报价中位数对比
    vals   = [d['图文笔记报价'].median(), d['视频笔记报价'].median()]
    labels = ['图文', '视频']
    bars   = axes[0].bar(labels, vals,
                         color=[C['lightblue'], C['navy']], alpha=0.90, zorder=3)
    set_xticklabels(axes[0], labels)
    set_ax(axes[0], title='图文 vs 视频 报价中位数', ylabel='报价（元）')
    for bar, v in zip(bars, vals):
        axes[0].text(bar.get_x() + bar.get_width()/2,
                     v + 200, f'{v:,.0f}',
                     ha='center', fontsize=10,
                     fontproperties=fp(10) if FP else None)

    # 2. 各群体 CPE 分组柱图
    group_names = list(CLUSTER_NAMES.values())
    col_key     = '群体标签_k3' if '群体标签_k3' in d.columns else '群体标签'
    img_cpe = [d[d[col_key]==k]['图文CPE'].median() for k in CLUSTER_NAMES]
    vid_cpe = [d[d[col_key]==k]['视频CPE'].median() for k in CLUSTER_NAMES]
    x = np.arange(len(group_names))
    w = 0.35
    axes[1].bar(x - w/2, img_cpe, w, label='图文CPE',
                color=C['lightblue'], alpha=0.90, zorder=3)
    axes[1].bar(x + w/2, vid_cpe, w, label='视频CPE',
                color=C['navy'], alpha=0.90, zorder=3)
    set_xticklabels(axes[1], group_names)
    set_ax(axes[1], title='各群体 图文/视频 CPE中位数', ylabel='CPE（元）')
    axes[1].legend(prop=fp(10) if FP else None)

    # 3. 各群体更划算形式占比（堆叠）
    d['更划算'] = d.apply(
        lambda r: '图文' if r['图文CPE'] < r['视频CPE'] else '视频', axis=1
    )
    piv = d.groupby(col_key)['更划算'].value_counts(normalize=True).unstack(fill_value=0)
    bottom = np.zeros(len(piv))
    color_map = {'图文': C['lightblue'], '视频': C['navy']}
    for col in piv.columns:
        axes[2].bar(range(len(piv)), piv[col].values, bottom=bottom,
                    label=col, color=color_map.get(col, C['blue']),
                    alpha=0.90, zorder=3)
        bottom += piv[col].values
    set_xticklabels(axes[2],
                    [CLUSTER_NAMES.get(i, str(i)) for i in piv.index])
    set_ax(axes[2], title='各群体更划算内容形式占比', ylabel='占比')
    axes[2].legend(prop=fp(10) if FP else None)

    fig.suptitle('图5  图文与视频内容形式效能对比',
                 fontproperties=fp(15) if FP else None, fontsize=15, y=1.02)
    fig.tight_layout()
    out = os.path.join(OUT, 'fig5_format_comparison.png')
    fig.savefig(out, dpi=180, bbox_inches='tight')
    plt.close(fig)
    print(f"  ✅ {out}")


# ══════════════════════════════════════════════════════════════════════
# 图6  肘部法则 + 轮廓系数
# ══════════════════════════════════════════════════════════════════════
def fig6_elbow(df):
    print("生成 图6 肘部法则与轮廓系数...")
    features = ['粉丝数','Top1年龄段占比','藏赞比','评赞比','商业笔记占比']

    # 剔除离群值
    mean_er  = df['评赞比'].mean();  std_er  = df['评赞比'].std()
    mean_biz = df['商业笔记占比'].mean(); std_biz = df['商业笔记占比'].std()
    mask = (df['评赞比'] > mean_er + 3*std_er) | \
           (df['商业笔记占比'] > mean_biz + 3*std_biz)
    df_clean = df[~mask].copy()

    X = df_clean[features].fillna(df_clean[features].median())
    X_scaled = StandardScaler().fit_transform(X)

    inertias, silhouettes = [], []
    K_range = range(2, 9)
    for k in K_range:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X_scaled)
        inertias.append(km.inertia_)
        silhouettes.append(silhouette_score(X_scaled, labels))

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))

    # 肘部法则
    axes[0].plot(list(K_range), inertias,
                 color=C['blue'], linewidth=2.2, marker='o',
                 markerfacecolor=C['navy'], markersize=7, zorder=3)
    axes[0].axvline(x=3, color=C['navy'], linestyle='--', linewidth=1.4,
                    alpha=0.8, label='K=3')
    axes[0].scatter([3], [inertias[1]], color=C['navy'], s=80, zorder=5)
    set_ax(axes[0], title='肘部法则（Elbow Method）',
           xlabel='K值（聚类数）', ylabel='组内惯性（Inertia）')
    axes[0].legend(prop=fp(10) if FP else None)

    # 轮廓系数
    axes[1].plot(list(K_range), silhouettes,
                 color=C['blue'], linewidth=2.2, marker='s',
                 markerfacecolor=C['navy'], markersize=7, zorder=3)
    axes[1].axvline(x=3, color=C['navy'], linestyle='--', linewidth=1.4,
                    alpha=0.8, label='K=3')
    axes[1].scatter([3], [silhouettes[1]], color=C['navy'], s=80, zorder=5)
    set_ax(axes[1], title='轮廓系数（Silhouette Score）',
           xlabel='K值（聚类数）', ylabel='轮廓系数')
    axes[1].legend(prop=fp(10) if FP else None)

    fig.suptitle('图6  K值选取：肘部法则与轮廓系数',
                 fontproperties=fp(15) if FP else None, fontsize=15, y=1.02)
    fig.tight_layout()
    out = os.path.join(OUT, 'fig6_elbow_silhouette.png')
    fig.savefig(out, dpi=180, bbox_inches='tight')
    plt.close(fig)

    print(f"  ✅ {out}")
    print(f"  轮廓系数各K值: { {k: round(s,4) for k,s in zip(K_range, silhouettes)} }")


# ══════════════════════════════════════════════════════════════════════
# 主程序
# ══════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    csv = os.path.join(DATA, 'daren_clusters_k3.csv')
    if not os.path.exists(csv):
        csv = os.path.join(DATA, 'combined_cleaned_final.csv')

    print(f"读取数据: {csv}")
    df = pd.read_csv(csv)

    # 补充衍生指标（如缺失）
    if '藏赞比' not in df.columns:
        df['藏赞比'] = (df['近60天平均收藏'] /
                       df['近60天平均点赞'].replace(0, np.nan)).round(4)
    if '评赞比' not in df.columns:
        df['评赞比'] = (df['近60天平均评论'] /
                       df['近60天平均点赞'].replace(0, np.nan)).round(4)
    if '商业笔记占比' not in df.columns:
        df['商业笔记占比'] = (df['商业笔记总数'] /
                            df['笔记总数'].replace(0, np.nan)).round(4)

    print(f"共 {len(df)} 条记录\n")

    fig1_tag_network(df)
    fig2_fan_interest(df)
    fig3_wordcloud(df)
    fig4_active_time(df)
    fig5_format_comparison(df)
    fig6_elbow(df)

    print(f"\n全部完成，图表保存在:\n{os.path.abspath(OUT)}/")
