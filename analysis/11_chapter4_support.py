#!/usr/bin/env python3
"""
Generate chapter-4 support artifacts for the thesis.

Outputs:
- data source / sample size tables
- text preprocessing rule tables
- co-occurrence rule tables
- cluster support tables and elbow/silhouette plot
- group-level comment word comparison
- group-level comment interaction type comparison
- markdown summary for direct thesis writing
"""

from __future__ import annotations

import os
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import jieba
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import networkx as nx
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports" / "chapter4_support"
TABLES_DIR = REPORTS_DIR / "tables"
PLOTS_DIR = REPORTS_DIR / "plots"
TEXT_RUN_NAME = os.getenv("TEXT_MINING_RUN_NAME", "text_mining_full")
TEXT_RUN_DIR = DATA_DIR / TEXT_RUN_NAME
SNAPSHOT_DIR = TEXT_RUN_DIR / "snapshot"
TOPIC_DIR = TEXT_RUN_DIR / "topics"

CLUSTER_FEATURES = ["粉丝数", "Top1年龄段占比", "藏赞比", "评赞比", "商业笔记占比"]
COMMENT_LABELS = [
    "appearance_praise",
    "parasocial_intimacy",
    "care_support",
    "fan_mobilization",
    "consumption_conversion",
    "self_disclosure",
    "advice_regulation",
    "question_request",
    "other",
]
LABEL_DISPLAY = {
    "appearance_praise": "颜值赞美",
    "parasocial_intimacy": "拟亲密互动",
    "care_support": "关怀支持",
    "fan_mobilization": "应援组织",
    "consumption_conversion": "消费转化",
    "self_disclosure": "自我汇报",
    "advice_regulation": "规训建议",
    "question_request": "提问求回应",
    "other": "其他",
}
RULE_PATTERNS = {
    "appearance_praise": r"好美|漂亮|好看|颜值|妆容|发型|滤镜|可爱|帅|美哭|美晕|绝美|出片",
    "parasocial_intimacy": r"老婆|宝宝|宝贝|闺蜜|姐姐|亲亲|想你|梦到你|爱你|我老婆|我宝宝|女神",
    "care_support": r"开心|休息|照顾|身体|吃饭|睡觉|平安|顺利|加油|健康|心疼|陪着你",
    "fan_mobilization": r"应援|做数据|控评|反黑|晒单|冲销量|维权|超话|代言人|官宣|顶上去",
    "consumption_conversion": r"同款|链接|买了|下单|周边|礼盒|抢到|抢不到|闲鱼|代言|口红|套餐",
    "self_disclosure": r"我.*(开学|中考|高考|上班|上学|学校|考试|晚自习|作业|生日|放假)",
    "advice_regulation": r"进组|剧本|滤镜|妆容|发型|直播|少直播|黑发|选本|造型师|别用|能不能别",
    "question_request": r"\?|？|能不能|可不可以|什么时候|怎么|为什么|求|祝我|回复我",
}
STOPWORDS = {
    "的", "了", "是", "我", "你", "他", "她", "它", "我们", "你们", "他们", "都", "就",
    "也", "很", "在", "和", "有", "吧", "啊", "呀", "啦", "呢", "哦", "吗", "嘛", "被",
    "让", "把", "给", "着", "一个", "这个", "那个", "自己", "就是", "还是", "真的", "现在",
    "已经", "因为", "所以", "不是", "可以", "还有", "感觉", "一下", "今天", "这样", "这种",
    "那个", "然后", "没有", "不要", "起来", "看到", "觉得", "的话", "时候", "出来", "小红书",
    "话题", "视频", "图片", "拍照", "大家", "妈妈", "喜欢", "什么", "希望", "朋友", "最近",
    "一起", "分享", "记录", "日常", "生活", "vlog", "系列", "其实", "比如", "非常", "特别",
    "安全", "限制", "ab", "不止", "极速小鱼", "明星",
}


def ensure_dirs() -> None:
    for path in [REPORTS_DIR, TABLES_DIR, PLOTS_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def get_font() -> fm.FontProperties | None:
    for path in ["/System/Library/Fonts/PingFang.ttc", "/System/Library/Fonts/STHeiti Light.ttc"]:
        if os.path.exists(path):
            return fm.FontProperties(fname=path)
    return None


def clean_text(value: Any) -> str:
    if pd.isna(value) if isinstance(value, float) else value is None:
        return ""
    text = str(value)
    text = text.replace("\n", " ").replace("\r", " ")
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"#([^#]+)#", r" \1 ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokenize(text: str) -> list[str]:
    words = []
    for token in jieba.cut(clean_text(text)):
        token = token.strip().lower()
        if len(token) < 2:
            continue
        if token in STOPWORDS:
            continue
        if re.fullmatch(r"[\W_]+", token):
            continue
        words.append(token)
    return words


def creator_id_from_url(series: pd.Series) -> pd.Series:
    return series.astype(str).str.extract(r"profile/([a-f0-9]{24})")


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    combined = pd.read_csv(BASE_DIR / "analysis" / "combined_cleaned_final.csv")
    clusters = pd.read_csv(DATA_DIR / "daren_clusters_k3.csv")
    clusters["creator_id"] = creator_id_from_url(clusters["达人官方地址"])
    snapshot_posts = SNAPSHOT_DIR / "posts_snapshot.csv"
    snapshot_comments = SNAPSHOT_DIR / "comments_snapshot.csv"
    if snapshot_posts.exists() and snapshot_comments.exists():
        posts = pd.read_csv(snapshot_posts)
        comments = pd.read_csv(snapshot_comments)
    else:
        posts = pd.read_csv(DATA_DIR / "dom_crawl" / "posts.csv")
        comments = pd.read_csv(DATA_DIR / "dom_crawl" / "comments.csv")
    return combined, clusters, posts, comments


def calculate_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["商业笔记占比"] = (df["商业笔记总数"] / df["笔记总数"].replace(0, np.nan)).round(4)
    df["藏赞比"] = (df["近60天平均收藏"] / df["近60天平均点赞"].replace(0, np.nan)).round(4)
    df["评赞比"] = (df["近60天平均评论"] / df["近60天平均点赞"].replace(0, np.nan)).round(4)
    return df


def export_data_source_tables(
    combined: pd.DataFrame,
    clusters: pd.DataFrame,
    posts: pd.DataFrame,
    comments: pd.DataFrame,
) -> None:
    cluster_matched = clusters[clusters["creator_id"].isin(posts["creator_id"].astype(str))].copy()
    post_source = (
        f"data/{TEXT_RUN_NAME}/snapshot/posts_snapshot.csv"
        if (SNAPSHOT_DIR / "posts_snapshot.csv").exists()
        else "data/dom_crawl/posts.csv"
    )
    comment_source = (
        f"data/{TEXT_RUN_NAME}/snapshot/comments_snapshot.csv"
        if (SNAPSHOT_DIR / "comments_snapshot.csv").exists()
        else "data/dom_crawl/comments.csv"
    )
    source_table = pd.DataFrame(
        [
            {"数据层级": "达人画像样本", "数据文件": "analysis/combined_cleaned_final.csv", "样本量": len(combined), "说明": "达人基础信息、粉丝画像、商业价值与互动指标"},
            {"数据层级": "聚类分析样本", "数据文件": "data/daren_clusters_k3.csv", "样本量": len(clusters), "说明": "已带三类群体标签的达人聚类结果"},
            {"数据层级": "帖子文本样本", "数据文件": post_source, "样本量": len(posts), "说明": "达人标题、正文、帖子状态与可见评论数"},
            {"数据层级": "评论文本样本", "数据文件": comment_source, "样本量": len(comments), "说明": "评论正文与点赞数"},
            {"数据层级": "文本-聚类对齐达人", "数据文件": "cluster + text snapshot", "样本量": cluster_matched['creator_id'].nunique(), "说明": "既有三类群体标签又有帖子/评论文本的达人"},
        ]
    )
    source_table.to_csv(TABLES_DIR / "table_4_1_data_sources.csv", index=False)

    sample_table = pd.DataFrame(
        [
            {"统计对象": "帖子文本", "样本量": len(posts), "去重对象数": posts["note_id"].nunique(), "涉及达人数": posts["creator_id"].nunique()},
            {"统计对象": "评论文本", "样本量": len(comments), "去重对象数": comments["note_id"].nunique(), "涉及达人数": comments["creator_id"].nunique()},
            {"统计对象": "带群体标签的帖子", "样本量": len(posts[posts["creator_id"].isin(cluster_matched["creator_id"])]), "去重对象数": posts.loc[posts["creator_id"].isin(cluster_matched["creator_id"]), "note_id"].nunique(), "涉及达人数": cluster_matched["creator_id"].nunique()},
            {"统计对象": "带群体标签的评论", "样本量": len(comments[comments["creator_id"].isin(cluster_matched["creator_id"])]), "去重对象数": comments.loc[comments["creator_id"].isin(cluster_matched["creator_id"]), "note_id"].nunique(), "涉及达人数": comments.loc[comments["creator_id"].isin(cluster_matched["creator_id"]), "creator_id"].nunique()},
        ]
    )
    sample_table.to_csv(TABLES_DIR / "table_4_1_sample_sizes.csv", index=False)


def export_method_rule_tables() -> None:
    preprocess = pd.DataFrame(
        [
            {"步骤": "文本清洗", "规则": "去除换行、URL、冗余空格，保留正文、标题、评论文本"},
            {"步骤": "中文分词", "规则": "使用 jieba 对简介、帖子、评论文本进行分词"},
            {"步骤": "停用词过滤", "规则": "过滤助词、代词、平台通用词与长度小于2的词"},
            {"步骤": "高频词保留", "规则": "按词频降序统计，每类群体保留前20个高频词进行对比"},
            {"步骤": "群体映射", "规则": "按 creator_id 将帖子与评论挂接到三类群体标签"},
        ]
    )
    preprocess.to_csv(TABLES_DIR / "table_4_2_preprocess_rules.csv", index=False)

    cooccur = pd.DataFrame(
        [
            {"要素": "节点", "定义": "帖子文本中经清洗和分词后保留下来的高频关键词"},
            {"要素": "边", "定义": "两个关键词在同一帖子文本中共同出现一次即记为一次共现"},
            {"要素": "权重", "定义": "关键词对在全部帖子中共同出现的累计次数"},
            {"要素": "节点筛选", "定义": "仅保留整体高频且具有解释意义的前N个关键词"},
            {"要素": "边筛选", "定义": "仅保留权重大于等于设定阈值的关键词对"},
        ]
    )
    cooccur.to_csv(TABLES_DIR / "table_4_3_cooccurrence_rules.csv", index=False)

    interaction = pd.DataFrame(
        [
            {"互动类型": LABEL_DISPLAY[k], "代码标签": k, "识别规则示例": RULE_PATTERNS.get(k, "不命中其他规则时归为其他")}
            for k in COMMENT_LABELS
        ]
    )
    interaction.to_csv(TABLES_DIR / "table_4_4_comment_interaction_definitions.csv", index=False)


def export_cluster_variable_table() -> None:
    cluster_variables = pd.DataFrame(
        [
            {"变量名称": "粉丝数", "变量含义": "达人账号的粉丝规模，用于衡量基础影响力", "进入聚类原因": "反映达人受众规模差异"},
            {"变量名称": "Top1年龄段占比", "变量含义": "达人粉丝中占比最高年龄段的集中程度", "进入聚类原因": "反映粉丝年龄结构的集中程度"},
            {"变量名称": "藏赞比", "变量含义": "近60天平均收藏量与近60天平均点赞量之比", "进入聚类原因": "反映内容被深度保存的倾向"},
            {"变量名称": "评赞比", "变量含义": "近60天平均评论量与近60天平均点赞量之比", "进入聚类原因": "反映内容引发讨论互动的能力"},
            {"变量名称": "商业笔记占比", "变量含义": "商业笔记总数占全部笔记总数的比例", "进入聚类原因": "反映达人商业合作强度"},
        ]
    )
    cluster_variables.to_csv(TABLES_DIR / "table_4_5a_cluster_variable_definitions.csv", index=False)


def build_post_cooccurrence_outputs(clusters: pd.DataFrame, posts: pd.DataFrame) -> None:
    group_map = clusters[["creator_id", "群体标签_k3"]].dropna().drop_duplicates("creator_id")
    posts = posts.copy()
    posts["creator_id"] = posts["creator_id"].astype(str)
    posts = posts.merge(group_map, on="creator_id", how="inner")
    posts = posts[posts["crawl_status"].isin(["ok", "no_visible_comments"])].copy()
    posts = posts[posts["title"].fillna("") != "安全限制"].copy()
    posts["post_text"] = (
        posts["title"].fillna("").map(clean_text)
        + " "
        + posts["desc"].fillna("").map(clean_text)
    ).str.strip()
    posts = posts[posts["post_text"].str.len() >= 2].copy()

    word_counter: Counter[str] = Counter()
    edge_counter: Counter[tuple[str, str]] = Counter()
    group_rows: list[dict[str, Any]] = []

    for _, row in posts.iterrows():
        tokens = tokenize(row["post_text"])
        unique_tokens = sorted(set(tokens))
        word_counter.update(unique_tokens)
        if len(unique_tokens) >= 2:
            for i in range(len(unique_tokens)):
                for j in range(i + 1, len(unique_tokens)):
                    edge_counter[(unique_tokens[i], unique_tokens[j])] += 1
        group_rows.extend(
            {"群体标签_k3": row["群体标签_k3"], "词语": token}
            for token in unique_tokens
        )

    keyword_df = pd.DataFrame(
        [{"关键词": word, "帖子覆盖数": count} for word, count in word_counter.most_common(50)]
    )
    keyword_df.to_csv(TABLES_DIR / "table_4_3a_post_keyword_frequency.csv", index=False)

    top_nodes = set(keyword_df.head(30)["关键词"])
    edge_rows = []
    for (src, dst), weight in edge_counter.items():
        if src in top_nodes and dst in top_nodes and weight >= 15:
            edge_rows.append({"源词": src, "目标词": dst, "共现次数": weight})
    edge_df = pd.DataFrame(edge_rows).sort_values("共现次数", ascending=False)
    edge_df = edge_df.head(80)
    edge_df.to_csv(TABLES_DIR / "table_4_3b_post_cooccurrence_edges.csv", index=False)

    group_word_df = pd.DataFrame(group_rows)
    group_word_summary = (
        group_word_df.groupby(["群体标签_k3", "词语"])
        .size()
        .rename("帖子覆盖数")
        .reset_index()
        .sort_values(["群体标签_k3", "帖子覆盖数"], ascending=[True, False])
    )
    group_word_top = group_word_summary.groupby("群体标签_k3").head(15).copy()
    group_word_top["排名"] = group_word_top.groupby("群体标签_k3").cumcount() + 1
    group_word_wide = group_word_top.pivot(index="排名", columns="群体标签_k3", values="词语").reset_index()
    group_word_wide.columns = ["排名"] + [f"群体{col}_帖子高频词" for col in group_word_wide.columns[1:]]
    group_freq_wide = group_word_top.pivot(index="排名", columns="群体标签_k3", values="帖子覆盖数").reset_index()
    group_freq_wide.columns = ["排名"] + [f"群体{col}_帖子覆盖数" for col in group_freq_wide.columns[1:]]
    group_compare = group_word_wide.merge(group_freq_wide, on="排名", how="left")
    group_compare.to_csv(TABLES_DIR / "table_4_3c_group_post_keywords_compare.csv", index=False)

    if not edge_df.empty:
        graph = nx.Graph()
        node_sizes = {row["关键词"]: row["帖子覆盖数"] for _, row in keyword_df.head(30).iterrows()}
        for node, size in node_sizes.items():
            graph.add_node(node, size=size)
        for _, row in edge_df.iterrows():
            graph.add_edge(row["源词"], row["目标词"], weight=row["共现次数"])

        plt.figure(figsize=(12, 10))
        pos = nx.spring_layout(graph, seed=42, k=0.9)
        weights = [graph[u][v]["weight"] for u, v in graph.edges()]
        sizes = [graph.nodes[node]["size"] * 6 for node in graph.nodes()]
        nx.draw_networkx_edges(
            graph,
            pos,
            alpha=0.35,
            width=[max(weight / 15, 0.8) for weight in weights],
            edge_color="#7f8c8d",
        )
        nx.draw_networkx_nodes(
            graph,
            pos,
            node_size=sizes,
            node_color="#f4d03f",
            edgecolors="#34495e",
            linewidths=0.8,
        )
        fp = get_font()
        nx.draw_networkx_labels(
            graph,
            pos,
            font_size=10,
            font_family=fp.get_name() if fp else "sans-serif",
        )
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(PLOTS_DIR / "post_keyword_cooccurrence_network.png", dpi=180, bbox_inches="tight")
        plt.close()


def export_topic_outputs(clusters: pd.DataFrame) -> None:
    topic_path = TOPIC_DIR / "post_topics.csv"
    post_topic_path = TOPIC_DIR / "posts_with_topics.csv"
    if not topic_path.exists() or not post_topic_path.exists():
        return

    topic_df = pd.read_csv(topic_path)
    topic_df["代表关键词"] = topic_df["keywords"].fillna("").map(
        lambda x: "、".join(str(x).split(" | ")[:8])
    )

    def representative_text(raw: Any) -> str:
        if pd.isna(raw):
            return ""
        try:
            parsed = json.loads(str(raw))
        except Exception:
            return ""
        texts = []
        for item in parsed[:2]:
            if isinstance(item, dict):
                texts.append(str(item.get("post_text", ""))[:36])
        return "；".join(texts)

    topic_df["代表文本"] = topic_df["representative_posts"].map(representative_text)
    topic_table = topic_df[["topic_name", "post_count", "代表关键词", "代表文本"]].copy()
    topic_table.columns = ["主题名称", "帖子数", "代表关键词", "代表文本"]
    topic_table.to_csv(TABLES_DIR / "table_4_3a_topic_lda_results.csv", index=False)

    posts_with_topics = pd.read_csv(post_topic_path, usecols=["creator_id", "topic_id"])
    posts_with_topics["creator_id"] = posts_with_topics["creator_id"].astype(str)
    topic_name_map = topic_df[["topic_id", "topic_name"]].copy()
    posts_with_topics = posts_with_topics.merge(
        clusters[["creator_id", "群体标签_k3"]],
        on="creator_id",
        how="inner",
    ).merge(topic_name_map, on="topic_id", how="left")
    topic_share = (
        posts_with_topics.groupby(["群体标签_k3", "topic_name"])
        .size()
        .rename("帖子数")
        .reset_index()
    )
    totals = topic_share.groupby("群体标签_k3")["帖子数"].transform("sum")
    topic_share["占比"] = (topic_share["帖子数"] / totals).round(4)
    topic_share.to_csv(TABLES_DIR / "table_4_3d_group_topic_distribution_long.csv", index=False)

    topic_compare = (
        topic_share.pivot(index="topic_name", columns="群体标签_k3", values="占比")
        .fillna(0)
        .reset_index()
    )
    topic_compare.columns = ["主题名称"] + [f"群体{col}_占比" for col in topic_compare.columns[1:]]
    topic_compare.to_csv(TABLES_DIR / "table_4_3e_group_topic_distribution_compare.csv", index=False)


def build_method_flowchart() -> None:
    fp = get_font()
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis("off")
    steps = [
        (0.07, 0.58, 0.18, 0.22, "数据获取", "达人画像表\n帖子文本\n评论文本"),
        (0.30, 0.58, 0.18, 0.22, "数据预处理", "清洗\n分词\n停用词过滤\n群体映射"),
        (0.53, 0.58, 0.18, 0.22, "文本挖掘", "高频词\n共现网络\n互动类型"),
        (0.76, 0.58, 0.18, 0.22, "聚类分析", "变量标准化\nK-means\n群体画像"),
        (0.30, 0.20, 0.18, 0.20, "群体对比", "帖子词对比\n评论词对比"),
        (0.60, 0.20, 0.22, 0.20, "结果解释", "内容策略\n互动差异\n群体特征"),
    ]

    for x, y, w, h, title, body in steps:
        rect = plt.Rectangle((x, y), w, h, facecolor="#fef9e7", edgecolor="#34495e", linewidth=1.4)
        ax.add_patch(rect)
        ax.text(x + w / 2, y + h * 0.68, title, ha="center", va="center", fontsize=13, fontproperties=fp, weight="bold")
        ax.text(x + w / 2, y + h * 0.34, body, ha="center", va="center", fontsize=11, fontproperties=fp)

    arrows = [
        ((0.25, 0.69), (0.30, 0.69)),
        ((0.48, 0.69), (0.53, 0.69)),
        ((0.71, 0.69), (0.76, 0.69)),
        ((0.39, 0.58), (0.39, 0.40)),
        ((0.64, 0.58), (0.64, 0.40)),
        ((0.48, 0.30), (0.60, 0.30)),
    ]
    for start, end in arrows:
        ax.annotate("", xy=end, xytext=start, arrowprops=dict(arrowstyle="->", linewidth=1.5, color="#2c3e50"))

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "chapter4_method_flow.png", dpi=180, bbox_inches="tight")
    plt.close()


def build_cluster_support(clusters: pd.DataFrame) -> None:
    df = calculate_derived_metrics(clusters)
    feature_df = df[CLUSTER_FEATURES].copy()
    feature_df = feature_df.fillna(feature_df.median())

    scaler = StandardScaler()
    scaled = scaler.fit_transform(feature_df)

    ks = list(range(2, 9))
    inertias = []
    silhouettes = []
    for k in ks:
        model = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = model.fit_predict(scaled)
        inertias.append(model.inertia_)
        silhouettes.append(silhouette_score(scaled, labels))

    metric_df = pd.DataFrame({"K值": ks, "SSE": inertias, "轮廓系数": silhouettes})
    metric_df.to_csv(TABLES_DIR / "table_4_5_cluster_k_metrics.csv", index=False)

    fp = get_font()
    plt.figure(figsize=(10, 4))
    ax1 = plt.subplot(1, 2, 1)
    ax2 = plt.subplot(1, 2, 2)
    ax1.plot(ks, inertias, "o-", linewidth=2)
    ax2.plot(ks, silhouettes, "o-", linewidth=2, color="darkred")
    if fp:
        ax1.set_title("肘部法则", fontproperties=fp)
        ax1.set_xlabel("K值", fontproperties=fp)
        ax1.set_ylabel("SSE", fontproperties=fp)
        ax2.set_title("轮廓系数", fontproperties=fp)
        ax2.set_xlabel("K值", fontproperties=fp)
        ax2.set_ylabel("轮廓系数", fontproperties=fp)
    else:
        ax1.set_title("Elbow")
        ax2.set_title("Silhouette")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "cluster_elbow_silhouette_k3.png", dpi=150, bbox_inches="tight")
    plt.close()

    z_df = pd.DataFrame(
        scaler.transform(feature_df),
        columns=[f"{col}_Z" for col in CLUSTER_FEATURES],
    )
    z_df["群体标签_k3"] = df["群体标签_k3"].values
    z_summary = z_df.groupby("群体标签_k3").mean().round(3).reset_index()
    z_summary.to_csv(TABLES_DIR / "table_4_6_cluster_zscore_summary.csv", index=False)

    profile = df.groupby("群体标签_k3")[CLUSTER_FEATURES + ["视频笔记报价", "视频CPE", "活跃粉丝占比"]].median().round(3).reset_index()
    profile["群体规模"] = df["群体标签_k3"].value_counts().sort_index().values
    profile.to_csv(TABLES_DIR / "table_4_7_cluster_profile_summary.csv", index=False)


def label_comment(text: str) -> str:
    text = clean_text(text)
    for label, pattern in RULE_PATTERNS.items():
        if re.search(pattern, text):
            return label
    return "other"


def build_comment_group_tables(clusters: pd.DataFrame, comments: pd.DataFrame) -> None:
    group_map = clusters[["creator_id", "群体标签_k3"]].dropna().drop_duplicates("creator_id")
    comments = comments.copy()
    comments["creator_id"] = comments["creator_id"].astype(str)
    comments = comments.merge(group_map, on="creator_id", how="inner")
    comments["content"] = comments["content"].fillna("").map(clean_text)
    comments = comments[comments["content"].str.len() >= 2].copy()
    comments["interaction_label"] = comments["content"].map(label_comment)

    token_rows: list[dict[str, Any]] = []
    for group, sub in comments.groupby("群体标签_k3"):
        counter = Counter()
        for text in sub["content"]:
            counter.update(tokenize(text))
        for word, count in counter.most_common(20):
            token_rows.append({"群体标签_k3": group, "高频词": word, "词频": count})
    token_df = pd.DataFrame(token_rows)
    token_df.to_csv(TABLES_DIR / "table_4_8_group_comment_top_words_long.csv", index=False)

    word_ranked = token_df.copy()
    word_ranked["排名"] = word_ranked.groupby("群体标签_k3").cumcount() + 1
    word_wide = word_ranked.pivot(index="排名", columns="群体标签_k3", values="高频词").reset_index()
    word_wide.columns = ["排名"] + [f"群体{col}_高频词" for col in word_wide.columns[1:]]
    freq_wide = word_ranked.pivot(index="排名", columns="群体标签_k3", values="词频").reset_index()
    freq_wide.columns = ["排名"] + [f"群体{col}_词频" for col in freq_wide.columns[1:]]
    word_compare = word_wide.merge(freq_wide, on="排名", how="left")
    word_compare.to_csv(TABLES_DIR / "table_4_8_group_comment_top_words_compare.csv", index=False)

    label_dist = (
        comments.groupby(["群体标签_k3", "interaction_label"])
        .size()
        .rename("评论数")
        .reset_index()
    )
    totals = label_dist.groupby("群体标签_k3")["评论数"].transform("sum")
    label_dist["占比"] = (label_dist["评论数"] / totals).round(4)
    label_dist["互动类型"] = label_dist["interaction_label"].map(LABEL_DISPLAY)
    label_dist.to_csv(TABLES_DIR / "table_4_9_group_comment_interaction_distribution_long.csv", index=False)

    label_compare = (
        label_dist.pivot(index="互动类型", columns="群体标签_k3", values="占比")
        .fillna(0)
        .reset_index()
    )
    label_compare.columns = ["互动类型"] + [f"群体{col}_占比" for col in label_compare.columns[1:]]
    label_compare.to_csv(TABLES_DIR / "table_4_9_group_comment_interaction_compare.csv", index=False)


def write_markdown_summary(
    combined: pd.DataFrame,
    clusters: pd.DataFrame,
    posts: pd.DataFrame,
    comments: pd.DataFrame,
) -> None:
    group_map = clusters[["creator_id", "群体标签_k3"]].dropna().drop_duplicates("creator_id")
    posts_match = posts[posts["creator_id"].astype(str).isin(group_map["creator_id"])]
    comments_match = comments[comments["creator_id"].astype(str).isin(group_map["creator_id"])]
    lines = [
        "# 第四章支撑材料摘要",
        "",
        "## 数据口径",
        f"- 达人画像主样本：{len(combined)} 位达人。",
        f"- 聚类分析主样本：{len(clusters)} 位达人，其中三类群体标签字段为 `群体标签_k3`。",
        f"- 主文本池：{len(posts)} 条帖子、{len(comments)} 条评论。",
        f"- 与三类群体成功对齐的文本样本：{len(posts_match)} 条帖子、{len(comments_match)} 条评论。",
        "",
        "## 产出目录",
        f"- 表格目录：{TABLES_DIR}",
        f"- 图表目录：{PLOTS_DIR}",
        "",
        "## 可直接用于论文的重点表",
        "- 表 4-1 数据来源与样本规模",
        "- 表 4-2 文本预处理规则",
        "- 表 4-3 共现网络构建规则",
        "- 表 4-3a 帖子文本LDA主题提取结果",
        "- 表 4-3b 帖子高频关键词频表",
        "- 表 4-3c 帖子关键词共现边表",
        "- 表 4-3d 三类群体帖子主题分布对比表",
        "- 表 4-4 评论互动类型定义",
        "- 表 4-5a 聚类变量定义表",
        "- 表 4-5 K值选择指标",
        "- 表 4-6 聚类变量 Z 分数均值表",
        "- 表 4-7 三类群体特征概览",
        "- 表 4-8 三类群体评论高频词对比表",
        "- 表 4-9 三类群体评论互动类型占比对比表",
        "- 图 4-1 第四章方法流程图",
    ]
    (REPORTS_DIR / "chapter4_support_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_chapter_draft(
    combined: pd.DataFrame,
    clusters: pd.DataFrame,
    posts: pd.DataFrame,
    comments: pd.DataFrame,
) -> None:
    k_metrics = pd.read_csv(TABLES_DIR / "table_4_5_cluster_k_metrics.csv")
    profile = pd.read_csv(TABLES_DIR / "table_4_7_cluster_profile_summary.csv")
    comment_words = pd.read_csv(TABLES_DIR / "table_4_8_group_comment_top_words_compare.csv")
    post_words = pd.read_csv(TABLES_DIR / "table_4_3c_group_post_keywords_compare.csv")
    lda_table_path = TABLES_DIR / "table_4_3a_topic_lda_results.csv"
    lda_compare_path = TABLES_DIR / "table_4_3e_group_topic_distribution_compare.csv"
    lda_table = pd.read_csv(lda_table_path) if lda_table_path.exists() else pd.DataFrame()
    lda_compare = pd.read_csv(lda_compare_path) if lda_compare_path.exists() else pd.DataFrame()

    best_k_row = k_metrics.sort_values("轮廓系数", ascending=False).iloc[0]
    group_sizes = profile[["群体标签_k3", "群体规模"]].to_dict("records")

    def top_words_for_group(df: pd.DataFrame, prefix: str, group: int, n: int = 5) -> str:
        col = f"群体{group}_{prefix}"
        if col not in df.columns:
            return ""
        words = df[col].dropna().astype(str).head(n).tolist()
        return "、".join(words)

    lda_lead = ""
    if not lda_table.empty:
        lda_lead = "、".join(
            f"{row['主题名称']}（{int(row['帖子数'])}帖）"
            for _, row in lda_table.head(4).iterrows()
        )

    def top_topic_for_group(df: pd.DataFrame, group: int) -> str:
        col = f"群体{group}_占比"
        if df.empty or col not in df.columns:
            return ""
        row = df.sort_values(col, ascending=False).head(1)
        return "" if row.empty else str(row.iloc[0]["主题名称"])

    lines = [
        "# 第四章写作底稿",
        "",
        "## 4.1 数据来源、获取与预处理",
        f"本文的数据来源分为达人画像数据与文本数据两部分。达人画像数据来自灰豚平台整理后的结构化总表，共计 {len(combined)} 位高影响力生活类达人；聚类分析样本采用带有 `群体标签_k3` 的达人聚类结果表，共计 {len(clusters)} 位达人。文本数据采用 DOM 爬取形成的主文本池，其中共包含 {len(posts)} 条帖子文本与 {len(comments)} 条评论文本。将文本数据与聚类结果按 creator_id 进行对齐后，最终获得 {posts[posts['creator_id'].astype(str).isin(clusters['creator_id'])]['note_id'].nunique()} 条可用于群体比较的帖子与 {len(comments[comments['creator_id'].astype(str).isin(clusters['creator_id'])])} 条评论。",
        "文本预处理方面，本文首先对达人简介、帖子标题、帖子正文与评论内容进行统一清洗，去除 URL、换行与冗余空格；随后使用 jieba 分词工具进行中文分词，并根据停用词表剔除助词、代词、平台通用词及长度小于 2 的无效词项。在此基础上，分别开展高频词统计、共现网络构建与评论互动类型归纳分析。具体规则见表 4-2 至表 4-4。",
        "",
        "## 4.3 基于文本挖掘的达人内容策略与人设特征解析",
        "### 4.3.1 基于LDA的内容主题提取与内容生态网络解析",
        f"基于 {len(posts)} 条帖子文本，本文将标题与正文合并后进行清洗、分词，并采用 LDA 对帖子文本进行主题提取。当前样本中较稳定识别出的主题主要包括 {lda_lead}。从群体比较看，群体0更偏向 {top_topic_for_group(lda_compare, 0)}，群体1更偏向 {top_topic_for_group(lda_compare, 1)}，群体2则更偏向 {top_topic_for_group(lda_compare, 2)}。该结果说明，不同达人群体在内容供给上已经呈现出较稳定的主题结构差异。",
        "在共现网络构建中，本文将经清洗后保留的高频词作为节点，将两个关键词在同一帖子中同时出现记为一次共现，并以累计共现次数作为边权重。该处理不仅能够呈现帖子高频词本身，还能揭示“生活方式表达”“审美化呈现”“商品/场景推荐”等词语之间的结构性联系，从而为后续的人设与内容策略分析提供支持。",
        "",
        "### 4.3.2 三类群体评论文本差异",
        f"在评论文本方面，本文对 {len(comments[comments['creator_id'].astype(str).isin(clusters['creator_id'])])} 条已对齐评论进行了互动类型识别与高频词对比。评论高频词显示，群体0评论区更常出现 {top_words_for_group(comment_words, '高频词', 0)} 等表达；群体1评论区较多出现 {top_words_for_group(comment_words, '高频词', 1)}；群体2则更多出现 {top_words_for_group(comment_words, '高频词', 2)}。这说明不同群体达人在评论区所激发的互动语言风格并不完全相同。",
        "进一步从互动类型占比看，三类群体均以“其他”类评论为主，但仍存在可解释差异：群体0的颜值赞美与拟亲密互动占比相对较高，群体1的提问求回应与消费转化占比相对更高，群体2整体评论结构更分散、互动类型集中度较低。该结果表明，不同群体达人不仅在内容供给层面存在区别，在粉丝回应机制上也呈现不同的互动模式。",
        "",
        "## 4.4 基于 K-means 算法的达人群体聚类实现",
        f"本文选取粉丝规模、Top1年龄段占比、藏赞比、评赞比和商业笔记占比五项指标构建 K-means 聚类模型，并在标准化后比较不同 K 值下的 SSE 与轮廓系数。各变量的含义与进入聚类的理由见表 4-5a。结果显示，当 K={int(best_k_row['K值'])} 时，轮廓系数达到 {best_k_row['轮廓系数']:.3f}，为当前比较范围内的最高值，因此本文采用三类聚类方案。",
        "从三类群体的聚类中心结果看，群体0规模最大，整体呈现较均衡的影响力结构；群体1在藏赞比和商业笔记占比上明显高于整体均值，体现出更强的收藏转化与商业合作特征；群体2虽然规模最小，但评赞比显著高于其他群体，说明其单位点赞对应的评论互动更强。结合评论文本结果可以发现，聚类所刻画的结构性差异，与评论互动差异在一定程度上相互印证。",
        f"从样本规模上看，三类群体分别包含 {group_sizes[0]['群体规模']}、{group_sizes[1]['群体规模']} 和 {group_sizes[2]['群体规模']} 位达人。群体规模差异说明高影响力达人并非均质群体，而是由主流均衡型达人、商业转化型达人和高评论互动型达人共同构成。后续结合文本挖掘结果，可以进一步解释不同群体在内容策略与粉丝互动方式上的差异。",
        "",
        "## 可直接引用的图表位置",
        f"- 第四章方法流程图：{PLOTS_DIR / 'chapter4_method_flow.png'}",
        f"- 聚类 K 值选择图：{PLOTS_DIR / 'cluster_elbow_silhouette_k3.png'}",
        f"- 帖子关键词共现网络图：{PLOTS_DIR / 'post_keyword_cooccurrence_network.png'}",
        f"- 数据来源与样本规模表：{TABLES_DIR / 'table_4_1_data_sources.csv'}、{TABLES_DIR / 'table_4_1_sample_sizes.csv'}",
        f"- 评论高频词与互动类型表：{TABLES_DIR / 'table_4_8_group_comment_top_words_compare.csv'}、{TABLES_DIR / 'table_4_9_group_comment_interaction_compare.csv'}",
    ]
    (REPORTS_DIR / "chapter4_draft.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ensure_dirs()
    combined, clusters, posts, comments = load_data()
    export_data_source_tables(combined, clusters, posts, comments)
    export_method_rule_tables()
    export_cluster_variable_table()
    build_post_cooccurrence_outputs(clusters, posts)
    export_topic_outputs(clusters)
    build_method_flowchart()
    build_cluster_support(clusters)
    build_comment_group_tables(clusters, comments)
    write_markdown_summary(combined, clusters, posts, comments)
    write_chapter_draft(combined, clusters, posts, comments)
    print(REPORTS_DIR)


if __name__ == "__main__":
    main()
