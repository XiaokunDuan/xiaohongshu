#!/usr/bin/env python3
"""
Incremental text mining pipeline for Xiaohongshu snapshot analysis.

Design goals:
- freeze a stable creator snapshot from dom_click_crawl JSON files
- support resumable LLM labeling with per-batch checkpoints
- support topic modeling for post texts
- support comment interaction classification using weak labels
- avoid editing crawler outputs while the user continues crawling

Usage examples:
    python analysis/10_text_mining_pipeline.py snapshot --sample-size 500
    python analysis/10_text_mining_pipeline.py topic-model-posts
    python analysis/10_text_mining_pipeline.py llm-label-comments --sample-size 1500
    python analysis/10_text_mining_pipeline.py train-comment-classifier
    python analysis/10_text_mining_pipeline.py classify-comments
    python analysis/10_text_mining_pipeline.py summarize
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import textwrap
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from sklearn.base import clone
from sklearn.decomposition import NMF
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.multiclass import OneVsRestClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.svm import LinearSVC

try:
    import jieba
except ModuleNotFoundError:  # pragma: no cover - environment dependent
    jieba = None

try:
    import statsmodels.api as sm
except ModuleNotFoundError:  # pragma: no cover - environment dependent
    sm = None


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
DOM_CLICK_DIR = DATA_DIR / "dom_click_crawl"
CREATORS_CSV = DATA_DIR / "daren_clusters_k3.csv"

RUN_NAME = "text_mining_500"
RUN_DIR = DATA_DIR / RUN_NAME
PLOTS_DIR = REPORTS_DIR / RUN_NAME
SNAPSHOT_DIR = RUN_DIR / "snapshot"
LLM_DIR = RUN_DIR / "llm_labeling"
CLASSIFIER_DIR = RUN_DIR / "comment_classifier"
TOPIC_DIR = RUN_DIR / "topics"
UNIT_DIR = RUN_DIR / "post_units"
CREATOR_MECH_DIR = RUN_DIR / "creator_mechanism"
REGRESSION_DIR = RUN_DIR / "regressions"

AICODEE_BASE_URL = os.getenv("AICODEE_BASE_URL", "https://v2.aicodee.com/v1")
AICODEE_API_KEY = os.getenv("AICODEE_API_KEY", "")
AICODEE_MODEL = os.getenv("AICODEE_MODEL", "MiniMax-M2.7-highspeed")

DEFAULT_TOPIC_COUNT = 8
DEFAULT_LLM_BATCH_SIZE = 20
DEFAULT_LLM_SAMPLE_SIZE = 1500
DESIGN_SPLIT_RATIO = 0.6

STOPWORDS = {
    "的", "了", "是", "我", "你", "他", "她", "它", "我们", "你们", "他们", "都", "就",
    "也", "很", "在", "和", "有", "吧", "啊", "呀", "啦", "呢", "哦", "吗", "嘛", "被",
    "让", "把", "给", "着", "一个", "这个", "那个", "自己", "就是", "还是", "真的", "现在",
    "已经", "因为", "所以", "不是", "可以", "还有", "感觉", "一下", "今天", "这样", "这种",
    "那个", "然后", "就是", "一个", "没有", "不要", "就是", "真的", "起来", "看到", "觉得",
    "的话", "一下", "一下子", "时候", "出来", "小红书", "话题", "视频", "图片", "拍照",
    "宝宝", "大家", "妈妈", "喜欢", "什么", "希望", "朋友", "最近", "一起", "真的", "一下",
    "分享", "记录", "感觉", "一下", "一下子", "可以", "这个", "那个", "一种", "还有", "我们",
    "你们", "自己", "特别", "非常", "一直", "很多", "一下", "一下吧", "一下啦", "一下哦",
    "日常", "生活", "vlog", "vlog日常", "日记", "时候", "一个劲", "一下子", "有点", "有些",
    "一下呢", "一下呀", "系列", "一下啦", "已经", "还是", "其实", "比如", "因为", "所以",
    "不是", "一样", "真的很", "超级", "好用", "今天来", "来了", "来了哦", "一下哦", "一下哈",
}

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

TOPIC_NAME_RULES = [
    ("母婴与育儿消费", ["奶粉", "宝宝", "妈妈", "育儿", "母婴", "成长"]),
    ("日常治愈与独居生活", ["宅家", "治愈", "独居", "幸福", "美食", "一天", "minivlog"]),
    ("女性成长与自我提升", ["人生", "女性", "成长", "工作", "提升", "勇气", "世界"]),
    ("情侣亲情与关系日常", ["情侣", "闺蜜", "夫妻", "恋爱", "亲情", "家人"]),
    ("购物开箱与好物种草", ["购物", "开箱", "种草", "好物", "快递", "分享"]),
    ("跨文化旅行与在华外国人", ["外国人", "中国", "旅游", "老外", "美国", "地铁"]),
    ("春日氛围与拍照场景", ["春天", "樱花", "拍照", "colorwalk", "花路", "春日"]),
    ("明星营业与娱乐曝光", ["明星", "ab", "日报", "新星", "营业"]),
]

LABEL_DISPLAY_NAMES = {
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

VARIABLE_DICTIONARY = [
    ("comment_count", "单帖评论数", "该帖子在快照中的有效评论总数"),
    ("ln_comment_count", "单帖评论数对数", "对 comment_count 做 log(1+x) 变换"),
    ("appearance_praise_share", "颜值赞美占比", "评论中 appearance_praise 占全部评论的比例"),
    ("parasocial_intimacy_share", "拟亲密互动占比", "评论中 parasocial_intimacy 占全部评论的比例"),
    ("consumption_conversion_share", "消费转化占比", "评论中 consumption_conversion 占全部评论的比例"),
    ("ln_post_length", "帖子文本长度对数", "帖子标题与正文总长度做 log(1+x) 变换"),
    ("aesthetic_marker", "审美化表达标记", "帖子含氛围、拍照、出片、妆容等表达"),
    ("shopping_marker", "种草导向标记", "帖子含开箱、购物、种草、好物、推荐等表达"),
    ("narrative_marker", "叙事表达标记", "帖子含今天、后来、因为、成长等叙事表达"),
    ("has_brand_marker", "商业合作标记", "帖子含合作、广告、品牌、代言等表达"),
    ("has_question_mark", "提问式表达标记", "帖子文本中出现问号"),
    ("ln_creator_followers", "博主粉丝数对数", "博主粉丝数做 log(1+x) 变换"),
    ("avg_comments_per_post", "单帖平均评论", "博主在快照池中的平均单帖评论"),
    ("dominant_topic_share", "主导主题占比", "博主最常见帖子主题占其全部快照帖子的比例"),
    ("ln_近60天平均评论", "近60天平均评论对数", "达人总表中的近60天平均评论做 log(1+x) 变换"),
    ("ln_视频笔记报价", "视频报价对数", "达人总表中的视频笔记报价做 log(1+x) 变换"),
]

MODEL_SPECS = [
    {
        "model": "post_comment_volume",
        "level": "post",
        "sample": "validation",
        "dependent": "ln_comment_count",
        "controls": ["ln_creator_followers", "topic dummies"],
        "predictors": ["ln_post_length", "aesthetic_marker", "shopping_marker", "narrative_marker", "has_brand_marker", "has_question_mark"],
        "purpose": "检验帖子文本特征与单帖评论量的关系",
    },
    {
        "model": "post_parasocial_share",
        "level": "post",
        "sample": "validation",
        "dependent": "parasocial_intimacy_share",
        "controls": ["ln_creator_followers", "topic dummies"],
        "predictors": ["ln_post_length", "aesthetic_marker", "shopping_marker", "narrative_marker", "has_brand_marker", "has_question_mark"],
        "purpose": "检验帖子文本特征与拟亲密互动强度的关系",
    },
    {
        "model": "post_conversion_share",
        "level": "post",
        "sample": "validation",
        "dependent": "consumption_conversion_share",
        "controls": ["ln_creator_followers", "topic dummies"],
        "predictors": ["ln_post_length", "aesthetic_marker", "shopping_marker", "narrative_marker", "has_brand_marker", "has_question_mark"],
        "purpose": "检验帖子文本特征与消费转化评论强度的关系",
    },
    {
        "model": "creator_avg_comment",
        "level": "creator",
        "sample": "validation",
        "dependent": "ln_近60天平均评论",
        "controls": ["ln_粉丝数", "活跃粉丝占比"],
        "predictors": ["avg_aesthetic_marker", "avg_shopping_marker", "avg_narrative_marker", "appearance_praise_share", "parasocial_intimacy_share", "consumption_conversion_share", "dominant_topic_share"],
        "purpose": "检验博主文本机制与整体互动能力的关系",
    },
    {
        "model": "creator_video_price",
        "level": "creator",
        "sample": "validation",
        "dependent": "ln_视频笔记报价",
        "controls": ["ln_粉丝数", "活跃粉丝占比"],
        "predictors": ["avg_aesthetic_marker", "avg_shopping_marker", "avg_narrative_marker", "appearance_praise_share", "parasocial_intimacy_share", "consumption_conversion_share", "dominant_topic_share"],
        "purpose": "检验博主文本机制与商业价格的关系",
    },
]

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


def ensure_dirs() -> None:
    for path in [RUN_DIR, PLOTS_DIR, SNAPSHOT_DIR, LLM_DIR, CLASSIFIER_DIR, TOPIC_DIR, UNIT_DIR, CREATOR_MECH_DIR, REGRESSION_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def find_mac_font() -> str | None:
    for path in [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/STHeiti Light.ttc",
    ]:
        if os.path.exists(path):
            return path
    return None


def clean_text(text: Any) -> str:
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return ""
    text = str(text)
    text = text.replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\[[^\]]+R\]", " ", text)
    text = re.sub(r"#([^#]+)\[话题\]#", r" \1 ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[@＠][\w\u4e00-\u9fff\.-]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokenize_zh(text: str) -> list[str]:
    words = []
    source_words = jieba.cut(clean_text(text)) if jieba else re.findall(r"[\u4e00-\u9fffA-Za-z0-9]{2,}", clean_text(text))
    for word in source_words:
        word = word.strip().lower()
        if len(word) < 2:
            continue
        if word in STOPWORDS:
            continue
        if re.fullmatch(r"[\W_]+", word):
            continue
        words.append(word)
    return words


def text_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]


def deterministic_split(value: str, design_ratio: float = DESIGN_SPLIT_RATIO) -> str:
    bucket = int(hashlib.sha1(str(value).encode("utf-8")).hexdigest()[:8], 16) / 0xFFFFFFFF
    return "design" if bucket < design_ratio else "validation"


def assign_analysis_split(df: pd.DataFrame, id_col: str = "creator_id") -> pd.DataFrame:
    df = df.copy()
    df["analysis_split"] = df[id_col].astype(str).map(deterministic_split)
    return df


def load_creator_lookup() -> pd.DataFrame:
    creators = pd.read_csv(CREATORS_CSV)
    creators["creator_id"] = creators["达人官方地址"].astype(str).str.extract(r"profile/([a-f0-9]{24})")
    keep_cols = [
        "creator_id",
        "达人名称",
        "简介",
        "性别",
        "地域",
        "Top1关注焦点",
        "群体标签_k3",
        "粉丝数",
    ]
    return creators[keep_cols].dropna(subset=["creator_id"]).drop_duplicates("creator_id")


def iter_dom_click_records() -> Iterable[dict[str, Any]]:
    for path in sorted(DOM_CLICK_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        yield data


def build_snapshot(sample_size: int = 500) -> None:
    ensure_dirs()
    creator_lookup = load_creator_lookup()
    creator_map = creator_lookup.set_index("creator_id").to_dict("index")

    creator_rows: list[dict[str, Any]] = []
    post_rows: list[dict[str, Any]] = []
    comment_rows: list[dict[str, Any]] = []

    for data in iter_dom_click_records():
        creator_id = data.get("user_id") or ""
        notes = data.get("notes", [])
        if not creator_id or not notes:
            continue

        joined = creator_map.get(creator_id, {})
        creator_name = joined.get("达人名称") or data.get("name") or ""
        cleaned_notes = 0
        cleaned_comments = 0

        for note in notes:
            raw_post_text = clean_text(f"{note.get('title', '')} {note.get('desc', '')}")
            if not raw_post_text:
                continue

            note_id = note.get("note_id", "")
            post_rows.append(
                {
                    "creator_id": creator_id,
                    "creator_name": creator_name,
                    "note_id": note_id,
                    "open_url": note.get("open_url", ""),
                    "title": clean_text(note.get("title", "")),
                    "desc": clean_text(note.get("desc", "")),
                    "post_text": raw_post_text,
                    "likes": note.get("likes", ""),
                    "crawl_status": note.get("crawl_status", ""),
                    "comment_count_visible": note.get("comment_count_visible", 0),
                }
            )
            cleaned_notes += 1

            for idx, comment in enumerate(note.get("comments", []), 1):
                comment_text = clean_text(comment.get("content", ""))
                if not comment_text:
                    continue
                comment_rows.append(
                    {
                        "comment_id": f"{note_id}_{idx}",
                        "creator_id": creator_id,
                        "creator_name": creator_name,
                        "note_id": note_id,
                        "content": comment_text,
                        "likes": comment.get("likes", ""),
                        "comment_index": idx,
                    }
                )
                cleaned_comments += 1

        creator_rows.append(
            {
                "creator_id": creator_id,
                "creator_name": creator_name,
                "bio": clean_text(joined.get("简介", "")),
                "gender": joined.get("性别", ""),
                "region": joined.get("地域", ""),
                "top_interest": joined.get("Top1关注焦点", ""),
                "cluster_k3": joined.get("群体标签_k3", ""),
                "followers": joined.get("粉丝数", ""),
                "note_count": cleaned_notes,
                "comment_count": cleaned_comments,
                "profile_title": clean_text(data.get("profile_title", "")),
            }
        )

    creators_df = pd.DataFrame(creator_rows).drop_duplicates("creator_id")
    creators_df = creators_df.sort_values(
        ["comment_count", "note_count", "followers"],
        ascending=[False, False, False],
    )
    sample_df = creators_df.head(sample_size).copy()
    sample_ids = set(sample_df["creator_id"])

    posts_df = pd.DataFrame(post_rows)
    comments_df = pd.DataFrame(comment_rows)
    posts_df = posts_df[posts_df["creator_id"].isin(sample_ids)].copy()
    comments_df = comments_df[comments_df["creator_id"].isin(sample_ids)].copy()

    sample_df.to_csv(SNAPSHOT_DIR / "creator_snapshot_500.csv", index=False)
    posts_df.to_csv(SNAPSHOT_DIR / "posts_snapshot_500.csv", index=False)
    comments_df.to_csv(SNAPSHOT_DIR / "comments_snapshot_500.csv", index=False)

    manifest = {
        "sample_size_requested": sample_size,
        "sample_size_actual": int(len(sample_df)),
        "posts": int(len(posts_df)),
        "comments": int(len(comments_df)),
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source": str(DOM_CLICK_DIR),
        "selection_rule": "top creators by comment_count, note_count, followers",
    }
    (SNAPSHOT_DIR / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


def load_snapshot() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    creators = pd.read_csv(SNAPSHOT_DIR / "creator_snapshot_500.csv")
    posts = pd.read_csv(SNAPSHOT_DIR / "posts_snapshot_500.csv")
    comments = pd.read_csv(SNAPSHOT_DIR / "comments_snapshot_500.csv")
    return creators, posts, comments


def run_topic_model_on_posts(topic_count: int = DEFAULT_TOPIC_COUNT) -> None:
    ensure_dirs()
    _, posts, _ = load_snapshot()
    posts = posts.copy()
    posts["post_text"] = posts["post_text"].fillna("").map(clean_text)
    posts = posts[posts["post_text"].str.len() >= 12].copy()
    if posts.empty:
        raise RuntimeError("No post texts available in snapshot.")

    vectorizer = TfidfVectorizer(
        tokenizer=tokenize_zh,
        token_pattern=None,
        min_df=3,
        max_df=0.85,
        ngram_range=(1, 2),
        max_features=6000,
    )
    matrix = vectorizer.fit_transform(posts["post_text"])
    model = NMF(n_components=min(topic_count, max(2, matrix.shape[0] - 1)), random_state=42)
    topic_weights = model.fit_transform(matrix)
    feature_names = vectorizer.get_feature_names_out()
    posts["topic_id"] = topic_weights.argmax(axis=1)
    posts["topic_score"] = topic_weights.max(axis=1)

    topic_rows: list[dict[str, Any]] = []
    for topic_id, component in enumerate(model.components_):
        top_indices = component.argsort()[::-1][:12]
        keywords = [feature_names[i] for i in top_indices]
        topic_posts = posts[posts["topic_id"] == topic_id].copy()
        topic_posts = topic_posts.sort_values("topic_score", ascending=False)
        representative = topic_posts.head(3)[["creator_name", "post_text", "note_id"]].to_dict("records")
        topic_name = infer_topic_name(keywords, representative)
        topic_rows.append(
            {
                "topic_id": topic_id,
                "topic_name": topic_name,
                "post_count": int(len(topic_posts)),
                "keywords": " | ".join(keywords),
                "representative_posts": json.dumps(representative, ensure_ascii=False),
            }
        )

    topic_df = pd.DataFrame(topic_rows).sort_values("post_count", ascending=False)
    topic_df.to_csv(TOPIC_DIR / "post_topics.csv", index=False)
    posts.to_csv(TOPIC_DIR / "posts_with_topics.csv", index=False)

    font_path = find_mac_font()
    plt.figure(figsize=(10, 6))
    plt.bar(topic_df["topic_id"].astype(str), topic_df["post_count"])
    plt.title("Post Topic Distribution", fontname=None)
    if font_path:
        plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC"]
    plt.xlabel("Topic ID")
    plt.ylabel("Posts")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "post_topic_distribution.png", dpi=150, bbox_inches="tight")
    plt.close()

    print(topic_df.head(10).to_string(index=False))


def sample_comments_for_labeling(sample_size: int = DEFAULT_LLM_SAMPLE_SIZE) -> pd.DataFrame:
    _, _, comments = load_snapshot()
    comments = comments.copy()
    comments["content"] = comments["content"].fillna("").map(clean_text)
    comments = comments[comments["content"].str.len().between(8, 260)].copy()
    if comments.empty:
        raise RuntimeError("No comments available in snapshot.")

    comments["rule_hits"] = comments["content"].map(rule_based_comment_labels).map(len)
    comments["sample_weight"] = 1 + comments["rule_hits"]
    sample_size = min(sample_size, len(comments))
    sampled = comments.sample(n=sample_size, weights="sample_weight", random_state=42)
    sampled["sample_id"] = sampled["comment_id"].map(lambda x: text_hash(str(x)))
    sampled = sampled.drop_duplicates("sample_id").copy()
    sampled.to_csv(LLM_DIR / "comment_label_sample.csv", index=False)
    return sampled


def llm_prompt_for_comment_batch(rows: list[dict[str, str]]) -> str:
    schema = {
        "sample_id": "copy from input",
        "primary_label": "one of COMMENT_LABELS",
        "secondary_labels": ["zero or more labels from COMMENT_LABELS except primary repetition"],
        "appearance_focus": 0,
        "parasocial_strength": 0,
        "commercial_intent": 0,
        "valence": "positive|negative|mixed|neutral",
        "confidence": 0.0,
        "reason_short": "short chinese phrase",
    }
    return textwrap.dedent(
        f"""
        你是中文社交媒体评论标注器。请严格输出 JSON 数组，不要输出任何额外文本，不要使用 markdown 代码块。

        标签定义：
        - appearance_praise: 颜值、妆容、滤镜、发型、漂亮、可爱、帅等外貌评价
        - parasocial_intimacy: 宝宝/老婆/闺蜜/亲亲/想你/梦到你等拟亲密表达
        - care_support: 吃饭、休息、身体、开心、加油、陪伴、安慰
        - fan_mobilization: 应援、做数据、冲销量、控评、反黑、维权、晒单
        - consumption_conversion: 买同款、求链接、抢周边、代言支持、购买行为
        - self_disclosure: 评论者自我汇报学习、考试、上学、生活近况
        - advice_regulation: 对博主提出职业/妆造/滤镜/直播/进组等建议或规训
        - question_request: 提问、求回复、求互动、求祝福
        - other: 不属于以上

        额外字段规则：
        - appearance_focus: 是否明显聚焦外貌/妆造/发型/滤镜，0 或 1
        - parasocial_strength: 拟亲密强度，0-3
        - commercial_intent: 是否包含购买、代言支持、同款、周边等商业意图，0 或 1
        - confidence: 0 到 1 的小数

        输出要求：
        - 输出一个 JSON 数组
        - 数组长度必须与输入样本数完全一致
        - 每个对象都必须带回原始 sample_id
        - 如果不确定，也必须给出最合理的标签，不要留空

        输出 schema 示例：
        {json.dumps(schema, ensure_ascii=False)}

        待标注评论样本：
        {json.dumps(rows, ensure_ascii=False)}
        """
    ).strip()


def call_aicodee_chat(prompt: str, max_retries: int = 3) -> dict[str, Any]:
    if not AICODEE_API_KEY:
        raise RuntimeError("AICODEE_API_KEY is not set in environment.")

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                f"{AICODEE_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {AICODEE_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": AICODEE_MODEL,
                    "temperature": 0.1,
                    "messages": [
                        {"role": "system", "content": "You are a strict JSON labeling assistant."},
                        {"role": "user", "content": prompt},
                    ],
                },
                timeout=180,
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # pragma: no cover - runtime path
            last_error = exc
            time.sleep(min(2 * attempt, 8))
    raise RuntimeError(f"LLM call failed after {max_retries} retries: {last_error}")


def normalize_parsed_label(parsed: dict[str, Any]) -> dict[str, Any]:
    parsed["secondary_labels"] = [
        label for label in parsed.get("secondary_labels", []) if label in COMMENT_LABELS
    ]
    if parsed.get("primary_label") not in COMMENT_LABELS:
        parsed["primary_label"] = "other"
    parsed["appearance_focus"] = int(parsed.get("appearance_focus", 0))
    parsed["parasocial_strength"] = int(parsed.get("parasocial_strength", 0))
    parsed["commercial_intent"] = int(parsed.get("commercial_intent", 0))
    parsed["confidence"] = float(parsed.get("confidence", 0.0))
    parsed["valence"] = str(parsed.get("valence", "neutral"))
    parsed["reason_short"] = str(parsed.get("reason_short", "")).strip()
    return parsed


def extract_json_objects(raw_text: str) -> list[dict[str, Any]]:
    raw_text = raw_text.strip()
    array_match = re.search(r"\[.*\]", raw_text, flags=re.S)
    object_match = re.search(r"\{.*\}", raw_text, flags=re.S)

    if array_match:
        parsed = json.loads(array_match.group(0))
        if not isinstance(parsed, list):
            raise ValueError("Model output is not a JSON array.")
        return [normalize_parsed_label(item) for item in parsed]

    if object_match:
        parsed = json.loads(object_match.group(0))
        if isinstance(parsed, dict):
            return [normalize_parsed_label(parsed)]

    raise ValueError("No JSON payload found in model output.")


def load_existing_llm_results() -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for path in sorted(LLM_DIR.glob("batch_*.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                if row.get("status") == "ok":
                    results[row["sample_id"]] = row
    return results


def llm_label_comments(sample_size: int = DEFAULT_LLM_SAMPLE_SIZE, batch_size: int = DEFAULT_LLM_BATCH_SIZE) -> None:
    ensure_dirs()
    sample_df = sample_comments_for_labeling(sample_size=sample_size)
    existing = load_existing_llm_results()
    pending = sample_df[~sample_df["sample_id"].isin(existing)].copy()
    if pending.empty:
        print("All sampled comments already labeled.")
        return

    batch_index = len(list(LLM_DIR.glob("batch_*.jsonl"))) + 1
    current_path = LLM_DIR / f"batch_{batch_index:04d}.jsonl"

    with current_path.open("a", encoding="utf-8") as handle:
        for start in range(0, len(pending), batch_size):
            batch = pending.iloc[start:start + batch_size]
            batch_rows = [
                {"sample_id": row["sample_id"], "content": row["content"]}
                for _, row in batch.iterrows()
            ]
            row_lookup = {
                row["sample_id"]: {
                    "comment_id": row["comment_id"],
                    "content": row["content"],
                }
                for _, row in batch.iterrows()
            }
            try:
                prompt = llm_prompt_for_comment_batch(batch_rows)
                payload = call_aicodee_chat(prompt)
                raw_response = payload["choices"][0]["message"]["content"]
                parsed_items = extract_json_objects(raw_response)
                parsed_by_id = {
                    item.get("sample_id"): item for item in parsed_items if item.get("sample_id")
                }
                for sample_id, lookup in row_lookup.items():
                    item = parsed_by_id.get(sample_id)
                    if item is None:
                        result_row = {
                            "sample_id": sample_id,
                            "comment_id": lookup["comment_id"],
                            "content": lookup["content"],
                            "status": "error",
                            "model": AICODEE_MODEL,
                            "raw_response": raw_response,
                            "parsed": {},
                            "error": "sample_id missing in batch response",
                        }
                    else:
                        result_row = {
                            "sample_id": sample_id,
                            "comment_id": lookup["comment_id"],
                            "content": lookup["content"],
                            "status": "ok",
                            "model": AICODEE_MODEL,
                            "raw_response": raw_response,
                            "parsed": item,
                        }
                    handle.write(json.dumps(result_row, ensure_ascii=False) + "\n")
                    handle.flush()
            except Exception as exc:  # pragma: no cover - runtime path
                for sample_id, lookup in row_lookup.items():
                    result_row = {
                        "sample_id": sample_id,
                        "comment_id": lookup["comment_id"],
                        "content": lookup["content"],
                        "status": "error",
                        "model": AICODEE_MODEL,
                        "raw_response": "",
                        "parsed": {},
                        "error": str(exc),
                    }
                    handle.write(json.dumps(result_row, ensure_ascii=False) + "\n")
                    handle.flush()
            print(f"Labeled {min(start + batch_size, len(pending))}/{len(pending)} pending comments", flush=True)

    export_llm_labels()


def export_llm_labels() -> None:
    rows = []
    for path in sorted(LLM_DIR.glob("batch_*.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                if row.get("status") != "ok":
                    continue
                parsed = row["parsed"]
                rows.append(
                    {
                        "sample_id": row["sample_id"],
                        "comment_id": row["comment_id"],
                        "content": row["content"],
                        "primary_label": parsed.get("primary_label", "other"),
                        "secondary_labels": json.dumps(parsed.get("secondary_labels", []), ensure_ascii=False),
                        "appearance_focus": parsed.get("appearance_focus", 0),
                        "parasocial_strength": parsed.get("parasocial_strength", 0),
                        "commercial_intent": parsed.get("commercial_intent", 0),
                        "valence": parsed.get("valence", "neutral"),
                        "confidence": parsed.get("confidence", 0.0),
                        "reason_short": parsed.get("reason_short", ""),
                    }
                )
    if rows:
        pd.DataFrame(rows).drop_duplicates("sample_id").to_csv(
            LLM_DIR / "labeled_comments.csv",
            index=False,
        )


def parse_secondary_labels(value: Any) -> list[str]:
    if isinstance(value, list):
        return [label for label in value if label in COMMENT_LABELS]
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return [label for label in parsed if label in COMMENT_LABELS]


def rule_based_comment_labels(text: str) -> list[str]:
    labels = [label for label, pattern in RULE_PATTERNS.items() if re.search(pattern, text)]
    return labels or ["other"]


def build_training_frame() -> pd.DataFrame:
    labeled_path = LLM_DIR / "labeled_comments.csv"
    frames = []
    if labeled_path.exists():
        df = pd.read_csv(labeled_path)
        df["secondary_labels"] = df["secondary_labels"].map(parse_secondary_labels)
        df["labels"] = df.apply(
            lambda row: sorted(set([row["primary_label"], *row["secondary_labels"]])) or ["other"],
            axis=1,
        )
        df["label_source"] = "llm"
        frames.append(df[["sample_id", "comment_id", "content", "labels", "label_source"]])

    sample_path = LLM_DIR / "comment_label_sample.csv"
    if sample_path.exists():
        sample_df = pd.read_csv(sample_path)
        if not frames:
            seen_ids: set[str] = set()
        else:
            seen_ids = set(frames[0]["sample_id"])
        pseudo = sample_df[~sample_df["sample_id"].isin(seen_ids)].copy()
        if not pseudo.empty:
            pseudo["labels"] = pseudo["content"].fillna("").map(rule_based_comment_labels)
            pseudo["label_source"] = "rules"
            frames.append(pseudo[["sample_id", "comment_id", "content", "labels", "label_source"]])

    if not frames:
        raise RuntimeError("Run llm-label-comments first.")
    return pd.concat(frames, ignore_index=True)


def train_comment_classifier() -> None:
    ensure_dirs()
    df = build_training_frame()
    df = df[df["content"].fillna("").str.len() >= 6].copy()
    if len(df) < 40:
        raise RuntimeError("Not enough labeled comments to train classifier.")

    mlb = MultiLabelBinarizer(classes=COMMENT_LABELS)
    y = mlb.fit_transform(df["labels"])
    X_train, X_test, y_train, y_test = train_test_split(
        df["content"],
        y,
        test_size=0.2,
        random_state=42,
    )

    pipeline = Pipeline(
        steps=[
            (
                "tfidf",
                TfidfVectorizer(
                    tokenizer=tokenize_zh,
                    token_pattern=None,
                    min_df=2,
                    max_df=0.9,
                    ngram_range=(1, 2),
                    max_features=10000,
                ),
            ),
            ("clf", OneVsRestClassifier(LinearSVC())),
        ]
    )
    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)

    report = classification_report(
        y_test,
        y_pred,
        target_names=mlb.classes_,
        zero_division=0,
        output_dict=True,
    )
    (CLASSIFIER_DIR / "classification_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    import joblib

    joblib.dump(pipeline, CLASSIFIER_DIR / "comment_classifier.joblib")
    joblib.dump(mlb, CLASSIFIER_DIR / "label_binarizer.joblib")
    print(json.dumps(report.get("micro avg", {}), ensure_ascii=False, indent=2))


def classify_comments() -> None:
    ensure_dirs()
    import joblib

    pipeline = joblib.load(CLASSIFIER_DIR / "comment_classifier.joblib")
    mlb: MultiLabelBinarizer = joblib.load(CLASSIFIER_DIR / "label_binarizer.joblib")
    _, _, comments = load_snapshot()
    comments = comments.copy()
    comments["content"] = comments["content"].fillna("").map(clean_text)
    comments = comments[comments["content"].str.len() >= 6].copy()

    predictions = pipeline.predict(comments["content"])
    labels = [list(mlb.inverse_transform(predictions[i:i + 1])[0]) for i in range(len(comments))]
    comments["predicted_labels"] = [json.dumps(label_list, ensure_ascii=False) for label_list in labels]
    comments["primary_predicted_label"] = [label_list[0] if label_list else "other" for label_list in labels]
    comments["appearance_focus_rule"] = comments["content"].str.contains(RULE_PATTERNS["appearance_praise"], regex=True)
    comments["parasocial_rule"] = comments["content"].str.contains(RULE_PATTERNS["parasocial_intimacy"], regex=True)
    comments["commercial_rule"] = comments["content"].str.contains(RULE_PATTERNS["consumption_conversion"], regex=True)
    comments.to_csv(CLASSIFIER_DIR / "comments_with_predictions.csv", index=False)

    label_counter = Counter(comments["primary_predicted_label"])
    summary_df = pd.DataFrame(
        [{"label": label, "count": count} for label, count in label_counter.most_common()]
    )
    summary_df.to_csv(CLASSIFIER_DIR / "prediction_summary.csv", index=False)

    font_path = find_mac_font()
    plt.figure(figsize=(10, 6))
    plt.bar(summary_df["label"], summary_df["count"])
    plt.xticks(rotation=30, ha="right")
    if font_path:
        plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC"]
    plt.title("Comment Interaction Label Distribution")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "comment_label_distribution.png", dpi=150, bbox_inches="tight")
    plt.close()
    build_topic_label_linkage()
    print(summary_df.to_string(index=False))


def build_topic_label_linkage() -> None:
    topic_path = TOPIC_DIR / "posts_with_topics.csv"
    pred_path = CLASSIFIER_DIR / "comments_with_predictions.csv"
    if not topic_path.exists() or not pred_path.exists():
        return

    posts = pd.read_csv(topic_path, usecols=["note_id", "topic_id"])
    comments = pd.read_csv(pred_path, usecols=["note_id", "primary_predicted_label"])
    merged = comments.merge(posts, on="note_id", how="inner")
    if merged.empty:
        return

    linkage = (
        merged.groupby(["topic_id", "primary_predicted_label"])
        .size()
        .rename("count")
        .reset_index()
    )
    linkage.to_csv(CLASSIFIER_DIR / "topic_comment_label_linkage.csv", index=False)

    pivot = (
        linkage.pivot(index="topic_id", columns="primary_predicted_label", values="count")
        .fillna(0)
        .astype(int)
    )
    pivot = pivot[[c for c in COMMENT_LABELS if c in pivot.columns] + [c for c in pivot.columns if c not in COMMENT_LABELS]]
    pivot.to_csv(CLASSIFIER_DIR / "topic_comment_label_heatmap.csv")

    if pivot.empty:
        return

    fig_w = max(8, 0.9 * len(pivot.columns))
    fig_h = max(5, 0.8 * len(pivot.index))
    plt.figure(figsize=(fig_w, fig_h))
    plt.imshow(pivot.values, aspect="auto", cmap="YlOrRd")
    plt.colorbar(label="Comments")
    plt.xticks(range(len(pivot.columns)), pivot.columns, rotation=35, ha="right")
    plt.yticks(range(len(pivot.index)), pivot.index.astype(str))
    plt.xlabel("Comment Label")
    plt.ylabel("Post Topic ID")
    plt.title("Post Topics x Comment Interaction Labels")
    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            value = pivot.iloc[i, j]
            if value > 0:
                plt.text(j, i, str(value), ha="center", va="center", fontsize=8, color="black")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "topic_comment_label_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close()


def infer_topic_name(keywords: list[str], representative_posts: list[dict[str, Any]]) -> str:
    haystack = " ".join(keywords) + " " + " ".join(post.get("post_text", "") for post in representative_posts)
    for topic_name, clues in TOPIC_NAME_RULES:
        if any(clue in haystack for clue in clues):
            return topic_name
    return "综合生活表达"


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return default


def log1p_series(series: pd.Series) -> pd.Series:
    return np.log1p(series.fillna(0).clip(lower=0))


def share_column(label: str) -> str:
    return f"{label}_share"


def count_column(label: str) -> str:
    return f"{label}_count"


def add_post_text_features(posts: pd.DataFrame) -> pd.DataFrame:
    posts = posts.copy()
    posts["post_text"] = posts["post_text"].fillna("").map(clean_text)
    posts["post_length"] = posts["post_text"].str.len()
    posts["has_question_mark"] = posts["post_text"].str.contains(r"\?|？", regex=True).astype(int)
    posts["has_brand_marker"] = posts["post_text"].str.contains(r"合作|广告|品牌|链接|福利|代言|种草|开箱|好物", regex=True).astype(int)
    posts["aesthetic_marker"] = posts["post_text"].str.contains(r"氛围|拍照|出片|妆容|春天|樱花|美美|漂亮|可爱|温柔", regex=True).astype(int)
    posts["narrative_marker"] = posts["post_text"].str.contains(r"今天|一次|后来|因为|所以|觉得|如果|我们|自己|人生|成长", regex=True).astype(int)
    posts["shopping_marker"] = posts["post_text"].str.contains(r"开箱|购物|种草|好物|购买|入手|推荐|测评", regex=True).astype(int)
    return posts


def build_post_comment_unit_table() -> Path:
    ensure_dirs()
    topic_path = TOPIC_DIR / "posts_with_topics.csv"
    topic_meta_path = TOPIC_DIR / "post_topics.csv"
    pred_path = CLASSIFIER_DIR / "comments_with_predictions.csv"
    if not topic_path.exists() or not pred_path.exists() or not topic_meta_path.exists():
        raise RuntimeError("Run topic-model-posts and classify-comments before building post units.")

    creators, _, _ = load_snapshot()
    posts = pd.read_csv(topic_path)
    topic_meta = pd.read_csv(topic_meta_path, usecols=["topic_id", "topic_name"])
    comments = pd.read_csv(pred_path)
    posts = posts.merge(topic_meta, on="topic_id", how="left")
    posts = assign_analysis_split(posts, "creator_id")
    posts = add_post_text_features(posts)
    comments["content"] = comments["content"].fillna("").map(clean_text)

    grouped = comments.groupby("note_id")
    base = grouped.agg(
        comment_count=("comment_id", "count"),
        comment_text_total_length=("content", lambda s: int(s.str.len().sum())),
    ).reset_index()

    primary_counts = (
        comments.groupby(["note_id", "primary_predicted_label"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    rename_map = {
        label: count_column(label)
        for label in primary_counts.columns
        if label != "note_id"
    }
    primary_counts = primary_counts.rename(columns=rename_map)

    rules = (
        comments.groupby("note_id")
        .agg(
            appearance_rule_hits=("appearance_focus_rule", "sum"),
            parasocial_rule_hits=("parasocial_rule", "sum"),
            commercial_rule_hits=("commercial_rule", "sum"),
        )
        .reset_index()
    )

    top_comments = (
        comments.sort_values(["note_id", "primary_predicted_label", "likes"], ascending=[True, True, False])
        .groupby("note_id")
        .head(3)
        .groupby("note_id")["content"]
        .apply(lambda s: " || ".join(s.head(3).tolist()))
        .reset_index(name="representative_comments")
    )

    post_units = posts.merge(base, on="note_id", how="left")
    post_units = post_units.merge(primary_counts, on="note_id", how="left")
    post_units = post_units.merge(rules, on="note_id", how="left")
    post_units = post_units.merge(top_comments, on="note_id", how="left")
    post_units = post_units.merge(
        creators[["creator_id", "cluster_k3", "top_interest", "followers", "gender", "region"]],
        on="creator_id",
        how="left",
    )

    post_units["comment_count"] = post_units["comment_count"].fillna(0).astype(int)
    post_units["comment_text_total_length"] = post_units["comment_text_total_length"].fillna(0).astype(int)
    for col in ["appearance_rule_hits", "parasocial_rule_hits", "commercial_rule_hits"]:
        post_units[col] = post_units[col].fillna(0).astype(int)

    for label in COMMENT_LABELS:
        col = count_column(label)
        if col not in post_units.columns:
            post_units[col] = 0
        post_units[col] = post_units[col].fillna(0).astype(int)
        post_units[share_column(label)] = np.where(
            post_units["comment_count"] > 0,
            post_units[col] / post_units["comment_count"],
            0.0,
        )

    ordered_counts = [count_column(label) for label in COMMENT_LABELS]
    dominant_idx = post_units[ordered_counts].values.argmax(axis=1)
    post_units["dominant_comment_label"] = [COMMENT_LABELS[i] for i in dominant_idx]
    post_units["dominant_comment_label_name"] = post_units["dominant_comment_label"].map(LABEL_DISPLAY_NAMES)
    post_units["ln_comment_count"] = log1p_series(post_units["comment_count"])
    post_units["ln_post_length"] = log1p_series(post_units["post_length"])
    post_units["creator_followers"] = post_units["followers"].map(safe_float)
    post_units["ln_creator_followers"] = log1p_series(post_units["creator_followers"])

    output_path = UNIT_DIR / "post_comment_unit_table.csv"
    post_units.to_csv(output_path, index=False)

    summary = (
        post_units.groupby(["topic_id", "topic_name"])
        .agg(
            posts=("note_id", "count"),
            avg_comments=("comment_count", "mean"),
            avg_parasocial_share=(share_column("parasocial_intimacy"), "mean"),
            avg_appearance_share=(share_column("appearance_praise"), "mean"),
            avg_conversion_share=(share_column("consumption_conversion"), "mean"),
        )
        .reset_index()
        .sort_values("posts", ascending=False)
    )
    summary.to_csv(UNIT_DIR / "post_unit_topic_summary.csv", index=False)
    split_summary = (
        post_units.groupby("analysis_split")
        .agg(
            posts=("note_id", "count"),
            creators=("creator_id", "nunique"),
            comments=("comment_count", "sum"),
        )
        .reset_index()
    )
    split_summary.to_csv(UNIT_DIR / "post_unit_split_summary.csv", index=False)
    return output_path


def load_structured_creator_base() -> pd.DataFrame:
    base_path = BASE_DIR / "analysis" / "combined_cleaned_final.csv"
    df = pd.read_csv(base_path)
    df["creator_id"] = df["达人官方地址"].astype(str).str.extract(r"profile/([a-f0-9]{24})")
    df = df.dropna(subset=["creator_id"]).copy()
    numeric_cols = [
        "粉丝数",
        "赞藏总数",
        "图文笔记报价",
        "视频笔记报价",
        "笔记总数",
        "商业笔记总数",
        "近60天平均点赞",
        "近60天平均收藏",
        "近60天平均评论",
        "近60天平均分享",
        "活跃粉丝占比",
        "水粉占比",
        "Top1年龄段占比",
        "Top1关注焦点占比",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def build_creator_mechanism_table() -> Path:
    ensure_dirs()
    unit_path = UNIT_DIR / "post_comment_unit_table.csv"
    if not unit_path.exists():
        raise RuntimeError("Run build-post-units first.")

    post_units = pd.read_csv(unit_path)
    structured = load_structured_creator_base()
    post_units = assign_analysis_split(post_units, "creator_id")

    agg_spec: dict[str, tuple[str, str]] = {
        "post_count_snapshot": ("note_id", "count"),
        "total_comments_snapshot": ("comment_count", "sum"),
        "avg_comments_per_post": ("comment_count", "mean"),
        "median_comments_per_post": ("comment_count", "median"),
        "avg_post_length": ("post_length", "mean"),
        "avg_post_topic_score": ("topic_score", "mean"),
        "avg_aesthetic_marker": ("aesthetic_marker", "mean"),
        "avg_narrative_marker": ("narrative_marker", "mean"),
        "avg_shopping_marker": ("shopping_marker", "mean"),
        "avg_has_brand_marker": ("has_brand_marker", "mean"),
        "avg_has_question_mark": ("has_question_mark", "mean"),
    }
    for label in COMMENT_LABELS:
        agg_spec[count_column(label)] = (count_column(label), "sum")
        agg_spec[share_column(label)] = (share_column(label), "mean")

    creator_units = (
        post_units.groupby(["creator_id", "creator_name", "cluster_k3", "top_interest"], dropna=False)
        .agg(**agg_spec)
        .reset_index()
    )
    creator_units["ln_total_comments_snapshot"] = log1p_series(creator_units["total_comments_snapshot"])
    creator_units["ln_avg_comments_per_post"] = log1p_series(creator_units["avg_comments_per_post"])
    creator_units = assign_analysis_split(creator_units, "creator_id")

    topic_mix = (
        post_units.groupby(["creator_id", "topic_name"])
        .size()
        .rename("topic_posts")
        .reset_index()
    )
    topic_totals = topic_mix.groupby("creator_id")["topic_posts"].transform("sum")
    topic_mix["topic_share"] = topic_mix["topic_posts"] / topic_totals
    dominant_topics = topic_mix.sort_values(["creator_id", "topic_share"], ascending=[True, False]).drop_duplicates("creator_id")
    dominant_topics = dominant_topics.rename(columns={"topic_name": "dominant_topic_name", "topic_share": "dominant_topic_share"})
    creator_units = creator_units.merge(dominant_topics[["creator_id", "dominant_topic_name", "dominant_topic_share"]], on="creator_id", how="left")

    merged = creator_units.merge(structured, on="creator_id", how="left")
    output_path = CREATOR_MECH_DIR / "creator_mechanism_table.csv"
    merged.to_csv(output_path, index=False)
    split_summary = (
        merged.groupby("analysis_split")
        .agg(
            creators=("creator_id", "count"),
            avg_snapshot_posts=("post_count_snapshot", "mean"),
            avg_snapshot_comments=("avg_comments_per_post", "mean"),
        )
        .reset_index()
    )
    split_summary.to_csv(CREATOR_MECH_DIR / "creator_split_summary.csv", index=False)
    return output_path


def regression_table(model: Any, title: str) -> pd.DataFrame:
    if sm is not None and hasattr(model, "params") and hasattr(model, "bse"):
        conf = model.conf_int()
        return pd.DataFrame(
            {
                "term": model.params.index,
                "coef": model.params.values,
                "std_err": model.bse.values,
                "t": model.tvalues.values,
                "p_value": model.pvalues.values,
                "ci_low": conf[0].values,
                "ci_high": conf[1].values,
                "model": title,
                "backend": "statsmodels",
            }
        )

    terms = ["const", *model["x_cols"]]
    coefs = [model["intercept"], *model["coef"]]
    return pd.DataFrame(
        {
            "term": terms,
            "coef": coefs,
            "std_err": np.nan,
            "t": np.nan,
            "p_value": np.nan,
            "ci_low": np.nan,
            "ci_high": np.nan,
            "model": title,
            "backend": "sklearn",
        }
    )


def fit_ols(df: pd.DataFrame, y_col: str, x_cols: list[str]) -> Any:
    frame = df[[y_col, *x_cols]].copy().replace([np.inf, -np.inf], np.nan).dropna()
    if len(frame) < max(30, len(x_cols) * 5):
        raise RuntimeError(f"Not enough usable rows for regression: {y_col}")
    y = pd.to_numeric(frame[y_col], errors="coerce")
    X = frame[x_cols].apply(pd.to_numeric, errors="coerce")
    valid = y.notna() & X.notna().all(axis=1)
    y = y[valid].astype(float)
    X = X.loc[valid].astype(float)
    if len(X) < max(30, len(x_cols) * 5):
        raise RuntimeError(f"Not enough numeric rows for regression: {y_col}")
    if sm is not None:
        X_sm = sm.add_constant(X, has_constant="add")
        return sm.OLS(y, X_sm).fit(cov_type="HC3")
    model = LinearRegression()
    model.fit(X, y)
    pred = model.predict(X)
    ss_res = float(np.sum((y - pred) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r_squared = 1 - ss_res / ss_tot if ss_tot else 0.0
    return {
        "intercept": float(model.intercept_),
        "coef": [float(v) for v in model.coef_],
        "x_cols": x_cols,
        "nobs": len(frame),
        "r_squared": r_squared,
        "adj_r_squared": np.nan,
    }


def export_protocol_artifacts() -> None:
    ensure_dirs()
    pd.DataFrame(VARIABLE_DICTIONARY, columns=["variable", "label_cn", "definition"]).to_csv(
        REGRESSION_DIR / "variable_dictionary.csv",
        index=False,
    )
    protocol = {
        "frozen_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "design_split_ratio": DESIGN_SPLIT_RATIO,
        "main_estimation_sample": "validation",
        "principles": [
            "do not add or remove models based on significance",
            "freeze variable definitions before main estimation",
            "report weak and null results together with strong results",
            "treat full-sample models as exploratory only",
        ],
        "model_specs": MODEL_SPECS,
    }
    (REGRESSION_DIR / "analysis_protocol.json").write_text(
        json.dumps(protocol, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def classify_effect_strength(table: pd.DataFrame) -> pd.DataFrame:
    table = table.copy()
    if "p_value" not in table.columns:
        table["effect_strength"] = "descriptive_only"
        return table
    def _strength(row: pd.Series) -> str:
        term = str(row["term"])
        if term == "const":
            return "intercept"
        p = row["p_value"]
        if pd.isna(p):
            return "descriptive_only"
        if p < 0.01:
            return "strong"
        if p < 0.05:
            return "moderate"
        if p < 0.10:
            return "weak"
        return "not_significant"
    table["effect_strength"] = table.apply(_strength, axis=1)
    return table


def run_mechanism_regressions() -> Path:
    ensure_dirs()
    export_protocol_artifacts()
    post_path = UNIT_DIR / "post_comment_unit_table.csv"
    creator_path = CREATOR_MECH_DIR / "creator_mechanism_table.csv"
    if not post_path.exists() or not creator_path.exists():
        raise RuntimeError("Run build-post-units and build-creator-mechanism first.")

    post_units = pd.read_csv(post_path)
    creator_units = pd.read_csv(creator_path)

    post_topic_dummies = pd.get_dummies(post_units["topic_name"], prefix="topic", drop_first=True)
    post_model_df = pd.concat([post_units, post_topic_dummies], axis=1)
    post_x = [
        "ln_post_length",
        "aesthetic_marker",
        "shopping_marker",
        "narrative_marker",
        "has_brand_marker",
        "has_question_mark",
        "ln_creator_followers",
    ] + post_topic_dummies.columns.tolist()

    creator_model_df = creator_units.copy()
    creator_model_df["ln_近60天平均评论"] = log1p_series(creator_model_df["近60天平均评论"])
    creator_model_df["ln_视频笔记报价"] = log1p_series(creator_model_df["视频笔记报价"])
    creator_x = [
        "avg_aesthetic_marker",
        "avg_shopping_marker",
        "avg_narrative_marker",
        share_column("appearance_praise"),
        share_column("parasocial_intimacy"),
        share_column("consumption_conversion"),
        "dominant_topic_share",
        "活跃粉丝占比",
        "粉丝数",
    ]
    creator_model_df["ln_粉丝数"] = log1p_series(creator_model_df["粉丝数"])
    creator_x = [c if c != "粉丝数" else "ln_粉丝数" for c in creator_x]

    model_registry = {
        "post_comment_volume": (post_model_df, "ln_comment_count", post_x),
        "post_parasocial_share": (post_model_df, share_column("parasocial_intimacy"), post_x),
        "post_conversion_share": (post_model_df, share_column("consumption_conversion"), post_x),
        "creator_avg_comment": (creator_model_df, "ln_近60天平均评论", creator_x),
        "creator_video_price": (creator_model_df, "ln_视频笔记报价", creator_x),
    }
    sample_frames = {
        "validation": lambda df: df[df["analysis_split"] == "validation"].copy(),
        "full_exploratory": lambda df: df.copy(),
    }

    table_frames = []
    metric_rows = []
    for sample_name, selector in sample_frames.items():
        for name, (base_df, y_col, x_cols) in model_registry.items():
            model_df = selector(base_df)
            model = fit_ols(model_df, y_col, x_cols)
            table = classify_effect_strength(regression_table(model, name))
            table["sample_name"] = sample_name
            table.to_csv(REGRESSION_DIR / f"{name}__{sample_name}.csv", index=False)
            table_frames.append(table)
            if sm is not None and hasattr(model, "nobs"):
                nobs = int(model.nobs)
                r_squared = float(getattr(model, "rsquared", np.nan))
                adj_r_squared = float(getattr(model, "rsquared_adj", np.nan))
                backend = "statsmodels"
            else:
                nobs = int(model["nobs"])
                r_squared = float(model["r_squared"])
                adj_r_squared = float(model["adj_r_squared"]) if not pd.isna(model["adj_r_squared"]) else np.nan
                backend = "sklearn"
            metric_rows.append(
                {
                    "model": name,
                    "sample_name": sample_name,
                    "backend": backend,
                    "nobs": nobs,
                    "r_squared": r_squared,
                    "adj_r_squared": adj_r_squared,
                }
            )
    pd.DataFrame(metric_rows).to_csv(REGRESSION_DIR / "regression_model_metrics.csv", index=False)
    all_tables = pd.concat(table_frames, ignore_index=True)
    all_tables.to_csv(REGRESSION_DIR / "all_regression_tables.csv", index=False)
    validation_tables = all_tables[all_tables["sample_name"] == "validation"].copy()
    validation_tables.to_csv(REGRESSION_DIR / "validation_regression_tables.csv", index=False)
    return REGRESSION_DIR / "regression_model_metrics.csv"


def build_mechanism_report() -> Path:
    ensure_dirs()
    creators, _, _ = load_snapshot()
    topic_path = TOPIC_DIR / "post_topics.csv"
    pred_path = CLASSIFIER_DIR / "prediction_summary.csv"
    linkage_path = CLASSIFIER_DIR / "topic_comment_label_linkage.csv"
    pred_detail_path = CLASSIFIER_DIR / "comments_with_predictions.csv"
    post_unit_path = UNIT_DIR / "post_comment_unit_table.csv"
    creator_mech_path = CREATOR_MECH_DIR / "creator_mechanism_table.csv"
    regression_metrics_path = REGRESSION_DIR / "regression_model_metrics.csv"
    protocol_path = REGRESSION_DIR / "analysis_protocol.json"
    variable_dict_path = REGRESSION_DIR / "variable_dictionary.csv"
    validation_tables_path = REGRESSION_DIR / "validation_regression_tables.csv"

    if not topic_path.exists() or not pred_path.exists() or not linkage_path.exists() or not pred_detail_path.exists():
        raise RuntimeError("Run topic-model-posts and classify-comments before building mechanism report.")

    topic_df = pd.read_csv(topic_path)
    pred_df = pd.read_csv(pred_path)
    linkage_df = pd.read_csv(linkage_path)
    comments_df = pd.read_csv(pred_detail_path)
    post_units = pd.read_csv(post_unit_path) if post_unit_path.exists() else pd.DataFrame()
    creator_mech = pd.read_csv(creator_mech_path) if creator_mech_path.exists() else pd.DataFrame()
    regression_metrics = pd.read_csv(regression_metrics_path) if regression_metrics_path.exists() else pd.DataFrame()
    validation_tables = pd.read_csv(validation_tables_path) if validation_tables_path.exists() else pd.DataFrame()

    topic_totals = topic_df.set_index("topic_id")["post_count"].to_dict()
    dominant_links = []
    for topic_id, group in linkage_df.groupby("topic_id"):
        topic_name = topic_df.loc[topic_df["topic_id"] == topic_id, "topic_name"].iloc[0]
        top_rows = group.sort_values("count", ascending=False).head(4)
        for _, row in top_rows.iterrows():
            if row["primary_predicted_label"] == "other":
                continue
            dominant_links.append(
                {
                    "topic_id": int(topic_id),
                    "topic_name": topic_name,
                    "label": row["primary_predicted_label"],
                    "label_name": LABEL_DISPLAY_NAMES.get(row["primary_predicted_label"], row["primary_predicted_label"]),
                    "count": int(row["count"]),
                    "share_within_topic": float(row["count"]) / max(1, int(group["count"].sum())),
                }
            )

    dominant_df = pd.DataFrame(dominant_links).sort_values(
        ["share_within_topic", "count"], ascending=[False, False]
    )
    dominant_df.to_csv(CLASSIFIER_DIR / "topic_mechanism_signals.csv", index=False)

    label_counts = pred_df.set_index("label")["count"].to_dict()
    mechanism_points = []
    if label_counts.get("appearance_praise", 0) > 0:
        mechanism_points.append("高互动内容明显伴随颜值赞美，说明审美化呈现是注意力入口。")
    if label_counts.get("parasocial_intimacy", 0) > 0:
        mechanism_points.append("评论区存在稳定的拟亲密互动，说明影响力并非只依赖信息，而依赖关系感。")
    if label_counts.get("consumption_conversion", 0) > 0:
        mechanism_points.append("评论中出现同款、购买、代言支持等表达，说明互动已延伸到消费转化。")
    if label_counts.get("advice_regulation", 0) > 0:
        mechanism_points.append("部分粉丝会对妆造、滤镜、进组和直播提出规训建议，说明粉丝关系具有参与式管理特征。")
    if label_counts.get("self_disclosure", 0) > 0:
        mechanism_points.append("大量评论带有考试、开学、上班等自我汇报，说明评论区兼具半公开陪伴空间属性。")

    top_topic_lines = []
    for _, row in topic_df.sort_values("post_count", ascending=False).head(6).iterrows():
        top_topic_lines.append(
            f"- Topic {int(row['topic_id'])} {row['topic_name']}：{int(row['post_count'])} 篇 | {row['keywords']}"
        )

    top_label_lines = []
    for _, row in pred_df.head(8).iterrows():
        top_label_lines.append(f"- {LABEL_DISPLAY_NAMES.get(row['label'], row['label'])}：{int(row['count'])}")

    dominant_lines = []
    for _, row in dominant_df.head(10).iterrows():
        dominant_lines.append(
            f"- Topic {row['topic_id']} {row['topic_name']} → {row['label_name']}：{row['count']} 条，占该主题评论 {row['share_within_topic']:.1%}"
        )

    post_unit_lines = []
    if not post_units.empty:
        strongest_posts = post_units.sort_values(
            [share_column("parasocial_intimacy"), "comment_count"],
            ascending=[False, False],
        ).head(3)
        for _, row in strongest_posts.iterrows():
            post_unit_lines.append(
                f"- {row['creator_name']} | Topic {int(row['topic_id'])} {row['topic_name']} | 评论 {int(row['comment_count'])} | 拟亲密占比 {row[share_column('parasocial_intimacy')]:.1%} | 颜值赞美占比 {row[share_column('appearance_praise')]:.1%}"
            )

    creator_mech_lines = []
    if not creator_mech.empty:
        creator_preview = creator_mech.sort_values("avg_comments_per_post", ascending=False).head(5)
        for _, row in creator_preview.iterrows():
            creator_mech_lines.append(
                f"- {row['creator_name']} | 群体 {row.get('cluster_k3', '')} | 主导主题 {row.get('dominant_topic_name', '')} | 单帖平均评论 {safe_float(row.get('avg_comments_per_post')):.1f} | 拟亲密均值 {safe_float(row.get(share_column('parasocial_intimacy'))):.1%}"
            )

    regression_lines = []
    if not regression_metrics.empty:
        for _, row in regression_metrics[regression_metrics["sample_name"] == "validation"].iterrows():
            regression_lines.append(
                f"- {row['model']}：验证集 N={int(row['nobs'])}，R²={safe_float(row['r_squared']):.3f}"
            )

    split_lines = []
    if not post_units.empty and "analysis_split" in post_units.columns:
        split_summary = (
            post_units.groupby("analysis_split")
            .agg(posts=("note_id", "count"), creators=("creator_id", "nunique"), comments=("comment_count", "sum"))
            .reset_index()
        )
        for _, row in split_summary.iterrows():
            split_lines.append(
                f"- {row['analysis_split']}：{int(row['creators'])} 位博主，{int(row['posts'])} 条帖子，{int(row['comments'])} 条评论"
            )

    effect_lines = []
    if not validation_tables.empty:
        core_terms = {
            "post_comment_volume": ["ln_post_length", "aesthetic_marker", "shopping_marker", "narrative_marker", "has_brand_marker", "has_question_mark", "ln_creator_followers"],
            "post_parasocial_share": ["ln_post_length", "aesthetic_marker", "shopping_marker", "narrative_marker", "has_brand_marker", "has_question_mark", "ln_creator_followers"],
            "post_conversion_share": ["ln_post_length", "aesthetic_marker", "shopping_marker", "narrative_marker", "has_brand_marker", "has_question_mark", "ln_creator_followers"],
            "creator_avg_comment": ["avg_aesthetic_marker", "avg_shopping_marker", "avg_narrative_marker", "appearance_praise_share", "parasocial_intimacy_share", "consumption_conversion_share", "dominant_topic_share", "活跃粉丝占比", "ln_粉丝数"],
            "creator_video_price": ["avg_aesthetic_marker", "avg_shopping_marker", "avg_narrative_marker", "appearance_praise_share", "parasocial_intimacy_share", "consumption_conversion_share", "dominant_topic_share", "活跃粉丝占比", "ln_粉丝数"],
        }
        for model_name, terms in core_terms.items():
            sub = validation_tables[(validation_tables["model"] == model_name) & (validation_tables["term"].isin(terms))]
            if sub.empty:
                continue
            strong = sub[sub["effect_strength"].isin(["strong", "moderate"])].sort_values("p_value").head(2)
            weak = sub[sub["effect_strength"] == "weak"].sort_values("p_value").head(1)
            null_count = int((sub["effect_strength"] == "not_significant").sum())
            for _, row in strong.iterrows():
                effect_lines.append(f"- 强结果 | {model_name} | {row['term']} | coef={row['coef']:.3f} | p={row['p_value']:.3g}")
            for _, row in weak.iterrows():
                effect_lines.append(f"- 弱结果 | {model_name} | {row['term']} | coef={row['coef']:.3f} | p={row['p_value']:.3g}")
            if null_count:
                effect_lines.append(f"- 空结果 | {model_name} | {null_count} 个核心变量未达 10% 显著性")

    creator_lines = []
    creator_stats = creators.groupby(["cluster_k3"]).agg(
        creators=("creator_id", "count"),
        notes=("note_count", "sum"),
        comments=("comment_count", "sum"),
    ).reset_index()
    for _, row in creator_stats.iterrows():
        creator_lines.append(
            f"- 群体 {row['cluster_k3']}：{int(row['creators'])} 位博主，{int(row['notes'])} 篇帖子，{int(row['comments'])} 条评论"
        )

    report = []
    report.append("# 小红书高影响力达人影响力形成机制报告")
    report.append("")
    report.append("## 样本")
    report.append(f"- 博主：{len(creators)}")
    report.append(f"- 帖子：{int(topic_df['post_count'].sum())}")
    report.append(f"- 评论：{int(comments_df.shape[0])}")
    report.extend(creator_lines)
    report.append("")
    report.append("## 分析纪律")
    report.append(f"- 主模型规格冻结文件：{protocol_path}")
    report.append(f"- 变量字典：{variable_dict_path}")
    report.append("- 主结果统一使用 validation 切分；full_exploratory 仅作探索性对照。")
    report.append("- 同时披露强结果、弱结果和空结果，不根据显著性删模型。")
    report.append("")
    report.append("## 样本切分")
    report.extend(split_lines)
    report.append("")
    report.append("## 内容供给结构")
    report.extend(top_topic_lines)
    report.append("")
    report.append("## 粉丝互动结构")
    report.extend(top_label_lines)
    report.append("")
    report.append("## 内容-互动联动")
    report.extend(dominant_lines)
    if post_unit_lines:
        report.append("")
        report.append("## 单帖级机制样本")
        report.extend(post_unit_lines)
    if creator_mech_lines:
        report.append("")
        report.append("## 博主级机制差异")
        report.extend(creator_mech_lines)
    if regression_lines:
        report.append("")
        report.append("## 回归验证")
        report.extend(regression_lines)
    if effect_lines:
        report.append("")
        report.append("## 验证集结果分层")
        report.extend(effect_lines)
    report.append("")
    report.append("## 机制解释")
    for point in mechanism_points:
        report.append(f"- {point}")
    report.append("")
    report.append("## 论文可直接使用的主结论")
    report.append("- 高影响力达人并不主要依赖高信息密度内容，而更多依赖日常化表达、审美化呈现与拟亲密互动来形成影响力。")
    report.append("- 内容供给侧的主题结构与评论区互动结构存在清晰联动，不同内容会触发不同类型的粉丝回应。")
    report.append("- 评论区不仅是反馈区，更是关系建构区，兼具情感陪伴、身份投射、消费动员与参与式规训功能。")
    report.append("- 当前结果应被视为探索性实证；若验证集上是弱结果或空结果，应按真实情况陈述，不将其包装为强结论。")

    output_path = PLOTS_DIR / "mechanism_report.md"
    output_path.write_text("\n".join(report) + "\n", encoding="utf-8")
    return output_path


def summarize_results() -> None:
    ensure_dirs()
    creators, posts, comments = load_snapshot()

    lines = []
    lines.append("# Xiaohongshu Text Mining Snapshot Summary")
    lines.append("")
    lines.append("## Snapshot")
    lines.append(f"- Creators: {len(creators)}")
    lines.append(f"- Posts: {len(posts)}")
    lines.append(f"- Comments: {len(comments)}")

    topic_path = TOPIC_DIR / "post_topics.csv"
    if topic_path.exists():
        topic_df = pd.read_csv(topic_path)
        lines.append("")
        lines.append("## Post Topics")
        for _, row in topic_df.head(8).iterrows():
            lines.append(
                f"- Topic {row['topic_id']}: {int(row['post_count'])} posts | {row['keywords']}"
            )

    pred_path = CLASSIFIER_DIR / "prediction_summary.csv"
    if pred_path.exists():
        pred_df = pd.read_csv(pred_path)
        lines.append("")
        lines.append("## Comment Interaction Labels")
        for _, row in pred_df.head(10).iterrows():
            lines.append(f"- {row['label']}: {int(row['count'])}")

    mech_path = None
    try:
        mech_path = build_mechanism_report()
    except Exception:
        mech_path = None
    if mech_path:
        lines.append("")
        lines.append(f"## Mechanism Report")
        lines.append(f"- {mech_path}")

    summary_path = PLOTS_DIR / "summary.md"
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(summary_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incremental text mining pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)

    snapshot = sub.add_parser("snapshot", help="Freeze creator/post/comment snapshot")
    snapshot.add_argument("--sample-size", type=int, default=500)

    topic = sub.add_parser("topic-model-posts", help="Run topic model on snapshot posts")
    topic.add_argument("--topic-count", type=int, default=DEFAULT_TOPIC_COUNT)

    llm = sub.add_parser("llm-label-comments", help="Label sampled comments with LLM")
    llm.add_argument("--sample-size", type=int, default=DEFAULT_LLM_SAMPLE_SIZE)
    llm.add_argument("--batch-size", type=int, default=DEFAULT_LLM_BATCH_SIZE)

    sub.add_parser("export-llm-labels", help="Merge labeled comment batches into CSV")
    sub.add_parser("train-comment-classifier", help="Train weakly supervised comment classifier")
    sub.add_parser("classify-comments", help="Predict labels for all snapshot comments")
    sub.add_parser("build-post-units", help="Build one-row-per-post interaction table")
    sub.add_parser("build-creator-mechanism", help="Aggregate post units to creator level and join creator base")
    sub.add_parser("run-regressions", help="Run post-level and creator-level mechanism regressions")
    sub.add_parser("summarize", help="Write markdown summary")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.cmd == "snapshot":
        build_snapshot(sample_size=args.sample_size)
    elif args.cmd == "topic-model-posts":
        run_topic_model_on_posts(topic_count=args.topic_count)
    elif args.cmd == "llm-label-comments":
        llm_label_comments(sample_size=args.sample_size, batch_size=args.batch_size)
    elif args.cmd == "export-llm-labels":
        export_llm_labels()
    elif args.cmd == "train-comment-classifier":
        train_comment_classifier()
    elif args.cmd == "classify-comments":
        classify_comments()
    elif args.cmd == "build-post-units":
        print(build_post_comment_unit_table())
    elif args.cmd == "build-creator-mechanism":
        print(build_creator_mechanism_table())
    elif args.cmd == "run-regressions":
        print(run_mechanism_regressions())
    elif args.cmd == "summarize":
        summarize_results()
    else:  # pragma: no cover
        raise ValueError(f"Unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
