#!/usr/bin/env python3
"""
Create a full-data revised DOCX copy while preserving the original thesis file.

This script:
- reads the latest chapter-4 support tables generated from text_mining_full
- starts from the original 20260331 DOCX
- updates selected chapter-4 sections in a copied DOCX
- leaves the original file untouched
"""

from __future__ import annotations

import copy
import csv
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
W = "{%s}" % NS["w"]

BASE_DIR = Path(__file__).resolve().parent.parent
TABLES_DIR = BASE_DIR / "reports" / "chapter4_support" / "tables"
SNAPSHOT_MANIFEST = BASE_DIR / "data" / "text_mining_full" / "snapshot" / "manifest.json"

ORIGINAL_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿20260331.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿20260331_全量数据重做版.docx")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def read_snapshot_numbers() -> dict[str, int]:
    manifest = json.loads(SNAPSHOT_MANIFEST.read_text(encoding="utf-8"))
    return {
        "creators_with_text": int(manifest["sample_size_actual"]),
        "posts": int(manifest["posts"]),
        "comments": int(manifest["comments"]),
    }


def format_pct(value: str, digits: int = 1) -> str:
    return f"{float(value) * 100:.{digits}f}%"


def format_int(value: str) -> str:
    return f"{int(float(value))}"


def paragraph_text(p: etree._Element) -> str:
    return "".join(t.text or "" for t in p.xpath(".//w:t", namespaces=NS)).strip()


def first_run_style(paragraph: etree._Element) -> etree._Element | None:
    run = paragraph.find("./w:r", NS)
    if run is None:
        return None
    rpr = run.find("./w:rPr", NS)
    return copy.deepcopy(rpr) if rpr is not None else None


def clear_paragraph_runs(paragraph: etree._Element) -> None:
    for child in list(paragraph):
        if child.tag != f"{W}pPr":
            paragraph.remove(child)


def set_paragraph_text(paragraph: etree._Element, text: str) -> None:
    rpr = first_run_style(paragraph)
    clear_paragraph_runs(paragraph)
    run = etree.SubElement(paragraph, f"{W}r")
    if rpr is not None:
        run.append(rpr)
    t = etree.SubElement(run, f"{W}t")
    if text.startswith(" ") or text.endswith(" "):
        t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
    t.text = text


def clone_body_paragraph(template: etree._Element, text: str) -> etree._Element:
    new_p = copy.deepcopy(template)
    set_paragraph_text(new_p, text)
    return new_p


def insert_after(ref_p: etree._Element, new_p: etree._Element) -> None:
    parent = ref_p.getparent()
    parent.insert(parent.index(ref_p) + 1, new_p)


def find_paragraph_by_exact_text(paragraphs: list[etree._Element], text: str) -> etree._Element:
    for p in paragraphs:
        if paragraph_text(p) == text:
            return p
    raise ValueError(f"Paragraph not found: {text}")


def replace_section_body(
    paragraphs: list[etree._Element],
    heading_text: str,
    next_heading_text: str,
    new_texts: list[str],
) -> None:
    heading = find_paragraph_by_exact_text(paragraphs, heading_text)
    next_heading = find_paragraph_by_exact_text(paragraphs, next_heading_text)
    parent = heading.getparent()
    children = list(parent)
    start_idx = children.index(heading) + 1
    end_idx = children.index(next_heading)
    target_paras = [el for el in children[start_idx:end_idx] if el.tag == f"{W}p"]
    if not target_paras:
        raise ValueError(f"No body paragraphs found between {heading_text} and {next_heading_text}")

    template = target_paras[0]
    set_paragraph_text(target_paras[0], new_texts[0])

    for p in target_paras[1:]:
        txt = paragraph_text(p)
        if txt.startswith("图") or txt.startswith("表") or txt == "":
            continue
        parent.remove(p)

    anchor = target_paras[0]
    for text in new_texts[1:]:
        new_p = clone_body_paragraph(template, text)
        insert_after(anchor, new_p)
        anchor = new_p


def first_body_paragraph_between(
    paragraphs: list[etree._Element],
    heading_text: str,
    next_heading_text: str,
) -> etree._Element:
    heading = find_paragraph_by_exact_text(paragraphs, heading_text)
    next_heading = find_paragraph_by_exact_text(paragraphs, next_heading_text)
    parent = heading.getparent()
    children = list(parent)
    start_idx = children.index(heading) + 1
    end_idx = children.index(next_heading)
    target_paras = [el for el in children[start_idx:end_idx] if el.tag == f"{W}p"]
    if not target_paras:
        raise ValueError(f"No body paragraph found between {heading_text} and {next_heading_text}")
    return target_paras[0]


def build_replacements() -> dict[str, list[str]]:
    snapshot = read_snapshot_numbers()
    sources = read_csv_rows(TABLES_DIR / "table_4_1_data_sources.csv")
    sample_sizes = read_csv_rows(TABLES_DIR / "table_4_1_sample_sizes.csv")
    topic_rows = read_csv_rows(TABLES_DIR / "table_4_3a_topic_lda_results.csv")
    topic_compare = read_csv_rows(TABLES_DIR / "table_4_3e_group_topic_distribution_compare.csv")
    comment_compare = read_csv_rows(TABLES_DIR / "table_4_9_group_comment_interaction_compare.csv")
    cluster_metrics = read_csv_rows(TABLES_DIR / "table_4_5_cluster_k_metrics.csv")
    cluster_profile = read_csv_rows(TABLES_DIR / "table_4_7_cluster_profile_summary.csv")
    comment_words = read_csv_rows(TABLES_DIR / "table_4_8_group_comment_top_words_compare.csv")

    aligned_creators = next(row for row in sources if row["数据层级"] == "文本-聚类对齐达人")["样本量"]
    aligned_posts = next(row for row in sample_sizes if row["统计对象"] == "带群体标签的帖子")
    aligned_comments = next(row for row in sample_sizes if row["统计对象"] == "带群体标签的评论")

    top_topics = "、".join(
        f"{row['主题名称']}（{row['帖子数']}帖）"
        for row in topic_rows[:4]
    )

    def group_top_topic(group: int) -> str:
        key = f"群体{group}_占比"
        row = max(topic_compare, key=lambda item: float(item.get(key, 0) or 0))
        return row["主题名称"]

    def group_top_words(group: int) -> str:
        key = f"群体{group}_高频词"
        words = [row[key] for row in comment_words[:5] if row.get(key)]
        return "、".join(words)

    comment_map = {row["互动类型"]: row for row in comment_compare}
    best_k = max(cluster_metrics, key=lambda row: float(row["轮廓系数"]))
    profile_by_group = {int(row["群体标签_k3"]): row for row in cluster_profile}

    return {
        "4.1数据来源、获取与预处理": [
            f"本研究的数据来源分为达人画像数据与文本数据两部分。达人画像数据来自灰豚平台整理后的结构化总表，共计3403位高影响力生活类达人；聚类分析样本采用带有群体标签_k3的达人聚类结果表，共计3371位达人。文本数据采用当前已补齐达人名称与核心指标的 DOM 点击抓取主文本池，共覆盖{snapshot['creators_with_text']}位有文本的达人，包含{snapshot['posts']}条帖子文本与{snapshot['comments']}条评论文本。将文本数据与聚类结果按creator_id进行对齐后，最终获得{aligned_creators}位可用于群体比较的达人、{aligned_posts['样本量']}条帖子与{aligned_comments['样本量']}条评论。",
            "在预处理环节，本文首先对达人简介、帖子标题、帖子正文与评论内容进行统一清洗，去除URL、换行与冗余空格；随后使用jieba进行中文分词，并结合停用词表剔除助词、代词、平台通用词及长度小于2的无效词项。在此基础上，分别开展帖子主题提取、关键词共现网络构建与评论互动类型识别，并将结果映射回三类达人群体进行比较分析，以增强第四章文本分析部分的证据链完整性。",
        ],
        "4.3 基于文本挖掘的达人内容策略与人设特征解析": [
            "为回应旧稿中文本样本偏小、主题提取支撑不足的问题，本文将第四章的文本分析扩展到全量抓取结果，并细化为内容主题分析与评论互动分析两部分。前者主要对应达人帖子标题与正文，后者主要对应评论文本；具体处理流程包括文本清洗、中文分词、停用词过滤、LDA主题提取、关键词共现网络构建以及评论互动类型归纳，并按三类聚类群体完成映射比较。"
        ],
        "4.3.1 泛生活与垂直兴趣交织的内容生态网络解析": [
            f"基于{snapshot['posts']}条帖子文本，本文将标题与正文合并后进行清洗、分词，并采用LDA识别内容主题。当前样本中较稳定识别出的主题主要包括{top_topics}。这一结果说明，高影响力生活类达人并不是围绕单一赛道持续输出，而是在泛生活表达之下嵌入旅行、居家审美、护肤抗老、母婴育儿等不同垂直兴趣主题。",
            f"从群体比较看，群体0更偏向{group_top_topic(0)}，群体1更偏向{group_top_topic(1)}，群体2则更偏向{group_top_topic(2)}。考虑到三类群体中{group_top_topic(0)}都占据主体份额，这意味着平台头部生活类达人的共同底盘依然是日常化、场景化与情绪氛围化表达；但群体1在母婴与消费导向主题上的占比相对更高，显示其内容结构更容易承接种草和商业转化。",
            "在共现网络构建中，本文将经清洗后保留的高频词作为网络节点，将两个关键词在同一帖子文本中共同出现一次记为一次共现，并以累计共现次数作为边权重。由此可以看到，“旅行”“氛围”“酒店”“美食”“护肤”“春天”“家居”等词并非孤立出现，而是在生活方式表达中相互勾连，共同构成一种兼具审美感、陪伴感与消费引导功能的内容生态。",
            "因此，帖子文本分析表明，小红书高影响力生活类达人更常通过泛生活叙事扩大触达面，再通过垂直兴趣主题形成记忆点与商业承接位。也就是说，内容供给侧呈现出“泛生活底盘+垂直兴趣补充”的复合结构，而非高度垂直化的单一输出模式。",
        ],
        "4.3.2 信任机制下的超级分享者人设文本解码": [
            "4.3.2 信任机制下的人设与评论互动文本解码"
        ],
        "4.3.2 信任机制下的人设与评论互动文本解码": [
            f"在互动文本层面，本文进一步对{aligned_comments['样本量']}条已对齐评论进行类型识别与群体比较。评论高频词显示，群体0更常出现{group_top_words(0)}等表达，群体1更常出现{group_top_words(1)}，群体2则更多出现{group_top_words(2)}。这些高频词大多围绕称呼、情绪反馈、审美判断和求回应展开，说明评论区的核心并不是理性讨论，而是围绕亲近感、陪伴感与即时反馈形成的轻互动场域。",
            f"从互动类型占比看，三类群体均以“其他”类评论为主，但仍存在明显差异：群体0的颜值赞美占比达到{format_pct(comment_map['颜值赞美']['群体0_占比'])}，高于群体1和群体2；群体1的提问求回应占比达到{format_pct(comment_map['提问求回应']['群体1_占比'])}，同时消费转化占比也略高；群体2的“其他”类评论占比达到{format_pct(comment_map['其他']['群体2_占比'])}，结构相对更分散。由此可见，高影响力达人在评论区激发的并不只是统一的情感支持，而是颜值赞美、拟亲密互动、提问求回应与轻度消费转化共同构成的互动机制。",
        ],
        "4.4.1 聚类特征变量的选择与模型设定": [
            "参考既有平台达人研究中对创作者进行类型划分的思路，本文选取粉丝数、Top1年龄段占比、藏赞比、评赞比和商业笔记占比五项指标构建K-means聚类模型。其中，粉丝数用于衡量账号的基础影响力，Top1年龄段占比用于刻画粉丝年龄结构的集中程度，藏赞比和评赞比分别反映内容被保存与引发讨论的倾向，商业笔记占比则用于衡量达人商业合作强度。聚类前先对上述变量进行标准化处理，以降低量纲差异带来的影响。",
            f"为确定最优聚类数K，本文综合使用肘部法则与轮廓系数，对不同K值方案进行比较。结果显示，当K={format_int(best_k['K值'])}时，轮廓系数达到{float(best_k['轮廓系数']):.3f}，为当前比较范围内的最高值，同时SSE曲线在较低K值区间后趋于平缓，说明三类划分在统计表现与结果解释之间取得了较好平衡。因此，本文最终采用K=3的聚类方案对3371位达人进行群体划分。",
        ],
        "4.4.2 三类高影响力达人群体的画像特征解析": [
            f"基于K-means聚类结果，本文将3371位高影响力达人划分为三类特征鲜明的群体。群体0共有{format_int(profile_by_group[0]['群体规模'])}位达人，占比最高，整体表现较为均衡，可概括为“均衡主流型”；群体1共有{format_int(profile_by_group[1]['群体规模'])}位达人，在藏赞比和商业笔记占比上明显高于整体均值，可概括为“收藏转化与商业合作型”；群体2共有{format_int(profile_by_group[2]['群体规模'])}位达人，评赞比显著高于其余两类，可概括为“高评论互动型”。三类群体的并存表明，小红书高影响力生活类达人生态并非均质结构，而是存在明确的功能分化。",
            f"进一步结合文本挖掘结果可以发现，不同群体在内容与互动机制上也存在差异。均衡主流型达人更依赖{group_top_topic(0)}这类泛生活表达维持广泛触达；收藏转化与商业合作型达人在{group_top_topic(1)}及消费导向评论上更突出，更容易承接品牌合作与种草场景；高评论互动型达人虽然规模较小，但其评赞比达到{float(profile_by_group[2]['评赞比']):.3f}，说明其单位点赞对应的评论讨论度更强。由此可见，达人画像变量、内容策略与评论互动之间并非彼此割裂，而是共同构成高影响力达人的形成机制。",
        ],
    }


def main() -> None:
    if not ORIGINAL_DOC.exists():
        raise FileNotFoundError(ORIGINAL_DOC)

    replacements = build_replacements()

    with ZipFile(ORIGINAL_DOC) as zin:
        xml_bytes = zin.read("word/document.xml")
        doc = etree.fromstring(xml_bytes)
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        replace_section_body(
            paragraphs,
            "4.1 高影响力达人画像结构分析",
            "4.2 高影响力达人群体划分与类型特征",
            replacements["4.1数据来源、获取与预处理"],
        )

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p_431 = find_paragraph_by_exact_text(paragraphs, "4.3.1 基于LDA的内容主题提取与内容生态网络解析")
        intro_template = first_body_paragraph_between(
            paragraphs,
            "4.3.1 基于LDA的内容主题提取与内容生态网络解析",
            "4.3.2 信任机制下的人设与评论互动文本解码",
        )
        intro_p = clone_body_paragraph(
            intro_template,
            replacements["4.3 基于文本挖掘的达人内容策略与人设特征解析"][0],
        )
        p_431.getparent().insert(p_431.getparent().index(p_431), intro_p)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        replace_section_body(
            paragraphs,
            "4.3.1 基于LDA的内容主题提取与内容生态网络解析",
            "4.3.2 信任机制下的人设与评论互动文本解码",
            replacements["4.3.1 泛生活与垂直兴趣交织的内容生态网络解析"],
        )

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        replace_section_body(
            paragraphs,
            "4.3.2 信任机制下的人设与评论互动文本解码",
            "4.3.3 内容发布时机",
            replacements["4.3.2 信任机制下的人设与评论互动文本解码"],
        )

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        replace_section_body(
            paragraphs,
            "4.2.1 聚类特征变量的选择与模型设定",
            "4.2.2 三类高影响力达人群体的画像特征解析",
            replacements["4.4.1 聚类特征变量的选择与模型设定"],
        )

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        replace_section_body(
            paragraphs,
            "4.2.2 三类高影响力达人群体的画像特征解析",
            "4.3 高影响力达人行为模式的文本表现",
            replacements["4.4.2 三类高影响力达人群体的画像特征解析"],
        )

        new_xml = etree.tostring(doc, xml_declaration=True, encoding="UTF-8", standalone="yes")

        with ZipFile(OUTPUT_DOC, "w", compression=ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = new_xml if item.filename == "word/document.xml" else zin.read(item.filename)
                zout.writestr(item, data)

    print(OUTPUT_DOC)


if __name__ == "__main__":
    main()
