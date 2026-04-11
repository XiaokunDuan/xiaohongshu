#!/usr/bin/env python3
"""
Final synchronization pass for bilingual abstract, keywords, and conclusion wording.
"""

from __future__ import annotations

import copy
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
W = "{%s}" % NS["w"]

INPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_同步修改版.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_最终同步版.docx")


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
    t.text = text


def find_by_prefix(paragraphs: list[etree._Element], prefix: str) -> etree._Element:
    for p in paragraphs:
        txt = paragraph_text(p)
        if txt.startswith(prefix):
            return p
    raise ValueError(f"Paragraph with prefix not found: {prefix}")


REPLACEMENTS = [
    (
        "The rise of content-driven e-commerce has fundamentally changed how consumers make purchasing decisions",
        "The rise of content-driven e-commerce has fundamentally changed how consumers make purchasing decisions and how brands reach their audiences. On platforms like Xiaohongshu (Little Red Book), high-influence lifestyle creators stand at the center of this shift, yet systematic empirical descriptions of their profiles, content strategies, and typological differentiation remain limited. This study examines 3,403 Xiaohongshu lifestyle creators with at least 100,000 followers, combining descriptive statistics, text mining of posts and comments, and K-means clustering to analyze their behavioral patterns. The findings are threefold. First, the creator ecosystem is structurally concentrated: female creators account for 66.62% of the sample, mid-tier creators make up 87.4%, and 61.74% operate independently. Their core audience is concentrated among women aged 18–34 in higher-tier cities. Second, successful creators tend to adopt a hybrid content strategy that combines broad lifestyle expression with selected vertical interests, while maintaining closeness through everyday persona construction and comment interaction. Post and comment texts show that appearance praise, parasocial intimacy, question-response interaction, and light consumption conversion form the main interaction mechanisms. Third, K-means clustering identifies three distinct groups: balanced mainstream creators, collection-and-commercial-conversion creators, and high-comment-interaction creators. These groups differ not only in follower structure, but also in content expression and interaction patterns. Overall, the study extends the supply-side understanding of influencer marketing by linking creator profiles, text expression, and interaction mechanisms, and offers practical implications for creators, brands, and platform governance."
    ),
    (
        "Keywords: Influencer Marketing; Content Strategy; K-means Clustering; User Profiling",
        "Keywords: Influencer Marketing; Text Mining; K-means Clustering; User Profiling"
    ),
    (
        "关键词：达人营销；内容策略；K-means聚类；用户画像",
        "关键词：达人营销；文本挖掘；K-means聚类；用户画像"
    ),
    (
        "本研究以小红书平台3403位粉丝数10万以上的生活记录类达人为样本，以粉丝画像数据为核心分析维度",
        "本研究以小红书平台3403位粉丝数10万以上的生活记录类达人为样本，并结合7375条帖子与94247条评论的文本数据，围绕三个研究问题展开系统分析，主要结论如下。"
    ),
    (
        "在内容策略与人设构建方面，成功达人普遍采用泛生活+垂直兴趣的T型内容布局",
        "在内容策略与互动机制方面，高影响力达人普遍采用泛生活+垂直兴趣的复合内容布局，并通过日常化、可亲近的人设表达维系与粉丝的关系。评论文本显示，颜值赞美、拟亲密互动、提问求回应与轻度消费转化构成了主要互动机制。内容发布时机上，粉丝活跃高峰集中于工作日17至21时；内容形式上，视频报价溢价约38%，但最优内容形式因达人类型而异，不存在统一最优解。"
    ),
]


def main() -> None:
    if not INPUT_DOC.exists():
        raise FileNotFoundError(INPUT_DOC)

    with ZipFile(INPUT_DOC) as zin:
        doc = etree.fromstring(zin.read("word/document.xml"))
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        for prefix, new_text in REPLACEMENTS:
            p = find_by_prefix(paragraphs, prefix)
            set_paragraph_text(p, new_text)

        new_xml = etree.tostring(doc, xml_declaration=True, encoding="UTF-8", standalone="yes")

        with ZipFile(OUTPUT_DOC, "w", compression=ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = new_xml if item.filename == "word/document.xml" else zin.read(item.filename)
                zout.writestr(item, data)

    print(OUTPUT_DOC)


if __name__ == "__main__":
    main()
