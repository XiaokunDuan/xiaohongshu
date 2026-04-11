#!/usr/bin/env python3
"""
Synchronize abstract, method description, framework, and conclusion
with the revised chapter-4 content while preserving formatting.
"""

from __future__ import annotations

import copy
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
W = "{%s}" % NS["w"]

INPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_第四章改稿.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_同步修改版.docx")


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
        "本文以小红书平台3403位粉丝数10万以上的生活记录类达人为研究对象，综合运用描述性统计分析、文本挖掘（词云与标签共现网络）及K-means聚类算法",
        "本文以小红书平台3403位粉丝数10万以上的生活记录类达人为研究对象，综合运用描述性统计分析、文本挖掘（帖子关键词共现、评论互动类型归纳与群体对比）及K-means聚类算法，围绕三个研究问题展开分析。研究发现如下：第一，该群体整体呈现女性主导（66.62%）、腰部达人为主（87.4%）、独立运营居多（61.74%）的结构特征，地域上高度集中于高线省市，核心受众为18至34岁年轻女性，商业笔记占比中位数仅为4.7%。第二，高影响力达人在内容策略上普遍采用“泛生活+垂直兴趣”的复合布局，并通过日常化、可亲近的人设表达与评论区互动维系粉丝关系；评论文本显示，颜值赞美、拟亲密互动、提问求回应和消费转化构成了主要互动机制。第三，经肘部法则与轮廓系数验证，K-means聚类将达人生态划分为均衡主流型、收藏转化与商业合作型、高评论互动型三类群体，不同群体在粉丝画像结构、内容表达与互动模式上表现出显著差异，与意见领袖理论和社会认同理论的分析框架相互印证。"
    ),
    (
        "本文采用的技术路线为数据分析法，包括对于已获取数据的描述性统计，达人标签的共现词网络分析，以及达人群体的k-means聚类分析。",
        "本文采用的技术路线包括描述性统计分析、文本挖掘与K-means聚类分析三部分。其中，文本挖掘部分不仅包含达人标签与帖子文本的高频词和共现网络分析，还进一步纳入评论文本的互动类型归纳与三类群体对比，以增强对内容策略和粉丝互动机制的解释。"
    ),
    (
        "目前的数据集由小红书3400多位10万粉丝及以上的生活记录类达人组成。原始数据包括：",
        "目前的数据集由3403位粉丝数10万以上的生活记录类达人画像样本，以及7375条帖子、94247条评论构成的文本样本组成。原始数据主要包括："
    ),
    (
        "数据预处理包括清洗、转换、特征工程三步。之后得到便于分析的数据形式。",
        "数据预处理包括清洗、转换、特征工程与文本分词四步。在结构化画像数据基础上，进一步对帖子标题、正文、达人简介与评论文本进行清洗、分词与停用词过滤，并将文本样本与三类聚类群体进行映射，以支持后续文本挖掘与群体比较分析。"
    ),
    (
        "本文的创新点在于从供给侧视角切入，以大样本数据对高影响力达人群体的画像特征与类型分化进行系统描述，弥补现有研究偏重消费者侧、忽视达人本身的不足。",
        "本文的创新点在于从供给侧视角切入，在大样本达人画像数据基础上进一步引入帖子与评论文本，对高影响力达人群体的内容表达、互动机制与类型分化进行联动分析，从而弥补现有研究偏重消费者侧、忽视达人供给行为与评论互动结构的不足。"
    ),
    (
        "基于上述两个理论，本文构建如下分析框架：意见领袖理论指出高影响力个体通过专业性与信任机制影响他人决策",
        "基于上述两个理论，本文构建如下分析框架：意见领袖理论指出高影响力个体通过信任机制、持续内容供给与互动关系影响他人决策，对应本文第四章中对达人内容策略、人设表达、评论互动机制及商业化差异的分析；社会认同理论强调受众通过群体归属感驱动消费行为，对应本文对粉丝画像特征、评论中的拟亲密互动与群体差异的解释。具体而言，聚类变量中的Top1年龄段占比和粉丝关注焦点反映了受众的社会认同结构，而藏赞比、评赞比、商业笔记占比以及评论互动差异共同揭示了达人影响力形成的不同路径。两个理论共同构成了“达人供给特征—用户认同需求—互动反馈机制”的分析视角。"
    ),
    (
        "在达人群体类型分化方面，经肘部法则与轮廓系数验证，最优聚类数为K=3，形成三类特征鲜明的群体。主流生活型（1659人，49%）",
        "在达人群体类型分化方面，经肘部法则与轮廓系数验证，最优聚类数为K=3，形成三类特征鲜明的群体。均衡主流型（2671人）整体表现较为均衡，是平台中的主流高影响力达人；收藏转化与商业合作型（617人）在藏赞比和商业笔记占比上更高；高评论互动型（83人）则表现出更强的评论互动特征。三类群体的并存表明，小红书达人生态已呈现出功能不同、策略各异的多元分化格局。从理论视角看，三类群体分别体现了不同的影响力实现路径与受众认同机制，与本文所构建的分析框架相互印证。"
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
