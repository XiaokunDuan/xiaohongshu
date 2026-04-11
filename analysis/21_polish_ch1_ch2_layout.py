#!/usr/bin/env python3
"""
Polish chapter 1 and 2 paragraph quality/layout in the thesis DOCX.

- convert rough bullet-like items in 1.4.1 into proper prose paragraphs
- split overlong review paragraphs into cleaner academic paragraphs
- slightly tighten first two chapters without touching later chapters
"""

from __future__ import annotations

import copy
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
W = "{%s}" % NS["w"]

INPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_送审版.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_前两章润色版.docx")


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


def clone_with_text(template: etree._Element, text: str) -> etree._Element:
    new_p = copy.deepcopy(template)
    set_paragraph_text(new_p, text)
    return new_p


def find_by_prefix(paragraphs: list[etree._Element], prefix: str) -> etree._Element:
    for p in paragraphs:
        if paragraph_text(p).startswith(prefix):
            return p
    raise ValueError(f"Paragraph prefix not found: {prefix}")


def find_by_exact(paragraphs: list[etree._Element], text: str) -> etree._Element:
    for p in paragraphs:
        if paragraph_text(p) == text:
            return p
    raise ValueError(f"Paragraph not found: {text}")


def insert_after(ref: etree._Element, new_el: etree._Element) -> None:
    parent = ref.getparent()
    parent.insert(parent.index(ref) + 1, new_el)


def remove_paragraph(body: etree._Element, text: str) -> None:
    for child in list(body):
        if etree.QName(child).localname == "p" and paragraph_text(child) == text:
            body.remove(child)
            return
    raise ValueError(f"Paragraph not found for removal: {text}")


def main() -> None:
    if not INPUT_DOC.exists():
        raise FileNotFoundError(INPUT_DOC)

    with ZipFile(INPUT_DOC) as zin:
        doc = etree.fromstring(zin.read("word/document.xml"))
        body = doc.xpath("//w:body", namespaces=NS)[0]
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # Chapter 1: make 1.4.1 look like real prose instead of rough list items
        intro = find_by_prefix(paragraphs, "目前的数据集由3403位粉丝数10万以上的生活记录类达人画像样本")
        list1 = find_by_exact(paragraphs, "1.基础身份信息，比如达人名称、小红书号、性别、地域。")
        list2 = find_by_exact(paragraphs, "2.影响力量化，包括粉丝数、赞藏总数等，用衡量达人的影响力。")
        list3 = find_by_exact(paragraphs, "3.近期内容营销表现，包括笔记总数、商业笔记总数以及近60天平均点赞/收藏/评论/分享，图文/视频笔记报价及对应的CPE（单次互动成本）和CPM（千次曝光成本）。")
        list4 = find_by_exact(paragraphs, "4.粉丝画像数据，包括活跃粉丝占比、水粉占比、粉丝男/女比例、粉丝地域、粉丝年龄、粉丝关注焦点等半结构化文本。")
        tail = find_by_prefix(paragraphs, "数据预处理包括清洗、转换、特征工程与文本分词四步")

        set_paragraph_text(
            intro,
            "本文所使用的数据包括两部分：一是3403位粉丝数10万以上的生活记录类达人画像样本，二是7375条帖子与94247条评论构成的文本样本。围绕研究问题，原始数据主要从基础身份、影响力表现、内容营销与商业化、粉丝画像四个层面进行整理。",
        )
        set_paragraph_text(
            list1,
            "第一，基础身份信息主要包括达人名称、小红书号、性别和地域等字段，用于刻画创作者的基本属性与账号归属特征。",
        )
        set_paragraph_text(
            list2,
            "第二，影响力量化指标主要包括粉丝数、累计获赞与收藏数等，用于衡量达人在平台中的基础传播规模与可见度。",
        )
        set_paragraph_text(
            list3,
            "第三，内容营销与商业化表现主要包括笔记总数、商业笔记总数、近60天平均点赞/收藏/评论/分享，以及图文和视频笔记的报价、CPE和CPM等指标，用于描述达人在内容供给和商业合作中的表现差异。",
        )
        set_paragraph_text(
            list4,
            "第四，粉丝画像数据主要包括活跃粉丝占比、水粉占比、粉丝性别比例、粉丝地域、粉丝年龄结构以及粉丝关注焦点等半结构化信息，用于刻画达人所连接的受众基本盘。",
        )
        set_paragraph_text(
            tail,
            "在上述数据基础上，本文进一步完成清洗、转换、特征工程与文本分词等预处理步骤，并对帖子标题、正文、达人简介与评论文本进行停用词过滤和群体映射，以支持后续的文本挖掘与聚类分析。",
        )

        set_paragraph_text(
            find_by_prefix(paragraphs, "本文的创新点在于从供给侧视角切入"),
            "本文的创新点主要体现在两个方面。其一，研究视角从平台消费侧转向达人供给侧，在大样本画像数据基础上系统刻画高影响力生活类达人的结构特征与群体差异。其二，在结构化画像数据之外进一步引入帖子与评论文本，通过文本挖掘与聚类分析的联动，呈现达人内容表达、互动机制与类型分化之间的关系。",
        )

        # Chapter 2: split long literature review paragraphs into readable academic blocks
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p98 = find_by_prefix(paragraphs, "在文本与用户行为研究方面，既有文献主要关注UGC内容如何影响用户的信息获取")
        p100 = find_by_prefix(paragraphs, "在KOL营销与达人影响力研究方面，现有文献已经从知识网红营销")
        p109 = find_by_prefix(paragraphs, "基于上述两个理论，本文将研究问题拆解为三个相互衔接的层面")

        # replace with shorter first half
        set_paragraph_text(
            p98,
            "在文本与用户行为研究方面，既有文献主要关注UGC内容如何影响用户的信息获取、情绪卷入与消费决策。一类研究从社交营销与信任机制出发，指出社会互动连接、共同愿景、信息质量和服务质量会通过提升信任与社交意愿，进而影响消费意愿；另一类研究则从用户动机和平台网络结构切入，发现小红书用户的信息分享主要围绕生活经验、购物和食品等主题，并通过弱关系传播、结构洞位置和社群认同形成内容扩散。",
        )
        p98b = clone_with_text(
            p98,
            "随着研究方法的推进，部分学者开始将聚类分析、自然语言处理等数据挖掘方法用于平台帖子与互动分类，从而为复杂用户行为提供量化描述。总体而言，这一脉络已经解释了小红书内容消费发生的条件和基本逻辑，但其分析对象多指向普通用户、内容消费者或单条内容，对高影响力达人作为独立群体的系统描述仍然不足。",
        )
        insert_after(p98, p98b)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p100 = find_by_prefix(paragraphs, "在KOL营销与达人影响力研究方面，现有文献已经从知识网红营销")
        set_paragraph_text(
            p100,
            "在KOL营销与达人影响力研究方面，现有文献已经从知识网红营销、种草经济、平台社区生态和达人信任机制等角度形成了较丰富的讨论。相关研究表明，小红书达人的影响力并不只来自单次传播效果，而是建立在持续内容供给、情感互动、平台文化生产和社群关系管理等多重机制之上。一部分研究强调达人在知识传递、裂变传播和情感交互中的营销价值；另一部分研究则从数字劳动、平台文化生产和社群经济的角度揭示小红书社区的运作逻辑。",
        )
        p100b = clone_with_text(
            p100,
            "与此同时，关于达人促进购买意愿的实证研究也显示，客户体验、信任感与商业转化之间存在稳定联系。总体来看，既有研究较好解释了达人营销为何有效，但多数文献仍将达人视为营销工具或传播节点，对于高影响力达人群体内部的异质性、受众结构差异以及行为模式分化缺乏系统刻画。",
        )
        insert_after(p100, p100b)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p109 = find_by_prefix(paragraphs, "基于上述两个理论，本文将研究问题拆解为三个相互衔接的层面")
        set_paragraph_text(
            p109,
            "基于上述两个理论，本文将研究问题拆解为三个相互衔接的层面：第一层是用户画像特征，用于说明达人服务于何种受众结构；第二层是内容表达与互动方式，用于观察达人如何通过帖子文本、人设表达和评论互动维持影响力；第三层是群体差异识别，即在前两层基础上通过聚类分析归纳不同类型达人。",
        )
        p109b = clone_with_text(
            p109,
            "就理论对应关系而言，意见领袖理论主要帮助理解达人在内容供给、互动维持和商业化表现上的差异，社会认同理论则帮助理解不同受众结构及其互动表达所反映出的身份认同特征。由此，本文形成“用户画像特征—内容与互动表现—达人群体差异”的分析框架。",
        )
        insert_after(p109, p109b)

        # Slight cleanup of chapter 1 technical route wording
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        set_paragraph_text(
            find_by_prefix(paragraphs, "本文采用的技术路线包括描述性统计分析、文本挖掘与K-means聚类分析三部分"),
            "本文的技术路线包括描述性统计分析、文本挖掘与K-means聚类分析三个环节。首先，通过结构化画像数据对高影响力达人及其粉丝群体进行宏观描述；其次，利用帖子、简介与评论文本开展关键词提取、互动类型识别与群体比较；最后，在核心变量标准化的基础上实施K-means聚类，并据此归纳不同类型达人的画像特征与行为差异。",
        )

        # remove any accidental duplicated inserted paragraphs before saving? none
        new_doc_xml = etree.tostring(doc, xml_declaration=True, encoding="UTF-8", standalone="yes")
        with ZipFile(OUTPUT_DOC, "w", compression=ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                if item.filename == "word/document.xml":
                    zout.writestr(item, new_doc_xml)
                else:
                    zout.writestr(item, zin.read(item.filename))

    print(OUTPUT_DOC)


if __name__ == "__main__":
    main()
