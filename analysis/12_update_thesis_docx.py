#!/usr/bin/env python3
"""
Create a chapter-4 revised DOCX copy while preserving existing paragraph formatting.

Strategy:
- start from the original DOCX
- replace only selected chapter-4 paragraphs and one subheading
- clone existing paragraph/run formatting from neighboring paragraphs
- write to a new DOCX copy, leaving the original untouched
"""

from __future__ import annotations

import copy
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
W = "{%s}" % NS["w"]

ORIGINAL_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_第四章改稿.docx")


REPLACEMENTS = {
    "4.1数据来源、获取与预处理": [
        "本研究的数据来源分为达人画像数据与文本数据两部分。达人画像数据来自灰豚平台整理后的结构化总表，共计3403位高影响力生活类达人；聚类分析样本采用带有群体标签_k3的达人聚类结果表，共计3371位达人。文本数据采用DOM爬取形成的主文本池，共包含7375条帖子文本与94247条评论文本。将文本数据与聚类结果按creator_id进行对齐后，最终获得7360条可用于群体比较的帖子与94126条评论。",
        "在预处理环节，本文首先对达人简介、帖子标题、帖子正文与评论内容进行统一清洗，去除URL、换行与冗余空格；随后使用jieba进行中文分词，并结合停用词表剔除助词、代词、平台通用词及长度小于2的无效词项。在此基础上，分别开展高频词统计、共现网络构建、评论互动类型归纳以及三类群体对比分析，以增强文本挖掘部分的方法完整性与结果解释力。",
    ],
    "4.3 基于文本挖掘的达人内容策略与人设特征解析": [
        "为回应文本挖掘部分方法展示不足的问题，本文将第四章的文本分析进一步细化为内容文本分析与互动文本分析两部分。前者主要对应达人标签、帖子标题与正文，后者主要对应达人简介与评论文本。具体处理流程包括文本清洗、中文分词、停用词过滤、高频词筛选、共现网络构建以及互动类型归纳，并按三类聚类群体完成映射比较。"
    ],
    "4.3.1 泛生活与垂直兴趣交织的内容生态网络解析": [
        "从方法上看，本文先对达人标签、帖子标题和正文进行统一清洗与分词，再将高频关键词作为网络节点，将两个关键词在同一帖子文本中共同出现一次记为一次共现，并以累计共现次数作为边权重。通过这一方式，内容分析不再停留于简单词频统计，而能够进一步呈现不同内容元素之间的结构性关系。",
        "从结果上看，帖子文本的高频词主要围绕“春天”“氛围”“一天”“旅行”“世界”等日常化与审美化表达展开，说明高影响力达人并不主要依赖高信息密度内容，而更依赖生活方式化、情绪氛围化和场景化表达来维持关注度。按三类群体比较，群体0更集中于“春天”“氛围”“旅行”等泛生活表达，群体1更常出现“推荐”“护肤”“直接”等种草导向词，群体2则更多出现“一天”“孩子”“广州”“碎片”等更具个人生活切片色彩的表达。",
        "结合图1和图2可以进一步发现，达人内容供给与粉丝兴趣焦点之间存在较高的一致性。无论从达人标签共现关系还是从文本关键词结构看，小红书高影响力生活类达人都呈现出“泛生活底盘+垂直兴趣补充”的内容组织方式，这也为后续的人设建构与互动差异分析提供了文本基础。",
        "因此，内容生态层面的文本挖掘结果表明，高影响力达人并不是围绕单一赛道持续输出，而是在泛生活语境中嵌入护肤、旅行、成长、情绪表达等不同主题，由此形成既具广泛触达能力、又能承接特定受众兴趣的内容网络。",
    ],
    "4.3.2 信任机制下的超级分享者人设文本解码": [
        "4.3.2 信任机制下的人设与评论互动文本解码"
    ],
    "4.3.2 信任机制下的人设与评论互动文本解码": [
        "在达人人设文本层面，简介高频词主要集中于“分享”“喜欢”“生活”“快乐”“好物”等表达，显示高影响力达人更倾向于以亲近、日常、可陪伴的方式塑造自身形象，而不是以“专家”“测评”“干货”式的专业权威语言建立影响力。这意味着小红书生活类达人更像是以“超级分享者”的身份与受众建立信任关系，人设文本的核心逻辑是可亲近性而非专业距离感。",
        "在互动文本层面，本文进一步对94126条已对齐评论进行归纳分析。结果显示，三类群体的评论都以泛互动类表达为主，但仍存在明显差异：群体0的颜值赞美与拟亲密互动占比相对更高，群体1的提问求回应与消费转化占比更高，群体2整体评论结构更分散、集中度较低。评论高频词中反复出现“宝宝”“姐姐”“哈哈哈”“怎么”“好看”等称呼和反应词，说明评论区的核心并不是理性讨论，而是拟亲密关系、颜值赞美、求回应和轻度消费转化共同构成的互动机制。",
    ],
    "4.4.1 聚类特征变量的选择与模型设定": [
        "参考Chen（2024）在小红书创作者相关性分析中采用聚类模型的思路，本文选取粉丝数、Top1年龄段占比、藏赞比、评赞比和商业笔记占比五项指标构建K-means聚类模型。其中，粉丝数用于衡量账号的基础体量，Top1年龄段占比用于刻画粉丝年龄结构的集中程度，藏赞比和评赞比分别反映内容被保存与引发讨论的倾向，商业笔记占比则衡量达人的商业合作强度。聚类前先对上述变量进行标准化处理，以消除量纲差异带来的影响。",
        "为确定最优聚类数K，本文综合使用肘部法则与轮廓系数，对K=2至K=8进行逐一比较。结果显示，当K=3时，轮廓系数达到0.354，为当前比较范围内的最高值，同时SSE曲线在K=2至K=3之间出现明显拐点，说明三类划分在统计表现与结果解释之间取得了较好平衡。因此，本文最终采用K=3的聚类方案对3371位达人进行群体划分。",
    ],
    "4.4.2 三类高影响力达人群体的画像特征解析": [
        "基于K-means聚类结果，本文将3371位高影响力达人划分为三类特征鲜明的群体。群体0共有2671位达人，占比最高，整体表现较为均衡，可概括为“均衡主流型”；群体1共有617位达人，在藏赞比和商业笔记占比上明显高于整体均值，可概括为“收藏转化与商业合作型”；群体2共有83位达人，评赞比显著高于其余两类，可概括为“高评论互动型”。三类群体的并存表明，小红书高影响力达人生态并非均质结构，而是存在明显的功能分化。",
        "进一步结合文本挖掘结果可以发现，不同群体在内容与互动机制上也存在差异。均衡主流型达人更依赖泛生活、氛围化与审美化表达；收藏转化与商业合作型达人更容易承接推荐、护肤、好物等种草导向内容；高评论互动型达人则更容易形成强讨论度但规模较小的互动场域。由此可见，达人画像变量、内容策略与评论互动之间并非彼此割裂，而是共同构成了高影响力达人的形成机制。",
    ],
}


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

    # remove extra existing non-caption paragraphs only when they are normal body text;
    # keep later figure/caption paragraphs for safety.
    for p in target_paras[1:]:
        txt = paragraph_text(p)
        if txt.startswith("图") or txt == "":
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


def main() -> None:
    if not ORIGINAL_DOC.exists():
        raise FileNotFoundError(ORIGINAL_DOC)

    with ZipFile(ORIGINAL_DOC) as zin:
        xml_bytes = zin.read("word/document.xml")
        doc = etree.fromstring(xml_bytes)
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # 4.1
        replace_section_body(
            paragraphs,
            "4.1数据来源、获取与预处理",
            "4.2 达人群体基础分布与宏观统计特征分析",
            REPLACEMENTS["4.1数据来源、获取与预处理"],
        )

        # 4.3 intro paragraph under heading
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p_431 = find_paragraph_by_exact_text(paragraphs, "4.3.1 泛生活与垂直兴趣交织的内容生态网络解析")
        intro_template = first_body_paragraph_between(
            paragraphs,
            "4.3.1 泛生活与垂直兴趣交织的内容生态网络解析",
            "4.3.2 信任机制下的超级分享者人设文本解码",
        )
        intro_p = clone_body_paragraph(intro_template, REPLACEMENTS["4.3 基于文本挖掘的达人内容策略与人设特征解析"][0])
        # insert right before 4.3.1
        p_431.getparent().insert(p_431.getparent().index(p_431), intro_p)

        # 4.3.1 body
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        replace_section_body(
            paragraphs,
            "4.3.1 泛生活与垂直兴趣交织的内容生态网络解析",
            "4.3.2 信任机制下的超级分享者人设文本解码",
            REPLACEMENTS["4.3.1 泛生活与垂直兴趣交织的内容生态网络解析"],
        )

        # update 4.3.2 heading text
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        old_432_heading = find_paragraph_by_exact_text(paragraphs, "4.3.2 信任机制下的超级分享者人设文本解码")
        set_paragraph_text(old_432_heading, REPLACEMENTS["4.3.2 信任机制下的超级分享者人设文本解码"][0])

        # 4.3.2 body
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        replace_section_body(
            paragraphs,
            "4.3.2 信任机制下的人设与评论互动文本解码",
            "4.3.3 内容发布时机",
            REPLACEMENTS["4.3.2 信任机制下的人设与评论互动文本解码"],
        )

        # 4.4.1 body
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        replace_section_body(
            paragraphs,
            "4.4.1 聚类特征变量的选择与模型设定",
            "4.4.2 三类高影响力达人群体的画像特征解析",
            REPLACEMENTS["4.4.1 聚类特征变量的选择与模型设定"],
        )

        # 4.4.2 body
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        replace_section_body(
            paragraphs,
            "4.4.2 三类高影响力达人群体的画像特征解析",
            "第五章 结论与展望",
            REPLACEMENTS["4.4.2 三类高影响力达人群体的画像特征解析"],
        )

        new_xml = etree.tostring(doc, xml_declaration=True, encoding="UTF-8", standalone="yes")

        with ZipFile(OUTPUT_DOC, "w", compression=ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = new_xml if item.filename == "word/document.xml" else zin.read(item.filename)
                zout.writestr(item, data)

    print(OUTPUT_DOC)


if __name__ == "__main__":
    main()
