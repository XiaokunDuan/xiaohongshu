#!/usr/bin/env python3
"""
Final cleanup pass on the unified thesis DOCX.
"""

from __future__ import annotations

import copy
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
W = "{%s}" % NS["w"]

INPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_统一总版.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_最终提交版.docx")


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


def remove_empty_paragraph_before(body: etree._Element, anchor_text: str) -> None:
    for child in list(body):
        if etree.QName(child).localname == "p" and paragraph_text(child) == anchor_text:
            idx = body.index(child)
            if idx > 0 and etree.QName(body[idx - 1]).localname == "p" and not paragraph_text(body[idx - 1]):
                body.remove(body[idx - 1])
            return


def set_ref_style_like_cn(cn_ref_ppr: etree._Element | None, paragraph: etree._Element) -> None:
    if cn_ref_ppr is None:
        return
    new_ppr = copy.deepcopy(cn_ref_ppr)
    # keep reference paragraph style and spacing, but drop paragraph-level rPr to preserve English run fonts
    rpr = new_ppr.find("./w:rPr", NS)
    if rpr is not None:
        new_ppr.remove(rpr)
    old = paragraph.find("./w:pPr", NS)
    if old is not None:
        paragraph.remove(old)
    paragraph.insert(0, new_ppr)


def main() -> None:
    if not INPUT_DOC.exists():
        raise FileNotFoundError(INPUT_DOC)

    with ZipFile(INPUT_DOC) as zin:
        doc = etree.fromstring(zin.read("word/document.xml"))
        body = doc.xpath("//w:body", namespaces=NS)[0]
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # First chapter: make research questions less like loose bullets
        set_paragraph_text(find_by_prefix(paragraphs, "通过以上分析，拟解决以下关键研究问题："),
                           "围绕研究目标，本文主要从以下三个层面展开分析。")
        set_paragraph_text(find_by_exact(paragraphs, "小红书高影响力生活记录类达人表现出怎样的画像特征？"),
                           "第一，识别小红书高影响力生活记录类达人的整体画像特征，包括其基础属性、粉丝结构与商业化表现。")
        set_paragraph_text(find_by_exact(paragraphs, "这些成功的达人采取了怎样的内容策略和人设构建来获取粉丝关注？"),
                           "第二，分析高影响力达人在内容表达、人设塑造与评论互动中的主要策略特征。")
        set_paragraph_text(find_by_exact(paragraphs, "基于粉丝画像特征与互动指标，达人群体可以聚类为哪些差异化类别？其画像差异如何体现在内容策略与商业模式上？"),
                           "第三，基于粉丝画像特征与互动指标，对达人群体进行聚类划分，并比较不同群体在内容策略与商业模式上的差异。")

        # Chapter 3 / 4.1 reduce repetition further
        set_paragraph_text(
            find_by_prefix(paragraphs, "结合当前小红书内容电商与达人营销的发展趋势"),
            "本文将研究对象界定为小红书平台中的高影响力生活记录类达人。结构化画像样本来自灰豚平台整理后的达人总表，共3403位达人；经缺失值处理与异常值筛查后，最终用于K-means聚类的有效样本为3371位。帖子与评论文本样本来自DOM爬取形成的主文本池，共包含7375条帖子和94247条评论。上述三类数据共同构成本文的研究基础。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "为对高影响力达人群体进行立体化描述，本文并未依赖单一流量指标"),
            "为对高影响力达人群体进行立体化描述，本文并未依赖单一流量指标，而是从基础身份、互动表现、商业化水平和粉丝画像四个维度构建指标体系。其中，用户画像是本文的核心切入维度，因为达人所面对的受众结构会与其内容表达、互动方式和商业合作倾向形成相对稳定的对应关系。基于这一思路，本文在结构化数据中提取粉丝数、藏赞比、评赞比、商业笔记占比、粉丝年龄结构和粉丝关注焦点等关键字段，并在文本数据中同步观察帖子关键词、简介表达和评论互动特征，从而形成结构化变量与文本变量互相补充的研究设计。",
        )
        p117 = find_by_prefix(paragraphs, "为对高影响力达人群体进行立体化描述，本文并未依赖单一流量指标")
        p117b = clone_with_text(
            p117,
            "在聚类变量选择上，本文优先保留能够同时反映账号体量、受众年龄结构、内容保存价值、讨论互动倾向与商业合作强度的指标，即粉丝数、Top1年龄段占比、藏赞比、评赞比和商业笔记占比。性别比例、地域分布、活跃粉丝占比、水粉占比以及报价、CPE、CPM等变量虽然具有解释价值，但更适合作为群体结果比较变量，而不直接作为决定分组边界的入模变量。",
        )
        insert_after(p117, p117b)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        set_paragraph_text(
            find_by_prefix(paragraphs, "在第三章完成研究对象、样本范围与变量定义说明的基础上"),
            "在第三章已完成研究对象、变量体系和方法路径说明的基础上，本节仅对第四章实际使用的数据口径和处理步骤作简要交代。第四章所用达人画像样本为3403位达人、聚类样本为3371位达人、文本样本为7375条帖子与94247条评论，对齐后的群体比较样本为7360条帖子与94126条评论，具体见表4-1。",
        )

        # Method examples in正文
        set_paragraph_text(
            find_by_prefix(paragraphs, "在互动文本层面，本文进一步对94126条已对齐评论进行归纳分析"),
            "在互动文本层面，本文进一步对94126条已对齐评论进行归纳分析。为提高评论分析的可解释性，本文依据高频表达、语义模式与互动意图，将评论划分为颜值赞美、拟亲密互动、关怀支持、提问求回应、消费转化等类别，具体识别规则见表4-2。例如，“好美”“绝美”“好看”等表达被归入颜值赞美，“老婆”“宝宝”“想你”等表达被归入拟亲密互动，“怎么”“什么时候”“求回复”等表达被归入提问求回应，“同款”“链接”“下单”等表达则归入消费转化。结果显示，三类群体的评论都以泛互动类表达为主，但仍存在明显差异：群体0的颜值赞美与拟亲密互动相对更多，群体1更常出现提问求回应与消费转化表达，群体2整体评论结构更分散。",
        )

        # 5.3 add theory/practice implications
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p182 = find_by_exact(paragraphs, "参考文献")
        h53 = clone_with_text(find_by_exact(paragraphs, "5.2 研究局限性"), "5.3 理论与实践启示")
        b53 = clone_with_text(
            find_by_prefix(paragraphs, "尽管本文在数据规模与分析维度上具有一定系统性"),
            "从理论层面看，本文在意见领袖理论和社会认同理论的基础上，进一步提供了高影响力达人差异化路径的描述性证据：不同达人并非仅在流量规模上存在差异，更在受众结构、内容表达和互动方式的组合上表现出不同的影响力实现方式。从实践层面看，研究结果可为达人自身的内容定位、品牌方的达人筛选以及平台对创作者生态的分层理解提供参考。",
        )
        body.insert(body.index(p182), h53)
        body.insert(body.index(p182), b53)

        # reference cleanup
        remove_empty_paragraph_before(body, "参考文献")
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        cn_ref_ppr = find_by_prefix(paragraphs, "刘洋，段宇杰，张鑫").find("./w:pPr", NS)
        for prefix in [
            "Chen, W. 2024.",
            "Xie, G. & Wang, X. 2025.",
            "Chen, N. & Yang, Y. 2023.",
            "Fang, Z. 2024.",
            "Song, R. & Lu, H. 2021.",
            "Li, S. et al. 2022.",
        ]:
            set_ref_style_like_cn(cn_ref_ppr, find_by_prefix(paragraphs, prefix))

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
