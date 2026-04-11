#!/usr/bin/env python3
"""
Rebuild chapter 4 tables as three-line tables and remove low-confidence tables.

Target:
- keep only four core tables
- convert them to three-line style directly in OOXML
- update body references and table numbering
"""

from __future__ import annotations

import copy
import csv
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
W = "{%s}" % NS["w"]

INPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_最终定稿版.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_三线表终版.docx")
TABLE_DIR = Path("/Users/dxk/xiaohongshu/reports/chapter4_support/tables")


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


def find_by_exact(paragraphs: list[etree._Element], text: str) -> etree._Element:
    for p in paragraphs:
        if paragraph_text(p) == text:
            return p
    raise ValueError(f"Paragraph not found: {text}")


def find_by_prefix(paragraphs: list[etree._Element], prefix: str) -> etree._Element:
    for p in paragraphs:
        if paragraph_text(p).startswith(prefix):
            return p
    raise ValueError(f"Paragraph prefix not found: {prefix}")


def insert_after(ref: etree._Element, new_el: etree._Element) -> None:
    parent = ref.getparent()
    parent.insert(parent.index(ref) + 1, new_el)


def remove_caption_and_following_table(body: etree._Element, caption_text: str) -> None:
    for child in list(body):
        if etree.QName(child).localname == "p" and paragraph_text(child) == caption_text:
            idx = body.index(child)
            body.remove(child)
            if idx < len(body) and etree.QName(body[idx]).localname == "tbl":
                body.remove(body[idx])
            return
    raise ValueError(f"Caption not found: {caption_text}")


def remove_orphan_table_after_paragraph(body: etree._Element, para_prefix: str) -> None:
    for child in list(body):
        if etree.QName(child).localname == "p" and paragraph_text(child).startswith(para_prefix):
            idx = body.index(child) + 1
            while idx < len(body) and etree.QName(body[idx]).localname == "tbl":
                body.remove(body[idx])
            return
    raise ValueError(f"Anchor paragraph not found: {para_prefix}")


def read_rows(name: str) -> list[list[str]]:
    with open(TABLE_DIR / name, newline="", encoding="utf-8-sig") as f:
        return list(csv.reader(f))


def pct(x: str, digits: int = 1) -> str:
    return f"{float(x) * 100:.{digits}f}%"


def build_rows() -> tuple[list[list[str]], list[list[str]], list[list[str]], list[list[str]]]:
    sample = read_rows("table_4_1_sample_sizes.csv")
    inter = read_rows("table_4_4_comment_interaction_definitions.csv")
    cluster_vars = read_rows("table_4_5a_cluster_variable_definitions.csv")
    profile = read_rows("table_4_7_cluster_profile_summary.csv")

    # keep only stable rows
    table1 = [
        ["样本类型", "样本量", "去重对象数", "涉及达人数"],
        sample[1],
        sample[2],
        sample[3],
        sample[4],
    ]

    table2 = [
        ["模块", "类别/环节", "规则说明"],
        ["文本预处理", "文本清洗", "去除换行、URL、冗余空格，保留标题、正文、评论核心文本"],
        ["文本预处理", "中文分词", "使用 jieba 分词，并统一词形"],
        ["文本预处理", "停用词过滤", "过滤助词、代词、平台通用词以及长度小于2的词项"],
        ["文本预处理", "关键词筛选", "结合词频与 TF-IDF 结果保留解释力较强的关键词"],
        ["互动识别", "颜值赞美", inter[1][2]],
        ["互动识别", "拟亲密互动", inter[2][2]],
        ["互动识别", "关怀支持", inter[3][2]],
        ["互动识别", "提问求回应", inter[8][2]],
        ["互动识别", "消费转化", inter[5][2]],
    ]

    table3 = [
        ["变量名称", "变量含义", "入模理由"],
        ["粉丝数", cluster_vars[1][1], cluster_vars[1][2]],
        ["Top1年龄段占比", cluster_vars[2][1], cluster_vars[2][2]],
        ["藏赞比", cluster_vars[3][1], cluster_vars[3][2]],
        ["评赞比", cluster_vars[4][1], cluster_vars[4][2]],
        ["商业笔记占比", cluster_vars[5][1], cluster_vars[5][2]],
        ["未纳入变量", "性别比例、地域分布、活跃粉丝占比、水粉占比、报价/CPE/CPM", "相关性较强或更适合作为结果解释变量，不直接决定聚类分组"],
    ]

    names = {"0": "均衡主流型", "1": "收藏转化与商业合作型", "2": "高评论互动型"}
    table4 = [[
        "群体",
        "规模",
        "粉丝数中位数",
        "Top1年龄段占比",
        "藏赞比",
        "评赞比",
        "商业笔记占比",
    ]]
    for row in profile[1:]:
        table4.append([
            names.get(row[0], row[0]),
            row[9],
            str(int(float(row[1]))),
            pct(row[2]),
            f"{float(row[3]):.3f}",
            f"{float(row[4]):.3f}",
            pct(row[5]),
        ])
    return table1, table2, table3, table4


def remove_indent(ppr: etree._Element) -> None:
    for child in list(ppr):
        if child.tag in {f"{W}ind", f"{W}jc"}:
            ppr.remove(child)


def apply_tc_border(tc: etree._Element, edge: str, val: str = "single", sz: str = "8") -> None:
    tc_pr = tc.find("./w:tcPr", NS)
    if tc_pr is None:
        tc_pr = etree.SubElement(tc, f"{W}tcPr")
    tc_borders = tc_pr.find("./w:tcBorders", NS)
    if tc_borders is None:
        tc_borders = etree.SubElement(tc_pr, f"{W}tcBorders")
    el = etree.SubElement(tc_borders, f"{W}{edge}")
    el.set(f"{W}val", val)
    if val != "nil":
        el.set(f"{W}sz", sz)
        el.set(f"{W}space", "0")
        el.set(f"{W}color", "000000")


def make_cell(text: str, run_style: etree._Element | None, align: str, bold: bool = False) -> etree._Element:
    tc = etree.Element(f"{W}tc")
    tc_pr = etree.SubElement(tc, f"{W}tcPr")
    tc_w = etree.SubElement(tc_pr, f"{W}tcW")
    tc_w.set(f"{W}w", "2400")
    tc_w.set(f"{W}type", "dxa")
    p = etree.SubElement(tc, f"{W}p")
    ppr = etree.SubElement(p, f"{W}pPr")
    remove_indent(ppr)
    jc = etree.SubElement(ppr, f"{W}jc")
    jc.set(f"{W}val", align)
    r = etree.SubElement(p, f"{W}r")
    if run_style is not None:
        rpr = copy.deepcopy(run_style)
        if bold:
            if rpr.find("./w:b", NS) is None:
                etree.SubElement(rpr, f"{W}b")
            if rpr.find("./w:bCs", NS) is None:
                etree.SubElement(rpr, f"{W}bCs")
        r.append(rpr)
    t = etree.SubElement(r, f"{W}t")
    t.text = text
    return tc


def build_three_line_table(rows: list[list[str]], run_style: etree._Element | None) -> etree._Element:
    cols = max(len(r) for r in rows)
    tbl = etree.Element(f"{W}tbl")
    tbl_pr = etree.SubElement(tbl, f"{W}tblPr")
    tbl_w = etree.SubElement(tbl_pr, f"{W}tblW")
    tbl_w.set(f"{W}w", "0")
    tbl_w.set(f"{W}type", "auto")
    jc = etree.SubElement(tbl_pr, f"{W}jc")
    jc.set(f"{W}val", "center")
    look = etree.SubElement(tbl_pr, f"{W}tblLook")
    look.set(f"{W}val", "0000")

    borders = etree.SubElement(tbl_pr, f"{W}tblBorders")
    top = etree.SubElement(borders, f"{W}top")
    top.set(f"{W}val", "single")
    top.set(f"{W}sz", "8")
    top.set(f"{W}space", "0")
    top.set(f"{W}color", "000000")
    bottom = etree.SubElement(borders, f"{W}bottom")
    bottom.set(f"{W}val", "single")
    bottom.set(f"{W}sz", "8")
    bottom.set(f"{W}space", "0")
    bottom.set(f"{W}color", "000000")
    for edge in ("left", "right", "insideH", "insideV"):
        e = etree.SubElement(borders, f"{W}{edge}")
        e.set(f"{W}val", "nil")

    grid = etree.SubElement(tbl, f"{W}tblGrid")
    for _ in range(cols):
        gc = etree.SubElement(grid, f"{W}gridCol")
        gc.set(f"{W}w", "2400")

    for r_idx, row in enumerate(rows):
        tr = etree.SubElement(tbl, f"{W}tr")
        for c_idx in range(cols):
            text = row[c_idx] if c_idx < len(row) else ""
            is_header = r_idx == 0
            is_text_col = c_idx in (1, 2) and cols >= 3
            tc = make_cell(text, run_style, align="left" if is_text_col else "center", bold=is_header)
            # remove all borders on cell
            for edge in ("top", "left", "bottom", "right"):
                apply_tc_border(tc, edge, val="nil")
            # header underline
            if is_header:
                apply_tc_border(tc, "bottom", val="single", sz="6")
            tr.append(tc)
    return tbl


def main() -> None:
    if not INPUT_DOC.exists():
        raise FileNotFoundError(INPUT_DOC)

    table1, table2, table3, table4 = build_rows()

    with ZipFile(INPUT_DOC) as zin:
        doc = etree.fromstring(zin.read("word/document.xml"))
        body = doc.xpath("//w:body", namespaces=NS)[0]
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # Update text references first
        set_paragraph_text(
            find_by_prefix(paragraphs, "在用户画像主线下，文本挖掘的作用并不是脱离画像单独讨论内容"),
            "在用户画像主线下，文本挖掘的作用并不是脱离画像单独讨论内容，而是考察不同用户画像所对应的达人在内容表达与评论互动上呈现出的差异。为回应文本挖掘部分方法展示不足的问题，本文将第四章的文本分析进一步细化为内容文本分析与互动文本分析两部分。前者主要对应达人标签、帖子标题与正文，后者主要对应达人简介与评论文本。具体处理流程包括文本清洗、中文分词、停用词过滤、TF-IDF辅助筛词、高频词筛选、共现网络构建以及互动类型归纳，并按三类聚类群体完成映射比较。相关处理规则见表4-2。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在互动文本层面，本文进一步对94126条已对齐评论进行归纳分析"),
            "在互动文本层面，本文进一步对94126条已对齐评论进行归纳分析。为提高评论分析的可解释性，本文依据高频表达、语义模式与互动意图，将评论划分为颜值赞美、拟亲密互动、关怀支持、提问求回应、消费转化等类别，具体识别规则见表4-2。结果显示，三类群体的评论都以泛互动类表达为主，但仍存在明显差异：群体0的颜值赞美与拟亲密互动相对更多，群体1更常出现提问求回应与消费转化表达，群体2整体评论结构更分散。评论高频词中反复出现“宝宝”“姐姐”“哈哈哈”“怎么”“好看”等称呼和反应词，说明评论区的核心并不是理性讨论，而是拟亲密关系、颜值赞美、求回应和轻度消费转化共同构成的互动机制。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在用户画像视角下，聚类分析的目的不是单纯把达人分组"),
            "在用户画像视角下，聚类分析的目的不是单纯把达人分组，而是识别不同受众结构下达人在互动风格和商业化表现上的组合差异。参考Chen（2024）在小红书创作者相关性分析中采用聚类模型的思路，本文选取粉丝数、Top1年龄段占比、藏赞比、评赞比和商业笔记占比五项指标构建K-means聚类模型。其中，粉丝数用于衡量账号的基础体量，Top1年龄段占比用于刻画粉丝年龄结构的集中程度，藏赞比和评赞比分别反映内容被保存与引发讨论的倾向，商业笔记占比则衡量达人的商业合作强度。之所以未将性别比例、地域分布、活跃粉丝占比、水粉占比以及报价/CPE/CPM直接纳入聚类，是因为这些指标与核心变量存在较强相关或量纲差异较大，更适合作为群体解释和结果比较变量，而非直接决定聚类分组的入模变量。聚类变量及其入模理由见表4-3。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "基于K-means聚类结果，本文将3371位高影响力达人划分为三类特征鲜明的群体"),
            "基于K-means聚类结果，本文将3371位高影响力达人划分为三类特征鲜明的群体，相关画像特征概览见表4-4。群体0共有2671位达人，占比最高，整体表现较为均衡，可概括为“均衡主流型”；群体1共有617位达人，在藏赞比和商业笔记占比上明显高于整体均值，可概括为“收藏转化与商业合作型”；群体2共有83位达人，评赞比显著高于其余两类，可概括为“高评论互动型”。三类群体的并存表明，小红书高影响力达人生态并非均质结构，而是存在明显的功能分化。",
        )

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        caption_template = find_by_exact(paragraphs, "表4-1 文本样本来源与规模")
        body_style = first_run_style(find_by_prefix(paragraphs, "在预处理环节，本文首先对达人简介"))

        # remove existing 7 table blocks
        for caption in [
            "表4-1 文本样本来源与规模",
            "表4-1a 文本预处理与共现网络构建规则",
            "表4-2 评论互动类型识别规则",
            "表4-3 三类群体评论互动类型占比比较",
            "表4-4 聚类变量定义与入模理由",
            "表4-5 三类群体画像特征概览",
            "表4-6 三类群体聚类中心Z分数比较",
        ]:
            remove_caption_and_following_table(body, caption)
        # when old captions were adjacent, the old data tables could remain orphaned
        remove_orphan_table_after_paragraph(body, "基于K-means聚类结果，本文将3371位高影响力达人划分为三类特征鲜明的群体")

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # insert 4 new tables
        p130 = find_by_prefix(paragraphs, "在预处理环节，本文首先对达人简介")
        cap1 = clone_with_text(caption_template, "表4-1 数据来源与样本规模")
        tbl1 = build_three_line_table(table1, body_style)
        insert_after(p130, cap1)
        insert_after(cap1, tbl1)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p136 = find_by_prefix(paragraphs, "在用户画像主线下，文本挖掘的作用并不是脱离画像单独讨论内容")
        cap2 = clone_with_text(caption_template, "表4-2 文本预处理与评论互动识别规则")
        tbl2 = build_three_line_table(table2, body_style)
        insert_after(p136, cap2)
        insert_after(cap2, tbl2)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p165 = find_by_prefix(paragraphs, "在用户画像视角下，聚类分析的目的不是单纯把达人分组")
        cap3 = clone_with_text(caption_template, "表4-3 聚类变量定义与入模理由")
        tbl3 = build_three_line_table(table3, body_style)
        insert_after(find_by_prefix(paragraphs, "在异常值处理方面，本文重点检查了评赞比"), cap3)
        insert_after(cap3, tbl3)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p172 = find_by_prefix(paragraphs, "基于K-means聚类结果，本文将3371位高影响力达人划分为三类特征鲜明的群体")
        cap4 = clone_with_text(caption_template, "表4-4 三类群体画像特征概览")
        tbl4 = build_three_line_table(table4, body_style)
        insert_after(p172, cap4)
        insert_after(cap4, tbl4)

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
