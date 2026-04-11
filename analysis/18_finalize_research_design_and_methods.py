#!/usr/bin/env python3
"""
Finalize thesis structure/method visibility:
- reposition chapter 3 as research design
- strengthen 4.3 method wording with TF-IDF/cooccurrence details
- strengthen 4.4 with omitted-variable rationale and outlier statement
- insert extra method table and z-score table
- rewrite conclusion by three research questions
- normalize a few figure/section caption spacings
"""

from __future__ import annotations

import copy
import csv
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
W = "{%s}" % NS["w"]

INPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_终稿润色版.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_最终定稿版.docx")
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


def insert_after(ref: etree._Element, new_el: etree._Element) -> None:
    parent = ref.getparent()
    parent.insert(parent.index(ref) + 1, new_el)


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


def remove_indent_and_center(ppr: etree._Element) -> None:
    for child in list(ppr):
        if child.tag in {f"{W}ind", f"{W}jc"}:
            ppr.remove(child)
    jc = etree.SubElement(ppr, f"{W}jc")
    jc.set(f"{W}val", "center")


def make_table_cell(text: str, run_style: etree._Element | None, bold: bool = False) -> etree._Element:
    tc = etree.Element(f"{W}tc")
    tc_pr = etree.SubElement(tc, f"{W}tcPr")
    tc_w = etree.SubElement(tc_pr, f"{W}tcW")
    tc_w.set(f"{W}w", "1800")
    tc_w.set(f"{W}type", "dxa")
    p = etree.SubElement(tc, f"{W}p")
    ppr = etree.SubElement(p, f"{W}pPr")
    remove_indent_and_center(ppr)
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


def build_table(rows: list[list[str]], style_run: etree._Element | None) -> etree._Element:
    tbl = etree.Element(f"{W}tbl")
    tbl_pr = etree.SubElement(tbl, f"{W}tblPr")
    tbl_style = etree.SubElement(tbl_pr, f"{W}tblStyle")
    tbl_style.set(f"{W}val", "TableGrid")
    tbl_w = etree.SubElement(tbl_pr, f"{W}tblW")
    tbl_w.set(f"{W}w", "0")
    tbl_w.set(f"{W}type", "auto")
    jc = etree.SubElement(tbl_pr, f"{W}jc")
    jc.set(f"{W}val", "center")
    borders = etree.SubElement(tbl_pr, f"{W}tblBorders")
    for edge in ("top", "left", "bottom", "right", "insideH", "insideV"):
        el = etree.SubElement(borders, f"{W}{edge}")
        el.set(f"{W}val", "single")
        el.set(f"{W}sz", "8")
        el.set(f"{W}space", "0")
        el.set(f"{W}color", "000000")

    cols = max(len(r) for r in rows)
    grid = etree.SubElement(tbl, f"{W}tblGrid")
    for _ in range(cols):
        gc = etree.SubElement(grid, f"{W}gridCol")
        gc.set(f"{W}w", "1800")

    for row_idx, row in enumerate(rows):
        tr = etree.SubElement(tbl, f"{W}tr")
        for cell in row:
            tr.append(make_table_cell(cell, style_run, bold=(row_idx == 0)))
    return tbl


def read_rows(name: str) -> list[list[str]]:
    with open(TABLE_DIR / name, newline="", encoding="utf-8-sig") as f:
        return list(csv.reader(f))


def pct(x: str, digits: int = 1) -> str:
    return f"{float(x) * 100:.{digits}f}%"


def main() -> None:
    if not INPUT_DOC.exists():
        raise FileNotFoundError(INPUT_DOC)

    preprocess = read_rows("table_4_2_preprocess_rules.csv")
    cooccurrence = read_rows("table_4_3_cooccurrence_rules.csv")
    zscore = read_rows("table_4_6_cluster_zscore_summary.csv")

    preprocess_co_rows = [["环节", "规则或定义"]]
    preprocess_co_rows.extend(preprocess[1:])
    preprocess_co_rows.extend(cooccurrence[1:])

    z_rows = [["群体", "粉丝数_Z", "Top1年龄段占比_Z", "藏赞比_Z", "评赞比_Z", "商业笔记占比_Z"]]
    names = {"0": "均衡主流型", "1": "收藏转化与商业合作型", "2": "高评论互动型"}
    for row in zscore[1:]:
        z_rows.append([names.get(row[0], row[0])] + [f"{float(v):.3f}" for v in row[1:]])

    with ZipFile(INPUT_DOC) as zin:
        doc = etree.fromstring(zin.read("word/document.xml"))
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # Chapter 3 restructuring
        set_paragraph_text(find_by_exact(paragraphs, "第三章 小红书高影响力生活类达人画像模型与指标体系构建"),
                           "第三章 研究设计：指标体系、文本挖掘思路与聚类方法")
        set_paragraph_text(find_by_exact(paragraphs, "3.1 研究对象界定与样本选取依据"),
                           "3.1 研究对象、样本范围与数据来源")
        set_paragraph_text(find_by_exact(paragraphs, "3.2 多维达人画像特征指标体系构建"),
                           "3.2 指标体系与核心变量定义")
        set_paragraph_text(find_by_exact(paragraphs, "3.3 行为模式的操作性定义与分析框架"),
                           "3.3 行为模式的操作性定义、分析框架与方法路径")

        set_paragraph_text(
            find_by_prefix(paragraphs, "结合当前小红书内容电商与达人营销的发展趋势"),
            "结合当前小红书内容电商与达人营销的发展趋势，本文将研究对象界定为平台中的高影响力生活记录类达人。结构化画像样本来自灰豚平台整理后的达人总表，共3403位达人；其中，经缺失值处理与异常值筛查后，最终用于K-means聚类的有效样本为3371位。文本样本来自DOM爬取形成的帖子与评论主文本池，共包含7375条帖子和94247条评论。上述三类数据共同构成本文的研究基础，也为后续的文本挖掘与聚类分析提供了互相参照的数据来源。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "为了对达人群体进行立体化的画像构建"),
            "为对高影响力达人群体进行立体化描述，本文并未依赖单一流量指标，而是从基础身份、互动表现、商业化水平和粉丝画像四个维度构建指标体系。其中，用户画像是本文的核心切入维度，原因在于达人所面对的受众结构会与其内容表达、互动方式和商业合作倾向形成稳定对应关系。基于这一思路，本文在结构化数据中提取粉丝数、藏赞比、评赞比、商业笔记占比、粉丝年龄结构、粉丝关注焦点等关键字段，并在文本数据中同步观察帖子关键词、简介表达和评论互动特征，从而形成结构化变量与文本变量互相补充的研究设计。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "本文所称“行为模式”"),
            "本文所称“行为模式”，并非泛指达人的所有线上行为，而是特指在特定用户画像约束下所呈现出的内容表达、互动方式与商业化特征的组合。换言之，本文关注的是达人在面向不同受众结构时，如何形成相对稳定的内容供给方式、评论互动风格和商业合作表现，而不是讨论单一行为动作本身。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "基于这一界定，第三章主要负责构建画像模型与指标体系"),
            "基于这一界定，本文的方法路径可以概括为三步：首先，在第三章完成样本界定、指标体系构建与核心变量定义；其次，在第四章前半部分通过描述性统计和文本挖掘，分别展示达人画像特征、内容表达和评论互动结构；最后，在第四章后半部分通过K-means聚类识别不同类型达人，并结合文本结果解释其群体差异。由此，第三章承担“研究设计与变量定义”的功能，第四章承担“数据分析与结果解释”的功能。",
        )

        # Chapter 4 spacing and method detail
        set_paragraph_text(find_by_exact(paragraphs, "4.1数据来源、获取与预处理"),
                           "4.1 数据来源、获取与预处理")
        set_paragraph_text(
            find_by_prefix(paragraphs, "在预处理环节，本文首先对达人简介、帖子标题"),
            "在预处理环节，本文首先对达人简介、帖子标题、帖子正文与评论内容进行统一清洗，去除URL、换行与冗余空格；随后使用jieba进行中文分词，并结合停用词表剔除助词、代词、平台通用词及长度小于2的无效词项。在关键词筛选阶段，本文先依据词频统计获得候选词，再结合TF-IDF权重对解释力较弱、泛化程度过高的词项进行剔除，以保留更能代表内容主题的关键词。在此基础上，分别开展高频词统计、共现网络构建、评论互动类型归纳以及三类群体对比分析，以增强文本挖掘部分的方法完整性与结果解释力。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在用户画像主线下，文本挖掘的作用并不是脱离画像单独讨论内容"),
            "在用户画像主线下，文本挖掘的作用并不是脱离画像单独讨论内容，而是考察不同用户画像所对应的达人在内容表达与评论互动上呈现出的差异。为回应文本挖掘部分方法展示不足的问题，本文将第四章的文本分析进一步细化为内容文本分析与互动文本分析两部分。前者主要对应达人标签、帖子标题与正文，后者主要对应达人简介与评论文本。具体处理流程包括文本清洗、中文分词、停用词过滤、TF-IDF辅助筛词、高频词筛选、共现网络构建以及互动类型归纳，并按三类聚类群体完成映射比较。相关预处理和共现网络构建规则见表4-1a。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "从方法上看，本文先对达人标签、帖子标题和正文进行统一清洗与分词"),
            "从方法上看，本文先对达人标签、帖子标题和正文进行统一清洗与分词，再依据词频与TF-IDF结果筛出具有解释意义的关键词作为候选节点。随后，将两个关键词在同一帖子文本中共同出现一次记为一次共现，并以累计共现次数作为边权重；对权重过低的边进行过滤后，再构建内容共现网络。通过这一方式，内容分析不再停留于简单词频统计，而能够进一步呈现不同内容元素之间的结构性关系。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在达人人设文本层面，简介高频词主要集中于“分享”"),
            "在达人人设文本层面，简介高频词主要集中于“分享”“喜欢”“生活”“快乐”“好物”等表达，显示高影响力达人更倾向于以亲近、日常、可陪伴的方式塑造自身形象，而不是以“专家”“测评”“干货”式的专业权威语言建立影响力。更准确地说，这类达人表现出以“超级分享者”为核心的人设特征，其文本风格强调可亲近性而非专业距离感。",
        )

        # clustering details
        set_paragraph_text(
            find_by_prefix(paragraphs, "在用户画像视角下，聚类分析的目的不是单纯把达人分组"),
            "在用户画像视角下，聚类分析的目的不是单纯把达人分组，而是识别不同受众结构下达人在互动风格和商业化表现上的组合差异。参考Chen（2024）在小红书创作者相关性分析中采用聚类模型的思路，本文选取粉丝数、Top1年龄段占比、藏赞比、评赞比和商业笔记占比五项指标构建K-means聚类模型。其中，粉丝数用于衡量账号的基础体量，Top1年龄段占比用于刻画粉丝年龄结构的集中程度，藏赞比和评赞比分别反映内容被保存与引发讨论的倾向，商业笔记占比则衡量达人的商业合作强度。之所以未将性别比例、地域分布、活跃粉丝占比、水粉占比以及报价/CPE/CPM直接纳入聚类，是因为这些指标与核心变量存在较强相关或量纲差异较大，更适合作为群体解释和结果比较变量，而非直接决定群体划分的入模变量。聚类前先对上述变量进行标准化处理，以消除量纲差异带来的影响，各变量的含义及入模理由见表4-4。",
        )
        # insert outlier paragraph after 164
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p164 = find_by_prefix(paragraphs, "在用户画像视角下，聚类分析的目的不是单纯把达人分组")
        template = find_by_exact(paragraphs, "图6 肘部法则与轮廓系数K值选取")
        outlier_p = clone_with_text(
            template,
            "在异常值处理方面，本文重点检查了评赞比和商业笔记占比等波动较大的指标，对极端异常样本进行剔除后，聚类样本由3403位达人收敛至3371位。剔除前后，各类群体的相对比例和核心特征保持总体稳定，说明本文的聚类结果并非由少数极端样本驱动，而具有一定稳健性。",
        )
        insert_after(p164, outlier_p)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        set_paragraph_text(
            find_by_prefix(paragraphs, "基于K-means聚类结果，本文将3371位高影响力达人划分为三类特征鲜明的群体"),
            "基于K-means聚类结果，本文将3371位高影响力达人划分为三类特征鲜明的群体，相关画像特征概览见表4-5，标准化后的聚类中心比较见表4-6。群体0共有2671位达人，占比最高，整体表现较为均衡，可概括为“均衡主流型”；群体1共有617位达人，在藏赞比和商业笔记占比上明显高于整体均值，可概括为“收藏转化与商业合作型”；群体2共有83位达人，评赞比显著高于其余两类，可概括为“高评论互动型”。三类群体的并存表明，小红书高影响力达人生态并非均质结构，而是存在明显的功能分化。",
        )

        # conclusion & limitations
        set_paragraph_text(
            find_by_prefix(paragraphs, "本研究以小红书平台3403位粉丝数10万以上的生活记录类达人为样本"),
            "本研究以小红书平台3403位粉丝数10万以上的生活记录类达人为样本，并结合7375条帖子与94247条评论的文本数据，围绕三个研究问题展开系统分析。以下结论分别对应达人画像特征、内容与互动特征以及达人群体分化三个层面。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在达人群体画像特征方面，该群体整体呈现女性主导"),
            "第一，在达人画像特征层面，高影响力生活类达人整体呈现女性主导（66.62%）、腰部达人为主（87.4%）、独立运营居多（61.74%）的结构特征，地域上主要集中于浙江、广东、上海、北京等高线省市，核心受众为18至34岁的年轻女性群体。商业化程度总体较为克制，商业笔记占比中位数仅为4.7%，说明该群体在维持内容表达与商业合作之间保持了相对审慎的平衡。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在内容策略与互动机制方面，高影响力达人普遍采用"),
            "第二，在内容与互动特征层面，高影响力达人普遍采用“泛生活+垂直兴趣”的复合内容布局，并通过日常化、可亲近的人设表达维系与粉丝关系。帖子文本呈现出明显的生活方式化、情绪氛围化与场景化特征；评论文本则显示，颜值赞美、拟亲密互动、提问求回应与轻度消费转化构成主要互动类型。与此同时，粉丝活跃高峰主要集中于工作日17至21时，视频内容在报价与互动成本上整体表现相对更优，但不同群体之间仍存在差异化选择空间。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在达人群体类型分化方面，经肘部法则与轮廓系数验证"),
            "第三，在达人群体分化层面，经肘部法则与轮廓系数验证，K=3是当前样本下较为合适的聚类方案。均衡主流型（2671人）在各项指标上整体较为平衡，是平台中的主流高影响力达人；收藏转化与商业合作型（617人）在藏赞比和商业笔记占比上表现更突出；高评论互动型（83人）则表现出更强的评论互动倾向。三类群体的并存说明，小红书高影响力达人并不存在单一成功路径，而是表现出多样化的受众结构、内容表达和互动组合方式。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "尽管本文在数据规模与分析维度上具有一定系统性"),
            "尽管本文在数据规模与分析维度上具有一定系统性，但仍存在若干局限。首先，本文使用的是横截面数据，只能反映达人在特定时间节点的静态状态，无法捕捉其从起步到高影响力阶段的动态演变过程，也无法进一步识别变量变化与后续表现之间的因果关系。其次，研究对象聚焦于小红书单一平台，所得结论是否适用于抖音、快手、微博等其他内容平台，仍需结合不同平台的算法机制、用户结构与内容生态进行进一步验证。再次，本文主要采用描述性统计、文本挖掘与聚类分析方法，尚未进一步引入回归模型或因果识别设计，因此当前结论更适合被理解为对高影响力达人群体特征及其差异的描述性证据。",
        )

        # caption spacing normalization
        for old, new in [
            ("图4高影响力达人粉丝高峰活跃时段与星期分布", "图4 高影响力达人粉丝高峰活跃时段与星期分布"),
            ("第四章 基于多维数据的达人行为特征与画像聚类分析", "第四章 基于多维数据的达人行为特征与画像聚类分析"),
        ]:
            try:
                set_paragraph_text(find_by_exact(paragraphs, old), new)
            except ValueError:
                pass

        # insert extra tables
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        body_style = first_run_style(find_by_prefix(paragraphs, "在预处理环节，本文首先对达人简介"))
        caption_template = find_by_exact(paragraphs, "表4-1 文本样本来源与规模")

        p133 = find_by_prefix(paragraphs, "在用户画像主线下，文本挖掘的作用并不是脱离画像单独讨论内容")
        cap_1a = clone_with_text(caption_template, "表4-1a 文本预处理与共现网络构建规则")
        tbl_1a = build_table(preprocess_co_rows, body_style)
        insert_after(p133, cap_1a)
        insert_after(cap_1a, tbl_1a)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p170 = find_by_prefix(paragraphs, "基于K-means聚类结果，本文将3371位高影响力达人划分为三类特征鲜明的群体")
        cap_46 = clone_with_text(caption_template, "表4-6 三类群体聚类中心Z分数比较")
        tbl_46 = build_table(z_rows, body_style)
        insert_after(find_by_exact(paragraphs, "表4-5 三类群体画像特征概览"), cap_46)
        insert_after(cap_46, tbl_46)

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
