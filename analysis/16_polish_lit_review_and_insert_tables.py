#!/usr/bin/env python3
"""
Further strengthen the thesis by:

1. rewriting chapter 2 literature review/theory sections to reduce repetition
   and make the structure closer to the referenced Keep thesis;
2. inserting several key chapter 4 tables directly into the DOCX so that
   workload is visible in the final Word document.

The script preserves formatting by cloning nearby paragraphs/runs from the
existing document and only operating on a copied DOCX.
"""

from __future__ import annotations

import copy
import csv
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
}
W = "{%s}" % NS["w"]

INPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_结构强化版.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_综述与表格强化版.docx")
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
    if text.startswith(" ") or text.endswith(" "):
        t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
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


def remove_indent_and_center(ppr: etree._Element) -> etree._Element:
    for child in list(ppr):
        if child.tag in {f"{W}ind", f"{W}jc"}:
            ppr.remove(child)
    jc = etree.SubElement(ppr, f"{W}jc")
    jc.set(f"{W}val", "center")
    return ppr


def make_table_cell(text: str, run_style: etree._Element | None, bold: bool = False) -> etree._Element:
    tc = etree.Element(f"{W}tc")
    tc_pr = etree.SubElement(tc, f"{W}tcPr")
    tc_w = etree.SubElement(tc_pr, f"{W}tcW")
    tc_w.set(f"{W}w", "2400")
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
    elif bold:
        rpr = etree.SubElement(r, f"{W}rPr")
        etree.SubElement(rpr, f"{W}b")
        etree.SubElement(rpr, f"{W}bCs")
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
    tbl_jc = etree.SubElement(tbl_pr, f"{W}jc")
    tbl_jc.set(f"{W}val", "center")

    borders = etree.SubElement(tbl_pr, f"{W}tblBorders")
    for edge in ("top", "left", "bottom", "right", "insideH", "insideV"):
        el = etree.SubElement(borders, f"{W}{edge}")
        el.set(f"{W}val", "single")
        el.set(f"{W}sz", "8")
        el.set(f"{W}space", "0")
        el.set(f"{W}color", "000000")

    grid = etree.SubElement(tbl, f"{W}tblGrid")
    cols = max(len(r) for r in rows)
    for _ in range(cols):
        gc = etree.SubElement(grid, f"{W}gridCol")
        gc.set(f"{W}w", "2400")

    for row_idx, row in enumerate(rows):
        tr = etree.SubElement(tbl, f"{W}tr")
        for cell in row:
            tr.append(make_table_cell(cell, style_run, bold=(row_idx == 0)))
    return tbl


def read_csv_rows(name: str) -> list[list[str]]:
    path = TABLE_DIR / name
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.reader(f))


def pct(v: str, digits: int = 1) -> str:
    return f"{float(v) * 100:.{digits}f}%"


def build_comment_compare_rows() -> list[list[str]]:
    rows = read_csv_rows("table_4_9_group_comment_interaction_compare.csv")
    out = [["互动类型", "均衡主流型", "收藏转化与商业合作型", "高评论互动型"]]
    for row in rows[1:]:
        out.append([row[0], pct(row[1]), pct(row[2]), pct(row[3])])
    return out


def build_profile_rows() -> list[list[str]]:
    src = read_csv_rows("table_4_7_cluster_profile_summary.csv")
    name_map = {"0": "均衡主流型", "1": "收藏转化与商业合作型", "2": "高评论互动型"}
    out = [[
        "群体",
        "群体规模",
        "粉丝数中位数",
        "Top1年龄段占比",
        "藏赞比",
        "评赞比",
        "商业笔记占比",
        "视频报价中位数(元)",
    ]]
    for row in src[1:]:
        out.append([
            name_map.get(row[0], row[0]),
            row[9],
            str(int(float(row[1]))),
            pct(row[2]),
            f"{float(row[3]):.3f}",
            f"{float(row[4]):.3f}",
            pct(row[5]),
            str(int(float(row[6]))),
        ])
    return out


def main() -> None:
    if not INPUT_DOC.exists():
        raise FileNotFoundError(INPUT_DOC)

    sample_rows = read_csv_rows("table_4_1_sample_sizes.csv")
    interaction_def_rows = read_csv_rows("table_4_4_comment_interaction_definitions.csv")
    cluster_var_rows = read_csv_rows("table_4_5a_cluster_variable_definitions.csv")
    comment_compare_rows = build_comment_compare_rows()
    profile_rows = build_profile_rows()

    with ZipFile(INPUT_DOC) as zin:
        doc = etree.fromstring(zin.read("word/document.xml"))
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # Chapter 2 refinement
        set_paragraph_text(
            find_by_exact(paragraphs, "2.2.3 文献总结"),
            "2.2.3 文献述评",
        )
        set_paragraph_text(
            find_by_exact(paragraphs, "2.3.4 文献总结"),
            "2.3.4 理论支撑与研究启示",
        )

        set_paragraph_text(
            find_by_prefix(paragraphs, "近年来，以小红书为主的内容电商平台发展壮大"),
            "围绕小红书等内容社区，现有研究主要从文本与用户行为、KOL营销与影响力机制两个脉络展开。为避免文献综述停留于材料堆砌，本文按照上述两条线索进行梳理，并在此基础上提炼与研究主题直接相关的理论基础与研究缺口。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "从内容消费的角度，研究者主要关注KOL对于营销的影响"),
            "在文本与用户行为研究方面，既有文献主要关注UGC内容如何影响用户的信息获取、情绪卷入与消费决策。一类研究从社交营销与信任机制出发，指出社会互动连接、共同愿景、信息质量和服务质量会通过提升信任与社交意愿，进而影响消费意愿；另一类研究则从用户动机和平台网络结构切入，发现小红书用户的信息分享主要围绕生活经验、购物和食品等主题，并通过弱关系传播、结构洞位置和社群认同形成内容扩散。随着研究方法的推进，部分学者开始将聚类分析、自然语言处理等数据挖掘方法用于平台帖子与互动分类，从而为复杂用户行为提供量化描述。总体而言，这一脉络已经解释了小红书内容消费发生的条件和基本逻辑，但其分析对象多指向普通用户、内容消费者或单条内容，对高影响力达人作为独立群体的系统描述仍然不足。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "与上节关注消费者行为不同，本节聚焦于KOL自身的营销策略与影响力形成机制"),
            "在KOL营销与达人影响力研究方面，现有文献已经从知识网红营销、种草经济、平台社区生态和达人信任机制等角度形成了较丰富的讨论。相关研究表明，小红书达人的影响力并不只来自单次传播效果，而是建立在持续内容供给、情感互动、平台文化生产和社群关系管理等多重机制之上。一部分研究强调达人在知识传递、裂变传播和情感交互中的营销价值；另一部分研究则从数字劳动、平台文化生产和社群经济的角度揭示小红书社区的运作逻辑。与此同时，关于达人促进购买意愿的实证研究也显示，客户体验、信任感与商业转化之间存在稳定联系。总体来看，既有研究较好解释了达人营销为何有效，但多数文献仍将达人视为营销工具或传播节点，对于高影响力达人群体内部的异质性、受众结构差异以及行为模式分化缺乏系统刻画。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "目前文献对于小红书平台已从营销策略"),
            "综合上述文献可以看到，现有研究已经较充分地讨论了小红书平台的内容消费、营销逻辑与信任生成机制，但仍存在三点不足：其一，研究对象更多聚焦普通用户、品牌传播或单次营销效果，对高影响力达人群体本身缺乏系统画像；其二，已有研究虽涉及文本分析和算法应用，但较少将文本挖掘结果与达人群体划分结合起来；其三，从供给侧视角同时考察受众画像、内容表达、互动方式与商业化特征的研究仍然有限。基于此，本文以高影响力生活类达人为研究对象，尝试通过多维画像、文本挖掘与聚类分析相结合的方式，对其行为模式进行描述性归纳。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "意见领袖（KOL）理论为本研究提供了重要的解释框架"),
            "意见领袖理论为本文理解高影响力达人的传播作用提供了基础框架。该理论认为，在特定社群中具备较高可见度、较强说服力与持续内容供给能力的个体，往往能够在信息扩散与决策形成过程中发挥关键影响。对应到小红书平台，高影响力达人并不只是拥有较大的粉丝规模，更重要的是通过持续发布内容、维持互动和塑造可信形象来影响受众。因而，本文在聚类和文本分析中关注粉丝规模、收藏评论结构、商业合作强度以及评论互动差异，目的在于从数据层面描述不同意见领袖类型的外在表现。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "社会认同理论则从受众心理层面解释了达人营销风靡的原因"),
            "社会认同理论进一步解释了受众为何会对不同达人形成持续关注。该理论强调，个体会通过群体归属、身份投射与情感认同来建构自我，并在消费与互动过程中表达这种认同。对于小红书用户而言，关注达人、参与评论、收藏内容乃至购买同款，往往不仅是功能性选择，也包含对某种生活方式、审美取向和圈层身份的认同。因此，本文将粉丝年龄结构、关注焦点以及评论中的拟亲密互动、颜值赞美和求回应表达视为理解达人影响力差异的重要线索。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "基于上述两个理论，本文构建如下分析框架"),
            "基于上述两个理论，本文将研究问题拆解为三个相互衔接的层面：第一层是用户画像特征，用于说明达人服务于何种受众结构；第二层是内容表达与互动方式，用于观察达人如何通过帖子文本、人设表达和评论互动维持影响力；第三层是群体差异识别，即在前两层基础上通过聚类分析归纳不同类型达人。就理论对应关系而言，意见领袖理论主要帮助理解达人在内容供给、互动维持和商业化表现上的差异，社会认同理论则帮助理解不同受众结构及其互动表达所反映出的身份认同特征。由此，本文形成“用户画像特征—内容与互动表现—达人群体差异”的分析框架。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "综合上述文献可以发现，学界目前对小红书等内容平台的营销机制"),
            "本章通过文献梳理和理论讨论，明确了本文的两个核心支点：一是以用户画像为核心变量，对高影响力达人进行多维度刻画；二是以文本挖掘与聚类分析作为主要方法，对达人内容表达、互动方式和群体差异进行实证描述。基于上述指标体系与分析框架，第三章将完成变量构建，第四章则进一步展开数据预处理、文本分析和群体聚类。",
        )

        # Chapter 4 polish and table references
        set_paragraph_text(
            find_by_prefix(paragraphs, "从“基于用户画像”的主线出发，本节首先说明第四章所使用的数据来源"),
            "从“基于用户画像”的主线出发，本节首先说明第四章所使用的数据来源及其与用户画像变量的关系。本研究的数据来源分为达人画像数据与文本数据两部分。达人画像数据来自灰豚平台整理后的结构化总表，共计3403位高影响力生活类达人；聚类分析样本采用带有群体标签_k3的达人聚类结果表，共计3371位达人。文本数据采用DOM爬取形成的主文本池，共包含7375条帖子文本与94247条评论文本。将文本数据与聚类结果按creator_id进行对齐后，最终获得7360条可用于群体比较的帖子与94126条评论，具体样本规模见表4-1。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在互动文本层面，本文进一步对94126条已对齐评论进行归纳分析"),
            "在互动文本层面，本文进一步对94126条已对齐评论进行归纳分析。为提高评论分析的可解释性，本文依据高频表达、语义模式与互动意图，将评论划分为颜值赞美、拟亲密互动、关怀支持、提问求回应、消费转化等类别，具体识别规则见表4-2。结果显示，三类群体的评论都以泛互动类表达为主，但仍存在明显差异：群体0的颜值赞美与拟亲密互动占比相对更高，群体1的提问求回应与消费转化占比更高，群体2整体评论结构更分散、集中度较低，具体占比比较见表4-3。评论高频词中反复出现“宝宝”“姐姐”“哈哈哈”“怎么”“好看”等称呼和反应词，说明评论区的核心并不是理性讨论，而是拟亲密关系、颜值赞美、求回应和轻度消费转化共同构成的互动机制。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在用户画像视角下，聚类分析的目的不是单纯把达人分组"),
            "在用户画像视角下，聚类分析的目的不是单纯把达人分组，而是识别不同受众结构下达人在互动风格和商业化表现上的组合差异。参考Chen（2024）在小红书创作者相关性分析中采用聚类模型的思路，本文选取粉丝数、Top1年龄段占比、藏赞比、评赞比和商业笔记占比五项指标构建K-means聚类模型。其中，粉丝数用于衡量账号的基础体量，Top1年龄段占比用于刻画粉丝年龄结构的集中程度，藏赞比和评赞比分别反映内容被保存与引发讨论的倾向，商业笔记占比则衡量达人的商业合作强度。聚类前先对上述变量进行标准化处理，以消除量纲差异带来的影响，各变量的含义及入模理由见表4-4。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "基于K-means聚类结果，本文将3371位高影响力达人划分为三类特征鲜明的群体"),
            "基于K-means聚类结果，本文将3371位高影响力达人划分为三类特征鲜明的群体，相关画像特征概览见表4-5。群体0共有2671位达人，占比最高，整体表现较为均衡，可概括为“均衡主流型”；群体1共有617位达人，在藏赞比和商业笔记占比上明显高于整体均值，可概括为“收藏转化与商业合作型”；群体2共有83位达人，评赞比显著高于其余两类，可概括为“高评论互动型”。三类群体的并存表明，小红书高影响力达人生态并非均质结构，而是存在明显的功能分化。",
        )

        # Light language clean-up in chapter 4
        set_paragraph_text(
            find_by_prefix(paragraphs, "本节首先从粉丝画像的视角对达人群体进行宏观描述"),
            "本节首先从粉丝画像的视角对达人群体进行宏观描述，考察用户画像特征（性别、年龄、地域、活跃时段）如何界定达人的受众基本盘。描述性统计结果显示，生活记录类达人以女性和独立创作者为主，分别约占样本的66%和61.7%。地域上高度集中于广东、浙江、上海、北京等高线省市，受众则主要分布于18至34岁的年轻女性群体。整体来看，该群体在商业化上较为克制，商业笔记占比中位数仅为4.7%；在互动结构上，收藏行为较为突出，藏赞比中位数为0.13，说明内容的保存价值在平台互动中占有重要位置。进一步比较不同粉丝画像的达人可以发现，面向25至34岁用户的达人往往具有更高的收藏价值，而面向更年轻用户的达人更容易引发评论互动。这一差异表明，不同年龄圈层的受众会以不同方式表达对达人内容的认同。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "数据中同时包含图文和视频两套报价与互动成本数据"),
            "数据中同时包含图文和视频两套报价与互动成本数据，为内容形式效能比较提供了基础。整体而言，视频笔记的报价中位数为20000元，高于图文笔记的14500元，溢价约38%，说明市场对视频内容给予了更高估值。从互动成本（CPE）看，三类群体均表现出视频CPE低于图文CPE的共同特征，但差距幅度并不一致：均衡主流型的视频CPE中位数最低，收藏转化与商业合作型与高评论互动型也普遍表现出视频更具效率的趋势。由此可见，视频在互动成本层面具有一定优势，但具体的内容形式选择仍需结合达人所属群体与粉丝偏好进行差异化判断。",
        )

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        body_style = first_run_style(find_by_prefix(paragraphs, "在预处理环节，本文首先对达人简介"))
        caption_template = find_by_exact(paragraphs, "图6 肘部法则与轮廓系数K值选取")

        # Insert tables in order of appearance
        p_128 = find_by_prefix(paragraphs, "在预处理环节，本文首先对达人简介")
        cap_41 = clone_with_text(caption_template, "表4-1 文本样本来源与规模")
        tbl_41 = build_table(sample_rows, body_style)
        insert_after(p_128, cap_41)
        insert_after(cap_41, tbl_41)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p_147 = find_by_prefix(paragraphs, "在互动文本层面，本文进一步对94126条已对齐评论进行归纳分析")
        cap_42 = clone_with_text(caption_template, "表4-2 评论互动类型识别规则")
        tbl_42 = build_table(interaction_def_rows, body_style)
        insert_after(p_147, cap_42)
        insert_after(cap_42, tbl_42)

        cap_43 = clone_with_text(caption_template, "表4-3 三类群体评论互动类型占比比较")
        tbl_43 = build_table(comment_compare_rows, body_style)
        insert_after(tbl_42, cap_43)
        insert_after(cap_43, tbl_43)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p_161 = find_by_prefix(paragraphs, "在用户画像视角下，聚类分析的目的不是单纯把达人分组")
        cap_44 = clone_with_text(caption_template, "表4-4 聚类变量定义与入模理由")
        tbl_44 = build_table(cluster_var_rows, body_style)
        insert_after(p_161, cap_44)
        insert_after(cap_44, tbl_44)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        p_166 = find_by_prefix(paragraphs, "基于K-means聚类结果，本文将3371位高影响力达人划分为三类特征鲜明的群体")
        cap_45 = clone_with_text(caption_template, "表4-5 三类群体画像特征概览")
        tbl_45 = build_table(profile_rows, body_style)
        insert_after(p_166, cap_45)
        insert_after(cap_45, tbl_45)

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
