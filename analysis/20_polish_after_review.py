#!/usr/bin/env python3
"""
Polish the three-line-table thesis based on the latest review:
- shorten and compact table 4-2 to avoid ugly page splits
- remove residual old cluster naming
- add k-means++ / stability / silhouette interpretation
- trim chapter 4 duplication
- fix English title-page wording
"""

from __future__ import annotations

import copy
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
W = "{%s}" % NS["w"]

INPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_三线表终版.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_送审版.docx")


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
        if paragraph_text(p).startswith(prefix):
            return p
    raise ValueError(f"Paragraph prefix not found: {prefix}")


def find_by_exact(paragraphs: list[etree._Element], text: str) -> etree._Element:
    for p in paragraphs:
        if paragraph_text(p) == text:
            return p
    raise ValueError(f"Paragraph not found: {text}")


def set_tc_border(tc: etree._Element, edge: str, val: str = "single", sz: str = "8") -> None:
    tc_pr = tc.find("./w:tcPr", NS)
    if tc_pr is None:
        tc_pr = etree.SubElement(tc, f"{W}tcPr")
    tc_borders = tc_pr.find("./w:tcBorders", NS)
    if tc_borders is None:
        tc_borders = etree.SubElement(tc_pr, f"{W}tcBorders")
    el = tc_borders.find(f"./w:{edge}", NS)
    if el is None:
        el = etree.SubElement(tc_borders, f"{W}{edge}")
    el.set(f"{W}val", val)
    if val != "nil":
        el.set(f"{W}sz", sz)
        el.set(f"{W}space", "0")
        el.set(f"{W}color", "000000")


def rebuild_compact_table2(tbl: etree._Element, run_style: etree._Element | None) -> etree._Element:
    rows = [
        ["模块", "环节", "规则说明"],
        ["文本处理", "清洗与分词", "去除换行、URL和冗余空格；使用 jieba 分词，并过滤停用词、助词、代词及长度小于2的词项"],
        ["文本处理", "关键词筛选", "先按词频初筛，再结合 TF-IDF 剔除泛化词，保留解释力较强的关键词"],
        ["文本处理", "共现网络", "以同一帖子为共现窗口，两个关键词在同一帖子同时出现记为一次共现，累计次数作为边权"],
        ["互动识别", "主要类别", "颜值赞美、拟亲密互动、关怀支持、提问求回应、消费转化"],
        ["互动识别", "识别示例", "如“好美/颜值”“老婆/宝宝”“加油/心疼”“怎么/求回复”“同款/链接/下单”等"],
    ]

    # clear existing rows
    for child in list(tbl):
        tbl.remove(child)

    tbl_pr = etree.SubElement(tbl, f"{W}tblPr")
    tbl_w = etree.SubElement(tbl_pr, f"{W}tblW")
    tbl_w.set(f"{W}w", "0")
    tbl_w.set(f"{W}type", "auto")
    jc = etree.SubElement(tbl_pr, f"{W}jc")
    jc.set(f"{W}val", "center")
    tbl_layout = etree.SubElement(tbl_pr, f"{W}tblLayout")
    tbl_layout.set(f"{W}type", "fixed")
    borders = etree.SubElement(tbl_pr, f"{W}tblBorders")
    for edge in ("left", "right", "insideH", "insideV"):
        e = etree.SubElement(borders, f"{W}{edge}")
        e.set(f"{W}val", "nil")
    for edge in ("top", "bottom"):
        e = etree.SubElement(borders, f"{W}{edge}")
        e.set(f"{W}val", "single")
        e.set(f"{W}sz", "8")
        e.set(f"{W}space", "0")
        e.set(f"{W}color", "000000")

    grid = etree.SubElement(tbl, f"{W}tblGrid")
    widths = ["1600", "1800", "5800"]
    for w in widths:
        gc = etree.SubElement(grid, f"{W}gridCol")
        gc.set(f"{W}w", w)

    for r_idx, row in enumerate(rows):
        tr = etree.SubElement(tbl, f"{W}tr")
        tr_pr = etree.SubElement(tr, f"{W}trPr")
        cant = etree.SubElement(tr_pr, f"{W}cantSplit")
        for c_idx, text in enumerate(row):
            tc = etree.SubElement(tr, f"{W}tc")
            tc_pr = etree.SubElement(tc, f"{W}tcPr")
            tc_w = etree.SubElement(tc_pr, f"{W}tcW")
            tc_w.set(f"{W}w", widths[c_idx])
            tc_w.set(f"{W}type", "dxa")
            p = etree.SubElement(tc, f"{W}p")
            ppr = etree.SubElement(p, f"{W}pPr")
            jc = etree.SubElement(ppr, f"{W}jc")
            jc.set(f"{W}val", "left" if c_idx == 2 else "center")
            r = etree.SubElement(p, f"{W}r")
            if run_style is not None:
                rpr = copy.deepcopy(run_style)
                if r_idx == 0:
                    if rpr.find("./w:b", NS) is None:
                        etree.SubElement(rpr, f"{W}b")
                    if rpr.find("./w:bCs", NS) is None:
                        etree.SubElement(rpr, f"{W}bCs")
                r.append(rpr)
            t = etree.SubElement(r, f"{W}t")
            t.text = text
            for edge in ("top", "left", "bottom", "right"):
                set_tc_border(tc, edge, "nil")
            if r_idx == 0:
                set_tc_border(tc, "bottom", "single", "6")
    return tbl


def main() -> None:
    if not INPUT_DOC.exists():
        raise FileNotFoundError(INPUT_DOC)

    with ZipFile(INPUT_DOC) as zin:
        doc = etree.fromstring(zin.read("word/document.xml"))
        body = doc.xpath("//w:body", namespaces=NS)[0]
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # English cover wording
        set_paragraph_text(
            find_by_exact(paragraphs, "in partial fulfillment of the requirement"),
            "in partial fulfillment of the requirements",
        )

        # reduce 3.1 / 4.1 duplication
        set_paragraph_text(
            find_by_prefix(paragraphs, "从“基于用户画像”的主线出发，本节首先说明第四章所使用的数据来源"),
            "在第三章完成研究对象、样本范围与变量定义说明的基础上，本节仅对第四章实际使用的数据口径作简要说明。本文的数据来源分为达人画像数据与文本数据两部分：达人画像数据来自灰豚平台整理后的结构化总表，共计3403位高影响力生活类达人；聚类分析样本为经缺失值处理与异常值筛查后的3371位达人；文本数据来自DOM爬取形成的主文本池，共包含7375条帖子文本与94247条评论文本。将文本数据与聚类结果按creator_id对齐后，最终获得7360条可用于群体比较的帖子与94126条评论，具体样本规模见表4-1。",
        )

        # method clarity
        set_paragraph_text(
            find_by_prefix(paragraphs, "在预处理环节，本文首先对达人简介、帖子标题、帖子正文与评论内容进行统一清洗"),
            "在预处理环节，本文首先对达人简介、帖子标题、帖子正文与评论内容进行统一清洗，去除URL、换行与冗余空格；随后使用jieba进行中文分词，并结合停用词表剔除助词、代词、平台通用词及长度小于2的无效词项。在关键词筛选阶段，本文采用“词频初筛 + TF-IDF辅助过滤”的方式：先保留出现频次较高的候选词，再剔除解释力较弱、泛化程度过高的词项，以保留更能代表内容主题的关键词。在此基础上，分别开展高频词统计、共现网络构建、评论互动类型归纳以及三类群体对比分析，以增强文本挖掘部分的方法完整性与结果解释力。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "从方法上看，本文先对达人标签、帖子标题和正文进行统一清洗与分词"),
            "从方法上看，本文先对达人标签、帖子标题和正文进行统一清洗与分词，再依据词频与TF-IDF结果筛出具有解释意义的关键词作为候选节点。本文以“同一帖子”为共现窗口，将两个关键词在同一帖子文本中共同出现一次记为一次共现，并以累计共现次数作为边权；在过滤权重过低的边后，再构建内容共现网络。通过这一方式，内容分析不再停留于简单词频统计，而能够进一步呈现不同内容元素之间的结构性关系。",
        )

        # remove old K=4 naming remnants
        set_paragraph_text(
            find_by_prefix(paragraphs, "参考内容策略不仅体现在发什么，还体现在何时发"),
            "内容策略不仅体现在发什么，还体现在何时发。对粉丝活跃时间字段的解析显示，3358位有效数据的达人粉丝高峰活跃时段集中于17至21时，其中18时达到峰值（448人），符合下班或放学后的碎片化使用场景。从星期维度看，周二（543人）和周三（555人）的活跃达人数量高于周末，表明用户在工作日间隙刷小红书的频率高于纯休息日，这一时段分布特征与工作日碎片化内容消费的使用场景相符。图4中的时段分布为全样本层面的整体规律。进一步按三类聚类群体交叉分析可以发现，不同群体在高峰日分布上存在差异，其中部分评论互动更强的群体更接近周末高峰，其余群体则更偏工作日活跃。上述发现说明，发布时间策略同样会受到受众结构差异的影响。",
        )

        # kmeans rigor
        set_paragraph_text(
            find_by_prefix(paragraphs, "为确定最优聚类数K，本文综合使用肘部法则与轮廓系数"),
            "为确定最优聚类数K，本文综合使用肘部法则与轮廓系数，对K=2至K=8进行逐一比较。聚类实现采用sklearn的KMeans，初始化方式为k-means++，并设置n_init=10、random_state=42，以降低随机初始化带来的波动。图6给出了不同K值下SSE与轮廓系数的变化情况。结果显示，当K=3时，轮廓系数达到0.354，为当前比较范围内的最高值，同时SSE曲线在K=2至K=3之间出现明显拐点。需要说明的是，0.354并不属于边界非常清晰的聚类结构，但对于生活类达人这类内容与受众特征高度连续、边界较模糊的样本而言，该结果仍可视为具有一定区分度且便于解释的分组方案。因此，本文最终采用K=3的聚类方案对3371位达人进行群体划分。",
        )

        # theory callback
        p172 = find_by_prefix(paragraphs, "进一步结合文本挖掘结果可以发现，不同群体在内容与互动方式上呈现出较为清晰的差异")
        template = find_by_exact(paragraphs, "第五章 结论与展望")
        theory_p = copy.deepcopy(template)
        set_paragraph_text(
            theory_p,
            "从理论回应看，均衡主流型更接近意见领袖理论中依靠持续内容供给维持广泛影响力的类型；收藏转化与商业合作型则表现出更强的内容保存价值与商业承接能力；高评论互动型可从社会认同理论视角理解，其较强的评论参与和关系表达更接近以互动认同维系影响力的路径。换言之，三类群体并非简单的流量高低差异，而是对应了不同的受众连接方式与影响力实现路径。",
        )
        body.insert(body.index(p172) + 1, theory_p)

        # limitations add data-source bias
        set_paragraph_text(
            find_by_prefix(paragraphs, "尽管本文在数据规模与分析维度上具有一定系统性"),
            "尽管本文在数据规模与分析维度上具有一定系统性，但仍存在若干局限。首先，本文使用的是横截面数据，只能反映达人在特定时间节点的静态状态，无法捕捉其从起步到高影响力阶段的动态演变过程，也无法进一步识别变量变化与后续表现之间的因果关系。其次，研究对象聚焦于小红书单一平台，所得结论是否适用于抖音、快手、微博等其他内容平台，仍需结合不同平台的算法机制、用户结构与内容生态进行进一步验证。再次，结构化画像数据主要来自第三方平台整理结果，仍可能存在覆盖范围与口径选择上的平台偏差；评论与帖子文本虽然规模较大，但仍受到爬取时点和可见范围的限制。最后，本文主要采用描述性统计、文本挖掘与聚类分析方法，尚未进一步引入回归模型或因果识别设计，因此当前结论更适合被理解为对高影响力达人群体特征及其差异的描述性证据。",
        )

        # compact table 4-2
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        body_style = first_run_style(find_by_prefix(paragraphs, "在预处理环节，本文首先对达人简介"))
        # find the table following caption 表4-2
        cap = find_by_exact(paragraphs, "表4-2 文本预处理与评论互动识别规则")
        idx = body.index(cap)
        if idx + 1 < len(body) and etree.QName(body[idx + 1]).localname == "tbl":
            rebuild_compact_table2(body[idx + 1], body_style)

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
