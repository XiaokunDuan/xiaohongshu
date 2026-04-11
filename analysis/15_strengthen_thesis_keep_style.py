#!/usr/bin/env python3
"""
Strengthen thesis structure and figure presentation in the final synced DOCX.

- add a 3.3 section to clarify "behavioral patterns" and chapter roles
- add user-profile linkage sentences
- soften a few over-strong claims
- insert the actual Figure 6 image (elbow + silhouette plot)
"""

from __future__ import annotations

import copy
import re
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}
W = "{%s}" % NS["w"]
R = "{%s}" % NS["r"]

INPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_最终同步版.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_结构强化版.docx")
FIG6_IMAGE = Path("/Users/dxk/xiaohongshu/reports/chapter4_support/plots/cluster_elbow_silhouette_k3.png")


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


def insert_after(ref: etree._Element, new_p: etree._Element) -> None:
    parent = ref.getparent()
    parent.insert(parent.index(ref) + 1, new_p)


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


def next_rel_id(rels: etree._Element) -> str:
    nums = []
    for rel in rels:
        rid = rel.get("Id", "")
        m = re.fullmatch(r"rId(\d+)", rid)
        if m:
            nums.append(int(m.group(1)))
    return f"rId{max(nums, default=0)+1}"


def update_embed_ids(paragraph: etree._Element, new_rid: str) -> None:
    for blip in paragraph.xpath(".//a:blip", namespaces=NS):
        blip.set(f"{R}embed", new_rid)


def main() -> None:
    if not INPUT_DOC.exists():
        raise FileNotFoundError(INPUT_DOC)
    if not FIG6_IMAGE.exists():
        raise FileNotFoundError(FIG6_IMAGE)

    with ZipFile(INPUT_DOC) as zin:
        doc = etree.fromstring(zin.read("word/document.xml"))
        rels = etree.fromstring(zin.read("word/_rels/document.xml.rels"))
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # 1) 1.3 user-profile core variable sentence
        q3 = find_by_exact(paragraphs, "基于粉丝画像特征与互动指标，达人群体可以聚类为哪些差异化类别？其画像差异如何体现在内容策略与商业模式上？")
        q_template = q3
        q_note = clone_with_text(
            q_template,
            "本文以粉丝画像为核心切入变量，并结合达人内容表达、互动指标与商业化表现，对高影响力生活类达人的行为模式进行归纳分析。",
        )
        insert_after(q3, q_note)

        # refresh paragraphs
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # 2) add 3.3 section before chapter 4
        last_32 = find_by_exact(paragraphs, "最后是粉丝群体的画像数据，这部分涵盖了活跃粉丝与非活跃粉丝的占比、性别构成、地域分布、年龄分层以及粉丝关注焦点的半结构化文本，以此来描绘达人背后的受众基本盘。")
        h2_template = find_by_exact(paragraphs, "3.2 多维达人画像特征指标体系构建")
        body_template = find_by_exact(paragraphs, "为了对达人群体进行立体化的画像构建，仅依靠单一的粉丝数量是不够的，需要从多个维度搭建科学的特征指标体系。其中，粉丝画像数据（即用户画像）是本文的核心分析维度——通过达人服务于什么样的用户来理解其内容策略与商业模式的差异，这也是题目基于用户画像的具体含义。本研究的数据维度主要包含以下四个方面：")
        p_33 = clone_with_text(h2_template, "3.3 行为模式的操作性定义与分析框架")
        p_33_body1 = clone_with_text(
            body_template,
            "本文所称“行为模式”，并非泛指达人的所有行为，而是特指在用户画像约束下所呈现出的内容表达、互动方式与商业化特征。也就是说，本文关注的不是单一行为动作，而是达人在面向不同受众结构时，如何在内容供给、评论互动和商业化表现上形成相对稳定的组合特征。",
        )
        p_33_body2 = clone_with_text(
            body_template,
            "基于这一界定，第三章主要负责构建画像模型与指标体系，明确哪些变量用于刻画达人及其受众；第四章则在此基础上开展描述性统计、文本挖掘与聚类分析，以识别不同达人群体在用户画像、内容表达和互动模式上的差异。由此，第三章承担“定义变量与分析框架”的功能，第四章承担“利用数据展开实证分析”的功能。",
        )
        insert_after(last_32, p_33)
        insert_after(p_33, p_33_body1)
        insert_after(p_33_body1, p_33_body2)

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # 3) tighten abstract and conclusion language
        p_abs = find_by_prefix(paragraphs, "本文以小红书平台3403位粉丝数10万以上的生活记录类达人为研究对象")
        set_paragraph_text(
            p_abs,
            "本文以小红书平台3403位粉丝数10万以上的生活记录类达人为研究对象，综合运用描述性统计分析、文本挖掘（帖子关键词共现、评论互动类型归纳与群体对比）及K-means聚类算法，围绕三个研究问题展开分析。研究发现如下：第一，该群体整体呈现女性主导（66.62%）、腰部达人为主（87.4%）、独立运营居多（61.74%）的结构特征，地域上高度集中于高线省市，核心受众为18至34岁年轻女性，商业笔记占比中位数仅为4.7%。第二，高影响力达人在内容策略上普遍采用“泛生活+垂直兴趣”的复合布局，并表现出以“超级分享者”为核心的人设特征；评论文本显示，颜值赞美、拟亲密互动、提问求回应和消费转化构成了主要互动类型。第三，经肘部法则与轮廓系数验证，K-means聚类将达人生态划分为均衡主流型、收藏转化与商业合作型、高评论互动型三类群体，各群体在粉丝画像、内容表达与商业化表现上呈现出明显差异，相关发现可从意见领袖理论和社会认同理论视角进行理解。"
        )
        p_abs2 = find_by_prefix(paragraphs, "本研究从达人供给侧视角丰富了现有文献对高影响力创作者群体的实证认识")
        set_paragraph_text(
            p_abs2,
            "本研究从达人供给侧视角丰富了现有文献对高影响力创作者群体的实证认识，所提出的群体分类框架与内容表达特征可为内容创作者的差异化定位、品牌方的达人筛选以及平台的流量分配优化提供描述性参考。"
        )

        p_concl2 = find_by_prefix(paragraphs, "在内容策略与互动机制方面")
        set_paragraph_text(
            p_concl2,
            "在内容策略与互动机制方面，高影响力达人普遍采用泛生活+垂直兴趣的复合内容布局，并通过日常化、可亲近的人设表达维系与粉丝的关系。评论文本显示，颜值赞美、拟亲密互动、提问求回应与轻度消费转化构成了主要互动类型。内容发布时机上，粉丝活跃高峰集中于工作日17至21时；内容形式上，视频报价溢价约38%，但最优内容形式因达人类型而异，不存在统一最优解。"
        )
        p_concl3 = find_by_prefix(paragraphs, "在达人群体类型分化方面")
        set_paragraph_text(
            p_concl3,
            "在达人群体类型分化方面，经肘部法则与轮廓系数验证，最优聚类数为K=3，形成三类特征鲜明的群体。均衡主流型（2671人）整体表现较为均衡，是平台中的主流高影响力达人；收藏转化与商业合作型（617人）在藏赞比和商业笔记占比上更高；高评论互动型（83人）则表现出更强的评论互动特征。三类群体的并存表明，小红书达人生态已呈现出功能不同、策略各异的多元分化格局。整体上，这些差异可从不同的影响力实现路径与受众认同方式来理解。"
        )

        # 4) strengthen chapter 4 intro links to user profiles
        p_41_1 = find_by_prefix(paragraphs, "本研究的数据来源分为达人画像数据与文本数据两部分")
        set_paragraph_text(
            p_41_1,
            "从“基于用户画像”的主线出发，本节首先说明第四章所使用的数据来源及其与用户画像变量的关系。本研究的数据来源分为达人画像数据与文本数据两部分。达人画像数据来自灰豚平台整理后的结构化总表，共计3403位高影响力生活类达人；聚类分析样本采用带有群体标签_k3的达人聚类结果表，共计3371位达人。文本数据采用DOM爬取形成的主文本池，共包含7375条帖子文本与94247条评论文本。将文本数据与聚类结果按creator_id进行对齐后，最终获得7360条可用于群体比较的帖子与94126条评论。"
        )
        p_43_intro = find_by_prefix(paragraphs, "为回应文本挖掘部分方法展示不足的问题")
        set_paragraph_text(
            p_43_intro,
            "在用户画像主线下，文本挖掘的作用并不是脱离画像单独讨论内容，而是考察不同用户画像所对应的达人在内容表达与评论互动上呈现出的差异。为回应文本挖掘部分方法展示不足的问题，本文将第四章的文本分析进一步细化为内容文本分析与互动文本分析两部分。前者主要对应达人标签、帖子标题与正文，后者主要对应达人简介与评论文本。具体处理流程包括文本清洗、中文分词、停用词过滤、高频词筛选、共现网络构建以及互动类型归纳，并按三类聚类群体完成映射比较。"
        )
        p_44_1 = find_by_prefix(paragraphs, "参考Chen（2024）在小红书创作者相关性分析中采用聚类模型的思路")
        set_paragraph_text(
            p_44_1,
            "在用户画像视角下，聚类分析的目的不是单纯把达人分组，而是识别不同受众结构下达人在互动风格和商业化表现上的组合差异。参考Chen（2024）在小红书创作者相关性分析中采用聚类模型的思路，本文选取粉丝数、Top1年龄段占比、藏赞比、评赞比和商业笔记占比五项指标构建K-means聚类模型。其中，粉丝数用于衡量账号的基础体量，Top1年龄段占比用于刻画粉丝年龄结构的集中程度，藏赞比和评赞比分别反映内容被保存与引发讨论的倾向，商业笔记占比则衡量达人的商业合作强度。聚类前先对上述变量进行标准化处理，以消除量纲差异带来的影响。"
        )

        # 5) insert real figure 6 image before caption
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)
        fig6_caption = find_by_exact(paragraphs, "图6 肘部法则与轮廓系数K值选取")
        existing_prev = fig6_caption.getprevious()
        has_img_before = existing_prev is not None and existing_prev.xpath(".//a:blip", namespaces=NS)
        image_bytes = FIG6_IMAGE.read_bytes()

        if not has_img_before:
            # clone an existing image paragraph as template
            img_template = None
            for p in paragraphs:
                if p.xpath(".//a:blip", namespaces=NS):
                    img_template = p
                    break
            if img_template is None:
                raise RuntimeError("No image paragraph template found in document")

            new_rid = next_rel_id(rels)
            rel_el = etree.Element("Relationship")
            rel_el.set("Id", new_rid)
            rel_el.set("Type", "http://schemas.openxmlformats.org/officeDocument/2006/relationships/image")
            rel_el.set("Target", "media/image7.png")
            rels.append(rel_el)

            new_img_p = copy.deepcopy(img_template)
            update_embed_ids(new_img_p, new_rid)
            fig6_caption.getparent().insert(fig6_caption.getparent().index(fig6_caption), new_img_p)
        else:
            new_rid = None

        # 6) refine figure 6 explanatory sentence for Keep-like flow
        p_44_2 = find_by_prefix(paragraphs, "为确定最优聚类数K")
        set_paragraph_text(
            p_44_2,
            "为确定最优聚类数K，本文综合使用肘部法则与轮廓系数，对K=2至K=8进行逐一比较。图6给出了不同K值下SSE与轮廓系数的变化情况。结果显示，当K=3时，轮廓系数达到0.354，为当前比较范围内的最高值，同时SSE曲线在K=2至K=3之间出现明显拐点，说明三类划分在统计表现与结果解释之间取得了较好平衡。因此，本文最终采用K=3的聚类方案对3371位达人进行群体划分。"
        )

        new_doc_xml = etree.tostring(doc, xml_declaration=True, encoding="UTF-8", standalone="yes")
        new_rels_xml = etree.tostring(rels, xml_declaration=True, encoding="UTF-8", standalone="yes")

        with ZipFile(OUTPUT_DOC, "w", compression=ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                if item.filename == "word/document.xml":
                    zout.writestr(item, new_doc_xml)
                elif item.filename == "word/_rels/document.xml.rels":
                    zout.writestr(item, new_rels_xml)
                else:
                    zout.writestr(item, zin.read(item.filename))
            # add fig6 image if not already present
            if "word/media/image7.png" not in zin.namelist():
                zout.writestr("word/media/image7.png", image_bytes)

    print(OUTPUT_DOC)


if __name__ == "__main__":
    main()
