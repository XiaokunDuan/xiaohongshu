#!/usr/bin/env python3
"""
Medium restructuring pass for the thesis submission version.
"""

from __future__ import annotations

import copy
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
W = "{%s}" % NS["w"]

INPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_最终提交版.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_送审收口版.docx")


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


def remove_paragraph(paragraph: etree._Element) -> None:
    parent = paragraph.getparent()
    parent.remove(paragraph)


def main() -> None:
    with ZipFile(INPUT_DOC) as zin:
        doc = etree.fromstring(zin.read("word/document.xml"))
        body = doc.xpath("//w:body", namespaces=NS)[0]
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # 1.2 研究目的与意义：补学术意义，压缩实践意义
        set_paragraph_text(
            find_by_prefix(paragraphs, "从达人视角看，归纳高影响力达人的行为特征"),
            "从学术层面看，现有关于小红书平台的研究更多聚焦于消费决策、平台治理与单一内容类型分析，对于高影响力生活记录类达人如何在用户画像约束下形成差异化内容表达、互动方式与商业化特征的系统讨论仍相对有限。本文以用户画像为切入视角，将结构化画像数据、帖子文本与评论文本结合起来考察高影响力达人群体，为平台创作者研究提供一组描述性、类型化的经验证据。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "从平台视角看，研究结果有助于平台识别不同类型达人的成长路径"),
            "从实践层面看，研究结果一方面可为达人自身调整内容定位、发布节奏与商业合作策略提供参考，另一方面也有助于平台与品牌方在创作者培育、达人筛选和资源配置时，更综合地考虑受众结构、互动表现与内容风格之间的对应关系。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "从品牌方与商家视角看，若仅依据表层流量指标进行达人选择"),
            "因此，本文既关注高影响力达人在平台生态中的结构特征，也关注其在内容表达和互动组织中的差异化路径，希望在学术解释与实践应用之间建立更稳妥的连接。",
        )

        # 1.3 行为模式定义再收紧
        set_paragraph_text(
            find_by_prefix(paragraphs, "本文以粉丝画像为核心切入变量"),
            "本文以粉丝画像为核心切入变量，并将“行为模式”界定为达人在特定受众结构约束下所呈现出的内容表达、互动方式与商业化特征的稳定组合，由此对高影响力生活类达人的差异化路径进行归纳分析。",
        )

        # 1.4.1 真正写成方法节
        p78 = find_by_prefix(paragraphs, "本文所使用的数据包括两部分：一是3403位粉丝数10万以上的生活记录类达人画像样本")
        set_paragraph_text(
            p78,
            "本文主要采用描述性统计分析、文本挖掘与K-means聚类分析三种方法，并分别对应不同研究问题。描述性统计用于刻画高影响力生活记录类达人的基础属性、粉丝结构与商业化表现，以回答达人整体画像特征如何分布的问题。",
        )
        p79 = find_by_prefix(paragraphs, "第一，基础身份信息主要包括达人名称")
        set_paragraph_text(
            p79,
            "文本挖掘主要用于处理达人简介、帖子标题与正文以及评论文本。具体包括统一清洗、中文分词、停用词过滤、关键词筛选、共现网络构建和评论互动类型归纳，以识别高影响力达人在内容表达与互动组织上的主要特征。",
        )
        p80 = find_by_prefix(paragraphs, "第二，影响力量化指标主要包括粉丝数")
        set_paragraph_text(
            p80,
            "K-means聚类分析则用于在用户画像视角下识别达人群体的差异化类型。本文在核心变量标准化基础上，对粉丝数、年龄结构、藏赞比、评赞比和商业笔记占比进行聚类，以比较不同群体在受众结构、内容策略与商业化表现上的组合差异。",
        )
        p81 = find_by_prefix(paragraphs, "第三，内容营销与商业化表现主要包括笔记总数")
        set_paragraph_text(
            p81,
            "上述三种方法构成由“整体画像描述—文本特征识别—群体类型划分”递进展开的研究路径，共同服务于本文三个研究问题的回答。与之对应的数据来源、变量定义和预处理口径将在第三章和第四章中进一步说明。",
        )
        remove_paragraph(find_by_prefix(doc.xpath("//w:body/w:p", namespaces=NS), "第四，粉丝画像数据主要包括活跃粉丝占比"))
        remove_paragraph(find_by_prefix(doc.xpath("//w:body/w:p", namespaces=NS), "在上述数据基础上，本文进一步完成清洗、转换"))

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # 3.1 明确样本门槛与数据时点
        set_paragraph_text(
            find_by_prefix(paragraphs, "本文将研究对象界定为小红书平台中的高影响力生活记录类达人"),
            "本文将研究对象界定为小红书平台中的高影响力生活记录类达人，并以粉丝数达到10万作为进入样本池的操作性门槛。需要说明的是，这一门槛主要用于限定研究对象范围，并不意味着本文将高影响力简单等同于单一流量指标；后续分析仍将结合互动表现、商业笔记占比与文本特征综合考察达人差异。结构化画像样本来自灰豚平台在论文研究阶段导出的达人总表，共3403位达人；经缺失值处理与异常值筛查后，最终用于K-means聚类的有效样本为3371位。帖子与评论文本样本来自同一研究阶段内对平台公开页面进行DOM抓取形成的主文本池，共包含7375条帖子和94247条评论。",
        )

        # 3.2 删除重复的四类维度展开段
        for prefix in [
            "首先是基础身份特征，涵盖了达人的账号名称",
            "其次是影响力量化指标，提取了粉丝总数",
            "再次是内容营销与商业转化表现，包括达人的笔记发布总量",
            "最后是粉丝群体的画像数据，这部分涵盖了活跃粉丝与非活跃粉丝的占比",
        ]:
            remove_paragraph(find_by_prefix(doc.xpath("//w:body/w:p", namespaces=NS), prefix))

        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        # 4.1 再压缩
        set_paragraph_text(
            find_by_prefix(paragraphs, "在第三章已完成研究对象、变量体系和方法路径说明的基础上"),
            "在第三章已完成研究对象界定、变量定义与方法路径说明的基础上，本节仅对第四章实际使用的数据口径作简要交代。第四章所用达人画像样本为3403位达人、聚类样本为3371位达人、文本样本为7375条帖子与94247条评论，对齐后的群体比较样本为7360条帖子与94126条评论，具体见表4-1。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在预处理环节，本文首先对达人简介、帖子标题、帖子正文与评论内容进行统一清洗"),
            "在预处理环节，本文主要完成统一清洗、中文分词、停用词过滤、群体映射与异常值处理五项工作。其中，共现关系以同一帖子中的关键词共现为统计窗口，TF-IDF仅用于辅助剔除解释力较弱的泛化词，而评论互动类型识别则采用规则归类方式，以保证文本分析结果具有较好的可解释性与可复核性。",
        )

        # 收紧因果化和过满措辞
        set_paragraph_text(
            find_by_prefix(paragraphs, "本节首先从粉丝画像的视角对达人群体进行宏观描述"),
            "本节首先从粉丝画像的视角对达人群体进行宏观描述，考察用户画像特征（性别、年龄、地域、活跃时段）如何勾勒达人的受众基本盘。描述性统计结果显示，生活记录类达人以女性和独立创作者为主，分别约占样本的66%和61.7%。地域上高度集中于广东、浙江、上海、北京等高线省市，受众则主要分布于18至34岁的年轻女性群体。整体来看，该群体在商业化上较为克制，商业笔记占比中位数仅为4.7%；在互动结构上，收藏行为较为突出，藏赞比中位数为0.13，说明内容的保存价值在平台互动中占有重要位置。进一步比较不同粉丝画像的达人可以发现，面向25至34岁用户的达人更常表现出较高的收藏倾向，而面向更年轻用户的达人更容易引发评论互动。这一差异表明，不同年龄圈层的受众会以不同方式表达对达人内容的认同。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "图1呈现了达人标签的共现网络结构"),
            "图1呈现了达人标签的共现网络结构。网络中节点大小代表标签出现频次，连线粗细代表两个标签的共现强度。可以观察到，接地气生活作为核心节点处于网络中心，与其他标签的共现频次最高（3214次），构成整个内容生态的底层连接；明星娱乐资讯、情感日常、宝宝日常等构成第二层的泛生活社群节点；美食、萌宠、科普等垂直兴趣标签则分布于网络外围，与核心节点保持稳定连接但彼此之间连线较疏。这一结构显示，高影响力达人普遍采用以泛生活表达为主干、以垂直兴趣内容为补充的内容组织方式。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在用户画像视角下，聚类分析的目的不是单纯把达人分组"),
            "在用户画像视角下，聚类分析的目的不是单纯把达人分组，而是识别不同受众结构下达人在互动风格和商业化表现上的组合差异。参考Chen（2024）在小红书创作者相关性分析中采用聚类模型的思路，本文选取粉丝数、Top1年龄段占比、藏赞比、评赞比和商业笔记占比五项指标构建K-means聚类模型。其中，粉丝数用于衡量账号的基础体量，Top1年龄段占比用于刻画粉丝年龄结构的集中程度，藏赞比和评赞比分别反映内容被保存与引发讨论的倾向，商业笔记占比则衡量达人的商业合作强度。之所以未将性别比例、地域分布、活跃粉丝占比、水粉占比以及报价、CPE、CPM直接纳入聚类，是因为这些指标更适合作为群体解释和结果比较变量，而不直接用于界定聚类边界。聚类变量及其入模理由见表4-3。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "进一步结合文本挖掘结果可以发现，不同群体在内容与互动方式上呈现出较为清晰的差异"),
            "进一步结合文本挖掘结果可以发现，不同群体在内容与互动方式上呈现出较为清晰的差异。均衡主流型达人更常采用泛生活、氛围化与审美化表达；收藏转化与商业合作型达人更容易承接推荐、护肤、好物等种草导向内容；高评论互动型达人则更容易形成讨论度较高但规模相对较小的互动场域。总体来看，达人画像变量、内容策略与评论互动之间呈现出较为稳定的对应关系，共同构成了高影响力达人差异化发展的外在表现。从理论回应看，均衡主流型可从意见领袖理论中依靠持续内容供给维持广泛影响力的路径进行理解；收藏转化与商业合作型则表现出更强的内容保存价值与商业承接能力；高评论互动型可从社会认同理论视角理解，其较强的评论参与和关系表达更接近以互动认同维系影响力的路径。换言之，三类群体并非简单的流量高低差异，而是对应了不同的受众连接方式与影响力实现路径。",
        )

        # 5.2 增补数据时点说明
        set_paragraph_text(
            find_by_prefix(paragraphs, "尽管本文在数据规模与分析维度上具有一定系统性"),
            "尽管本文在数据规模与分析维度上具有一定系统性，但仍存在若干局限。首先，本文使用的是横截面数据，只能反映达人在特定时间节点的静态状态，无法捕捉其从起步到高影响力阶段的动态演变过程，也无法进一步识别变量变化与后续表现之间的因果关系。其次，研究对象聚焦于小红书单一平台，所得结论是否适用于抖音、快手、微博等其他内容平台，仍需结合不同平台的算法机制、用户结构与内容生态进行进一步验证。再次，结构化画像数据主要来自第三方平台整理结果，仍可能存在覆盖范围与口径选择上的平台偏差；评论与帖子文本虽然规模较大，但采集主要集中于论文研究阶段的单一时间窗口，仍受到抓取时点和可见范围的限制。最后，本文主要采用描述性统计、文本挖掘与聚类分析方法，尚未进一步引入回归模型或因果识别设计，因此当前结论更适合被理解为对高影响力达人群体特征及其差异的描述性证据。",
        )

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
