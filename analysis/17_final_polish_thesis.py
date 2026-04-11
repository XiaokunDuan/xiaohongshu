#!/usr/bin/env python3
"""
Final polish pass for the thesis DOCX:
- tighten abstract and English abstract wording
- reduce colloquial expressions in chapter 1
- polish conclusion and limitations
- preserve existing formatting by cloning paragraph styles
"""

from __future__ import annotations

import copy
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
W = "{%s}" % NS["w"]

INPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_综述与表格强化版.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_终稿润色版.docx")


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


def main() -> None:
    if not INPUT_DOC.exists():
        raise FileNotFoundError(INPUT_DOC)

    with ZipFile(INPUT_DOC) as zin:
        doc = etree.fromstring(zin.read("word/document.xml"))
        paragraphs = doc.xpath("//w:body/w:p", namespaces=NS)

        set_paragraph_text(
            find_by_prefix(paragraphs, "随着内容经济的兴起"),
            "随着内容经济的发展，以小红书为代表的内容社区平台正在重塑消费者决策路径与品牌营销方式。现有文献已从营销策略、用户行为和平台机制等角度对这一现象展开讨论，但较少从供给侧出发，以用户画像为核心维度，对高影响力达人群体的结构特征、内容表达和类型分化进行系统描述。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "The rise of content-driven e-commerce has fundamentally changed"),
            "The rise of content-driven e-commerce has reshaped both consumer decision-making and brand communication. On platforms such as Xiaohongshu, high-influence lifestyle creators occupy a central position in this process, yet systematic empirical descriptions of their profile structure, content expression, and group differentiation remain limited. This study examines 3,403 Xiaohongshu lifestyle creators with at least 100,000 followers, and combines descriptive statistics, text mining of posts and comments, and K-means clustering to describe their behavioral patterns. The findings are threefold. First, the creator ecosystem is structurally concentrated: female creators account for 66.62% of the sample, mid-tier creators make up 87.4%, and 61.74% operate independently. Their core audience is concentrated among women aged 18–34 in higher-tier cities. Second, successful creators tend to adopt a hybrid content strategy that combines broad lifestyle expression with selected vertical interests, while maintaining closeness through everyday persona construction and comment interaction. Post and comment texts show that appearance praise, parasocial intimacy, question-response interaction, and light consumption conversion form the main interaction patterns. Third, K-means clustering identifies three distinct groups: balanced mainstream creators, collection-and-commercial-conversion creators, and high-comment-interaction creators. These groups differ in follower structure, content expression, and interaction style. Overall, the study provides a descriptive account of how user profiles, text expression, and creator differentiation are linked on Xiaohongshu, and offers practical reference for creator positioning, brand selection, and platform governance.",
        )

        set_paragraph_text(
            find_by_prefix(paragraphs, "这一模式的成功，离不开平台庞大的用户基础和活跃的生态"),
            "这一模式的形成离不开平台庞大的用户基础与高频互动生态。官方及第三方数据显示，截至2023年底，小红书月活跃用户数已超过2.6亿，日活跃用户数稳定在1亿量级，平台内容创作者超过6900万。海量内容沉淀与持续互动共同构成了平台的核心资产，平台笔记日均发布量超过300万篇，其中70%以上为用户原创内容（UGC）。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在小红书生态中，拔草动作的转化程度与生产内容的用户"),
            "在小红书生态中，最终的消费转化与内容生产者即达人的影响力密切相关。若创作者在特定领域具备持续稳定的内容输出能力，并能够在互动中建立可信、可亲近的形象，粉丝对其内容与推荐的接受程度往往会随之提升。对于生活类高影响力达人而言，这种影响力不仅体现为流量规模，也体现为其所承载的生活方式想象、情感连接与商业转化潜力。因此，围绕达人画像特征、内容表达方式以及与粉丝的互动模式展开研究，具有较强的现实意义与研究价值。基于此，本文以小红书生活记录类高影响力达人为研究对象，在用户画像视角下分析其内容表达、互动方式及群体差异，并讨论其潜在的商业价值。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "从达人的角度，归纳高影响力达人的行为特征"),
            "从达人视角看，归纳高影响力达人的行为特征，有助于为同类创作者制定内容策略和商业化策略提供参考。例如，内容发布频率、图文与视频形式的配置，以及商业合作与粉丝信任之间的平衡方式，均可通过群体比较获得经验性启示。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "从平台的角度，一方面，可以为平台制定精准化流量的策略"),
            "从平台视角看，研究结果有助于平台识别不同类型达人的成长路径与受众结构特征，从而为流量分配、创作者培育与平台治理提供参考。同时，若某些内容表达或互动方式与更高的用户参与度相关，也可为平台理解推荐机制与社区生态提供描述性依据。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "从品牌方或者商家的角度，透过表面的流量"),
            "从品牌方与商家视角看，若仅依据表层流量指标进行达人选择，往往难以准确判断合作效果。结合达人画像、人设表达、受众结构与互动特征进行综合评估，更有助于提升投放匹配度与资源配置效率。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "所谓的种草模式，本质上是内容经济发展衍生出的一种典型商业形态"),
            "所谓种草模式，是内容经济发展过程中形成的一种典型商业形态。与传统以商品陈列为核心的货架式电商不同，平台通过持续内容生产与算法分发吸引用户注意，再借助互动与搜索等行为将流量逐步转化为潜在消费需求。在小红书平台上，这一逻辑表现得尤为明显：创作者通过图文或视频发布内容，用户在浏览、收藏、关注等过程中完成“种草”，并在后续通过搜索、跳转或站外渠道实现购买，从而形成相对完整的内容转化链条。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "针对本文要研究的高影响力达人，可以将其理解为在特定圈层中具有较强话语权"),
            "本文所研究的高影响力达人，是指在特定圈层中具有较强传播能力、较高可见度和稳定互动表现的内容创作者。在小红书生态中，这类达人不仅拥有相对可观的粉丝规模，还能够通过持续内容供给和互动关系维持受众关注，并在一定程度上影响粉丝的消费认知与行为。界定这一群体的关键，不在于单一流量指标，而在于其受众结构、互动表现和商业化能力的综合呈现。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "画像这一概念原本多用于提炼普通用户的特征标签"),
            "画像原本多用于刻画普通用户的结构特征，在本文中则同时适用于达人与其粉丝群体。达人画像强调对创作者基础属性、互动表现、商业化程度和内容表达特征的综合刻画；用户画像则更侧重于粉丝年龄、性别、地域、关注焦点和活跃程度等维度。将两者结合，有助于从“服务于什么样的受众”这一视角理解达人行为模式的差异。",
        )

        set_paragraph_text(
            find_by_prefix(paragraphs, "进一步结合文本挖掘结果可以发现"),
            "进一步结合文本挖掘结果可以发现，不同群体在内容与互动方式上呈现出较为清晰的差异。均衡主流型达人更常采用泛生活、氛围化与审美化表达；收藏转化与商业合作型达人更容易承接推荐、护肤、好物等种草导向内容；高评论互动型达人则更容易形成讨论度较高但规模相对较小的互动场域。总体来看，达人画像变量、内容策略与评论互动之间具有较强关联性，共同构成了高影响力达人差异化发展的外在表现。",
        )
        set_paragraph_text(
            find_by_prefix(paragraphs, "在本文尽管本研究在数据规模与分析维度上具有一定系统性"),
            "尽管本文在数据规模与分析维度上具有一定系统性，但仍存在若干局限。首先，本文使用的是横截面数据，只能反映达人在特定时间节点的静态状态，无法捕捉其从起步到高影响力阶段的动态演变过程，也无法进一步识别变量变化与后续表现之间的因果关系。其次，研究对象聚焦于小红书单一平台，所得结论是否适用于抖音、快手、微博等其他内容平台，仍需结合不同平台的算法机制、用户结构与内容生态进行进一步验证。再次，文本分析部分虽然引入了帖子与评论文本，但仍以描述性挖掘和规则归纳为主，后续仍可在更大样本和更细粒度语义标注基础上继续深化。",
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
