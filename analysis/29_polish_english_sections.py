#!/usr/bin/env python3
"""
Polish English abstract, keywords, and cluster naming in the thesis docx.
"""

from pathlib import Path

from docx import Document


INPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_版式微调终版.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_英文润色版.docx")


def find_para(doc: Document, exact: str):
    for p in doc.paragraphs:
        if p.text == exact:
            return p
    raise ValueError(exact)


def main():
    doc = Document(str(INPUT_DOC))

    # English title on cover
    find_para(
        doc,
        "A Study on Behavioral Patterns of High-Influence Lifestyle Creators on Xiaohongshu Based on User Profiles",
    ).text = "A Study of Behavioral Patterns among High-Impact Lifestyle Creators on Xiaohongshu from a User-Profile Perspective"

    # English abstract
    find_para(
        doc,
        "The rise of content-driven e-commerce has reshaped both consumer decision-making and brand communication. On platforms such as Xiaohongshu, high-influence lifestyle creators occupy a central position in this process, yet systematic empirical descriptions of their profile structure, content expression, and group differentiation remain limited. This study examines 3,403 Xiaohongshu lifestyle creators with at least 100,000 followers, and combines descriptive statistics, text mining of posts and comments, and K-means clustering to describe their behavioral patterns. The findings are threefold. First, the creator ecosystem is structurally concentrated: female creators account for 66.62% of the sample, mid-tier creators make up 87.4%, and 61.74% operate independently. Their core audience is concentrated among women aged 18–34 in higher-tier cities, while fan-quality indicators such as active-fan share and low suspicious-fan share remain relatively stable overall. Second, high-influence creators tend to adopt a hybrid content strategy that combines broad lifestyle expression with selected vertical interests, while maintaining closeness through everyday persona construction and comment interaction. Post and comment texts show that appearance praise, parasocial intimacy, question-response interaction, and light consumption conversion form the main interaction patterns. Third, K-means clustering identifies three distinct groups: balanced mainstream creators, collection-and-commercial-conversion creators, and high-comment-interaction creators. These groups differ not only in follower structure and content expression, but also in engagement quality and commercialization performance. Overall, the study provides a descriptive account of how user profiles, text expression, and creator differentiation are linked on Xiaohongshu.",
    ).text = (
        "The rise of content-driven e-commerce has reshaped both consumer decision-making and brand communication. On platforms such as Xiaohongshu, high-impact lifestyle creators play a central role in this process, yet systematic empirical descriptions of their profile structure, textual expression, and group differentiation remain limited. "
        "This study examines 3,403 Xiaohongshu lifestyle creators with at least 100,000 followers and combines descriptive statistics, text mining of posts and comments, and K-means clustering to analyze their behavioral patterns. "
        "The findings are threefold. First, the creator ecosystem is structurally concentrated: female creators account for 66.62% of the sample, mid-tier creators make up 87.4%, and 61.74% of the creators operate independently. Their core audience is concentrated among women aged 18–34 in higher-tier cities. The sample also shows relatively stable fan-quality indicators, with a median active-fan share of 39.75% and a median suspicious-fan share of only 7.52%. "
        "Second, high-impact creators tend to adopt a hybrid content strategy that combines broad lifestyle expression with selected vertical interests while maintaining closeness through everyday persona construction and comment interaction. Post and comment texts suggest that appearance praise, parasocial intimacy, questions seeking response, and light purchase intent constitute the main interaction patterns. "
        "Third, K-means clustering identifies three distinct groups: balanced mainstream creators, collection-driven and commercially oriented creators, and high-comment-interaction creators. These groups differ not only in follower structure and content expression, but also in engagement quality and commercialization performance. Overall, the study provides a descriptive account of how user profiles, textual expression, and creator differentiation are linked on Xiaohongshu."
    )

    # Keywords
    find_para(
        doc,
        "Keywords: Influencer Marketing; Text Mining; K-means Clustering; User Profiling",
    ).text = "Keywords: Influencer Marketing; Text Mining; K-means Clustering; User-Profile Analysis"

    doc.save(str(OUTPUT_DOC))
    print(OUTPUT_DOC)


if __name__ == "__main__":
    main()
