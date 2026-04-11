from docx import Document
from docx.oxml import OxmlElement

SRC = "/Users/dxk/Downloads/段晓坤 毕业论文二稿_LDA主方法版.docx"
OUT = "/Users/dxk/Downloads/段晓坤 毕业论文二稿_LDA主方法终版.docx"


def remove_paragraph(paragraph):
    p = paragraph._element
    p.getparent().remove(p)
    p._p = p._element = None


doc = Document(SRC)

for p in list(doc.paragraphs):
    t = p.text.strip()
    if t.startswith("作为补充性检验，LDA 主题提取将帖子文本大体归纳为四类内容主题"):
        remove_paragraph(p)
        break

doc.save(OUT)
print(OUT)
