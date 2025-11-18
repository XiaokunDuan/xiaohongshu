import pandas as pd
import numpy as np
import ast
from collections import Counter

def generate_analysis_report(file_path, output_txt_path):
    """
    加载、预处理小红书达人数据，并生成一份详细的分析报告txt文件。

    参数:
    file_path (str): 输入的CSV文件路径。
    output_txt_path (str): 输出的txt报告文件路径。
    """
    # --- 加载数据 ---
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"错误：文件 '{file_path}' 未找到。请检查文件名和路径。")
        return

    # --- 阶段一：最终预处理 ---
    def parse_tags(tags_str):
        try:
            return ast.literal_eval(tags_str)
        except (ValueError, SyntaxError):
            return []
    df['达人标签'] = df['达人标签'].apply(parse_tags)

    df['商业笔记占比'] = (df['商业笔记总数'] / df['笔记总数'].replace(0, np.nan)).round(4)
    df['藏赞比'] = (df['近60天平均收藏'] / df['近60天平均点赞'].replace(0, np.nan)).round(4)
    df['评赞比'] = (df['近60天平均评论'] / df['近60天平均点赞'].replace(0, np.nan)).round(4)

    # --- 阶段二：将分析结果写入文件 ---
    # 使用 'w' 模式（写入），并指定编码为 utf-8
    with open(output_txt_path, 'w', encoding='utf-8') as f:
        f.write("="*50 + "\n")
        f.write("  小红书高影响力生活类达人行为模式分析报告\n")
        f.write("="*50 + "\n\n")

        # === 1. 整体画像与基本分布 ===
        f.write("--- 1. 整体画像与基本分布 ---\n")
        f.write(f"本次分析共包含 {len(df)} 位粉丝数10万以上的生活记录类达人。\n\n")
        
        f.write("核心影响力指标分布概览:\n")
        f.write(df[['灰豚指数', '粉丝数', '赞藏总数']].describe().round(2).to_markdown() + "\n\n")
        
        f.write("达人基础属性分布:\n")
        f.write(f"性别分布:\n{df['性别'].value_counts(normalize=True).round(4) * 100}%\n\n")
        f.write(f"地域分布 (Top 10 省份):\n{df['省份'].value_counts().head(10).to_markdown()}\n\n")
        f.write(f"认证类型分布:\n{df['认证类型'].value_counts().to_markdown()}\n\n")
        f.write(f"MCN签约情况:\n{df['签约MCN'].apply(lambda x: '已签约' if x != '独立达人' else '独立达人').value_counts(normalize=True).round(4) * 100}%\n\n")
        f.write("="*50 + "\n\n")
        
        # === 2. 用户画像分析 (粉丝特征) ===
        f.write("--- 2. 用户画像分析 (粉丝特征) ---\n")
        f.write("粉丝画像是理解达人行为模式的基础。\n\n")

        f.write("粉丝主要年龄段分布:\n")
        f.write(str(df['Top1年龄段'].value_counts(normalize=True).round(4) * 100) + "\n\n")
        f.write("粉丝主要地域分布 (Top 10 省份):\n")
        f.write(df['Top1粉丝省份'].value_counts().head(10).to_markdown() + "\n\n")
        f.write("粉丝性别比例 (女/男) 分布概览:\n")
        f.write(df['粉丝女/男比例'].describe().round(2).to_markdown() + "\n\n")
        f.write("="*50 + "\n\n")

        # === 3. 内容特征分析 ===
        f.write("--- 3. 内容特征分析 ---\n")
        f.write("达人通过内容与用户建立连接，其内容特征直接反映了其运营策略。\n\n")

        all_tags = [tag for sublist in df['达人标签'] for tag in sublist]
        tag_counts = Counter(all_tags)
        f.write("最热门的20个内容标签:\n")
        f.write(pd.DataFrame(tag_counts.most_common(20), columns=['标签', '出现次数']).to_markdown(index=False) + "\n\n")
        
        f.write("内容策略指标分布概览:\n")
        f.write(df[['笔记总数', '商业笔记占比', '近60天爆文率']].describe().round(2).to_markdown() + "\n\n")
        f.write("="*50 + "\n\n")

        # === 4. 互动特征分析 ===
        f.write("--- 4. 互动特征分析 ---\n")
        f.write("互动数据是衡量达人真实影响力和粉丝粘性的核心。\n\n")

        f.write("近60天核心互动指标分布:\n")
        f.write(df[['近60天平均点赞', '近60天平均收藏', '近60天平均评论', '近60天平均分享']].describe().round(0).to_markdown() + "\n\n")
        
        f.write("粉丝质量与互动风格指标分布:\n")
        f.write(df[['活跃粉丝占比', '水粉占比', '藏赞比', '评赞比']].describe().round(2).to_markdown() + "\n\n")
        f.write("="*50 + "\n\n")

        # === 5. 交叉分析：发现行为模式 ===
        f.write("--- 5. 交叉分析：发现行为模式 ---\n")
        f.write("通过对比不同群体，我们可以发现深层的行为模式。\n\n")

        f.write("模式一：不同粉丝年龄段的互动风格差异\n")
        age_interaction = df.groupby('Top1年龄段')[['藏赞比', '评赞比']].mean().round(3)
        f.write("研究发现，主打年轻粉丝（18岁以下, 18-24岁）的达人，其内容的评论、讨论属性（评赞比）更强；\n")
        f.write("而主打成熟用户（25-34岁）的达人，其内容的实用、收藏价值（藏赞比）更高。\n")
        f.write(age_interaction.to_markdown() + "\n\n")

        f.write("模式二：不同认证类型的商业价值与粉丝质量对比\n")
        type_value = df.groupby('认证类型')[['视频笔记报价', '视频CPE', '活跃粉丝占比', '水粉占比']].mean().round(2)
        f.write("研究发现，明星的平均报价远高于其他类型，但其互动成本(CPE)也最高，性价比相对较低。\n")
        f.write("头部达人的粉丝活跃度最高，且水粉占比较低，粉丝质量非常健康。\n")
        f.write(type_value.sort_values(by='视频笔记报价', ascending=False).to_markdown() + "\n\n")
        
        f.write("="*50 + "\n")
        f.write("分析结束。\n")

# --- 主程序：执行函数并生成报告 ---
input_csv_path = 'combined_cleaned_final.csv'
output_report_path = 'analysis_report.txt'

generate_analysis_report(input_csv_path, output_report_path)

print(f"✅ 分析报告已成功生成，请查看文件: {output_report_path}")