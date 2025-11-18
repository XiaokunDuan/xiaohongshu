# 1_value_score_analysis.py (FIXED)

import pandas as pd
import numpy as np

def calculate_derived_metrics(df):
    """一个独立的函数，用于计算所有派生指标。"""
    df['商业笔记占比'] = (df['商业笔记总数'] / df['笔记总数'].replace(0, np.nan)).round(4)
    df['藏赞比'] = (df['近60天平均收藏'] / df['近60天平均点赞'].replace(0, np.nan)).round(4)
    df['评赞比'] = (df['近60天平均评论'] / df['近60天平均点赞'].replace(0, np.nan)).round(4)
    return df

def analyze_daren_value(df, output_csv, output_report): # df作为参数传入
    """
    构建达人价值评估模型，寻找高性价比达人。
    """
    # --- 1. 定义和计算关键指标 ---
    median_cpe = df[df['视频CPE'] > 0]['视频CPE'].median()
    df['视频CPE_adj'] = df['视频CPE'].replace(0, median_cpe).fillna(median_cpe)
    
    cost_benefit = 1 / df['视频CPE_adj']
    df['互动成本指数'] = (cost_benefit - cost_benefit.min()) / (cost_benefit.max() - cost_benefit.min())

    df['粉丝质量指数'] = df['活跃粉丝占比'] * (1 - df['水粉占比'])

    # 这里现在可以安全地使用了
    df['内容吸引力指数'] = df['藏赞比'] + df['评赞比'] 
    df['内容吸引力指数'] = df['内容吸引力指数'].fillna(0)
    df['内容吸引力指数'] = (df['内容吸引力指数'] - df['内容吸引力指数'].min()) / (df['内容吸引力指数'].max() - df['内容吸引力指数'].min())

    # --- 后续代码不变... ---
    w_fan_quality = 0.6
    w_content_appeal = 0.4
    df['综合价值分'] = ((df['粉丝质量指数'] * w_fan_quality) + (df['内容吸引力指数'] * w_content_appeal)) * (df['互动成本指数'] + 0.1)
    df['综合价值分'] = ((df['综合价值分'] - df['综合价值分'].min()) / (df['综合价值分'].max() - df['综合价值分'].min()) * 100).round(2)

    df_sorted = df.sort_values(by='综合价值分', ascending=False)
    output_columns = [
        '达人名称', '综合价值分', '粉丝数', '认证类型', '视频笔记报价', '视频CPE',
        '互动成本指数', '粉丝质量指数', '内容吸引力指数', '省份'
    ]
    df_final = df_sorted[output_columns]
    df_final.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"✅ 价值评估结果已保存到: {output_csv}")

    with open(output_report, 'w', encoding='utf-8') as f:
        f.write("="*50 + "\n")
        f.write("  方向一：达人价值评估模型分析报告\n")
        f.write("="*50 + "\n\n")
        f.write("--- Top 20 价值洼地达人 ---\n")
        f.write(df_final.head(20).to_markdown(index=False) + "\n\n")
        top_10_percent = df_final.head(int(len(df_final) * 0.1))
        f.write("--- 高价值达人 (Top 10%) 群体特征分析 ---\n")
        f.write("\n1. 认证类型分布:\n")
        f.write(top_10_percent['认证类型'].value_counts(normalize=True).round(4).to_markdown() + "\n\n")
        bins = [0, 200000, 500000, 1000000, 50000000]
        labels = ['10-20万', '20-50万', '50-100万', '100万+']
        top_10_percent['粉丝量级'] = pd.cut(top_10_percent['粉丝数'], bins=bins, labels=labels)
        f.write("2. 粉丝量级分布:\n")
        f.write(top_10_percent['粉丝量级'].value_counts(normalize=True).round(4).to_markdown() + "\n\n")
        f.write("3. 地域分布 (Top 5):\n")
        f.write(top_10_percent['省份'].value_counts().head(5).to_markdown() + "\n\n")
    print(f"✅ 价值评估分析报告已生成: {output_report}")

# --- 主程序 ---
if __name__ == "__main__":
    df = pd.read_csv('combined_cleaned_final.csv')
    df = calculate_derived_metrics(df) # 先计算派生指标
    analyze_daren_value(
        df=df, # 将处理好的df传入
        output_csv='daren_value_scores.csv',
        output_report='daren_value_report.txt'
    )