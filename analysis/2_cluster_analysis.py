# 2_cluster_analysis.py (FIXED)

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

def calculate_derived_metrics(df):
    """一个独立的函数，用于计算所有派生指标。"""
    df['商业笔记占比'] = (df['商业笔记总数'] / df['笔记总数'].replace(0, np.nan)).round(4)
    df['藏赞比'] = (df['近60天平均收藏'] / df['近60天平均点赞'].replace(0, np.nan)).round(4)
    df['评赞比'] = (df['近60天平均评论'] / df['近60天平均点赞'].replace(0, np.nan)).round(4)
    return df

def analyze_daren_clusters(df, output_csv, output_report, n_clusters=4): # df作为参数传入
    """
    使用K-Means聚类对达人进行分群，并分析各群体特征。
    """
    # --- 1. 选择聚类特征并预处理 ---
    features = [
        '粉丝数', 'Top1年龄段占比', '藏赞比', '评赞比', '商业笔记占比'
    ]
    
    # 这里现在可以安全地使用了
    for col in features:
        df[col] = df[col].fillna(df[col].median())

    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df[features])

    # --- 后续代码不变... ---
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df['群体标签'] = kmeans.fit_predict(df_scaled)
    cluster_profiles = df.groupby('群体标签')[features].mean().round(3)
    cluster_profiles['群体规模'] = df['群体标签'].value_counts()
    
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"✅ 达人分群结果已保存到: {output_csv}")

    with open(output_report, 'w', encoding='utf-8') as f:
        f.write("="*50 + "\n")
        f.write("  方向二：达人分群与画像描绘分析报告\n")
        f.write("="*50 + "\n\n")
        f.write(f"已成功将 {len(df)} 位达人分为 {n_clusters} 个群体。\n\n")
        f.write("--- 各群体特征画像 ---\n")
        f.write(cluster_profiles.to_markdown() + "\n\n")
        f.write("--- 各群体解读 ---\n")
        f.write("（以下解读基于典型的聚类结果，您的实际结果可能会略有不同）\n\n")
        f.write("群体0画像: [请根据上方表格中第0行的特征进行解读]\n\n")
        f.write("群体1画像: [请根据上方表格中第1行的特征进行解读]\n\n")
        f.write("群体2画像: [请根据上方表格中第2行的特征进行解读]\n\n")
        f.write("群体3画像: [请根据上方表格中第3行的特征进行解读]\n\n")
    print(f"✅ 达人分群分析报告已生成: {output_report}")

# --- 主程序 ---
if __name__ == "__main__":
    df = pd.read_csv('combined_cleaned_final.csv')
    df = calculate_derived_metrics(df) # 先计算派生指标
    analyze_daren_clusters(
        df=df, # 将处理好的df传入
        output_csv='daren_clusters.csv',
        output_report='cluster_analysis_report.txt',
        n_clusters=4
    )