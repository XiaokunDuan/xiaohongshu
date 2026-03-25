"""
7_recluster_k3.py
剔除评赞比离群值后，重新以K=3跑聚类分析
包含：肘部法则选K、聚类结果、结果输出覆盖 daren_clusters_k3.csv
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import os
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# ── 字体 ──────────────────────────────────────────────────────────────
def get_font():
    for p in ['/System/Library/Fonts/PingFang.ttc',
              '/System/Library/Fonts/STHeiti Light.ttc']:
        if os.path.exists(p):
            return fm.FontProperties(fname=p)
    return None

FP = get_font()
OUT = os.path.join('..', 'reports')

# ══════════════════════════════════════════════════════════════════════
# 1. 载入数据 & 剔除离群值
# ══════════════════════════════════════════════════════════════════════
df = pd.read_csv(os.path.join('..', 'data', 'daren_clusters.csv'))
print(f"原始数据: {len(df)} 条")

# 用均值+3σ作为上界剔除评赞比极端值（学术惯用方法）
mean_er = df['评赞比'].mean()
std_er  = df['评赞比'].std()
threshold_er = mean_er + 3 * std_er

mean_biz = df['商业笔记占比'].mean()
std_biz  = df['商业笔记占比'].std()
threshold_biz = mean_biz + 3 * std_biz

mask_outlier = (df['评赞比'] > threshold_er) | (df['商业笔记占比'] > threshold_biz)
outliers = df[mask_outlier]
print(f"评赞比阈值（均值+3σ）: {threshold_er:.4f}")
print(f"商业笔记占比阈值（均值+3σ）: {threshold_biz:.4f}")
print(f"剔除离群值共 {len(outliers)} 条")

df_clean = df[~mask_outlier].copy()
print(f"清洗后数据: {len(df_clean)} 条\n")

# ══════════════════════════════════════════════════════════════════════
# 2. 特征选择 & 标准化
# ══════════════════════════════════════════════════════════════════════
features = ['粉丝数', 'Top1年龄段占比', '藏赞比', '评赞比', '商业笔记占比']
X = df_clean[features].fillna(df_clean[features].median())
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# ══════════════════════════════════════════════════════════════════════
# 3. 肘部法则 + 轮廓系数 选 K
# ══════════════════════════════════════════════════════════════════════
inertias = []
silhouettes = []
K_range = range(2, 9)

for k in K_range:
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = km.fit_predict(X_scaled)
    inertias.append(km.inertia_)
    silhouettes.append(silhouette_score(X_scaled, labels))

fig, axes = plt.subplots(1, 2, figsize=(12, 4))

axes[0].plot(list(K_range), inertias, 'bo-', linewidth=2)
axes[0].axvline(x=3, color='red', linestyle='--', alpha=0.7)
if FP:
    axes[0].set_title('肘部法则（Elbow Method）', fontproperties=FP, fontsize=13)
    axes[0].set_xlabel('K值（聚类数）', fontproperties=FP)
    axes[0].set_ylabel('组内惯性（Inertia）', fontproperties=FP)
else:
    axes[0].set_title('Elbow Method')
    axes[0].set_xlabel('K')
    axes[0].set_ylabel('Inertia')

axes[1].plot(list(K_range), silhouettes, 'rs-', linewidth=2)
axes[1].axvline(x=3, color='red', linestyle='--', alpha=0.7)
if FP:
    axes[1].set_title('轮廓系数（Silhouette Score）', fontproperties=FP, fontsize=13)
    axes[1].set_xlabel('K值（聚类数）', fontproperties=FP)
    axes[1].set_ylabel('轮廓系数', fontproperties=FP)
else:
    axes[1].set_title('Silhouette Score')
    axes[1].set_xlabel('K')
    axes[1].set_ylabel('Score')

plt.tight_layout()
elbow_path = os.path.join(OUT, 'elbow_silhouette.png')
plt.savefig(elbow_path, dpi=150, bbox_inches='tight')
plt.close()
print(f"肘部法则图已保存: {elbow_path}")

print("\n各K值的轮廓系数:")
for k, s in zip(K_range, silhouettes):
    print(f"  K={k}: {s:.4f}")

# ══════════════════════════════════════════════════════════════════════
# 4. 最终聚类：K=3
# ══════════════════════════════════════════════════════════════════════
K_FINAL = 3
km_final = KMeans(n_clusters=K_FINAL, random_state=42, n_init=10)
df_clean['群体标签_k3'] = km_final.fit_predict(X_scaled)

print(f"\n====== K={K_FINAL} 聚类结果 ======")
print("\n各群体规模:")
print(df_clean['群体标签_k3'].value_counts().sort_index().to_string())

print("\n各群体特征画像（中位数）:")
profile = df_clean.groupby('群体标签_k3')[features + ['视频笔记报价','视频CPE','活跃粉丝占比']].median().round(3)
profile['群体规模'] = df_clean['群体标签_k3'].value_counts().sort_index()
print(profile.to_string())

# ══════════════════════════════════════════════════════════════════════
# 5. 保存结果
# ══════════════════════════════════════════════════════════════════════
out_csv = os.path.join('..', 'data', 'daren_clusters_k3.csv')
df_clean.to_csv(out_csv, index=False)
print(f"\n结果已保存: {out_csv}")
print(f"（离群值 {len(outliers)} 条已从文件中剔除）")
