"""
8_log_recluster.py
对右偏变量做 log(1+x) 变换后重跑聚类
不删除任何数据点，用变换代替剔除
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import os
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

def get_font():
    for p in ['/System/Library/Fonts/PingFang.ttc',
              '/System/Library/Fonts/STHeiti Light.ttc']:
        if os.path.exists(p):
            return fm.FontProperties(fname=p)
    return None

FP = get_font()
OUT = os.path.join('..', 'reports')

# ── 载入原始聚类数据（含所有3403条）──────────────────────────────────
df = pd.read_csv(os.path.join('..', 'data', 'daren_clusters.csv'))
print(f"载入数据: {len(df)} 条")

# ── 特征选择 & log(1+x) 变换 ─────────────────────────────────────────
features_raw = ['粉丝数', 'Top1年龄段占比', '藏赞比', '评赞比', '商业笔记占比']
skewed      = ['粉丝数', '藏赞比', '评赞比', '商业笔记占比']   # 右偏变量做变换
normal_feat = ['Top1年龄段占比']                               # 分布较正常，不变换

X = df[features_raw].fillna(df[features_raw].median()).copy()
for col in skewed:
    X[col] = np.log1p(X[col])

print("变换后各特征分布（偏度）:")
for col in features_raw:
    print(f"  {col}: {X[col].skew():.3f}")

X_scaled = StandardScaler().fit_transform(X)

# ── 肘部法则 + 轮廓系数 ───────────────────────────────────────────────
inertias, silhouettes = [], []
K_range = range(2, 9)

for k in K_range:
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = km.fit_predict(X_scaled)
    inertias.append(km.inertia_)
    silhouettes.append(silhouette_score(X_scaled, labels))

print("\n各K值轮廓系数:")
for k, s in zip(K_range, silhouettes):
    marker = " <-- 推荐" if s == max(silhouettes) else ""
    print(f"  K={k}: {s:.4f}{marker}")

best_k = list(K_range)[silhouettes.index(max(silhouettes))]

# 画肘部图
fig, axes = plt.subplots(1, 2, figsize=(12, 4))
axes[0].plot(list(K_range), inertias, 'bo-', linewidth=2)
axes[1].plot(list(K_range), silhouettes, 'rs-', linewidth=2)
for ax, title, ylabel in zip(axes,
    ['肘部法则（对数变换后）', '轮廓系数（对数变换后）'],
    ['组内惯性', '轮廓系数']):
    if FP:
        ax.set_title(title, fontproperties=FP, fontsize=13)
        ax.set_xlabel('K值', fontproperties=FP)
        ax.set_ylabel(ylabel, fontproperties=FP)
    else:
        ax.set_title(title)
axes[0].axvline(x=best_k, color='red', linestyle='--', alpha=0.7)
axes[1].axvline(x=best_k, color='red', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig(os.path.join(OUT, 'elbow_log.png'), dpi=150, bbox_inches='tight')
plt.close()

# ── 用最优K跑最终聚类，同时也跑K=4方便对比 ───────────────────────────
for k in sorted(set([best_k, 4])):
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    df[f'cluster_k{k}'] = km.fit_predict(X_scaled)

    print(f"\n====== K={k} 聚类结果 ======")
    sizes = df[f'cluster_k{k}'].value_counts().sort_index()
    print("各群体规模:")
    print(sizes.to_string())

    print("\n各群体特征画像（原始值中位数）:")
    show_cols = features_raw + ['视频笔记报价', '视频CPE', '活跃粉丝占比']
    profile = df.groupby(f'cluster_k{k}')[show_cols].median().round(3)
    profile['规模'] = sizes
    print(profile.to_string())

# ── 保存最优K结果 ─────────────────────────────────────────────────────
best_col = f'cluster_k{best_k}'
df['群体标签_log'] = df[best_col]
out_csv = os.path.join('..', 'data', 'daren_clusters_log.csv')
df.to_csv(out_csv, index=False)
print(f"\n结果已保存: {out_csv}（最优K={best_k}）")
