# 3_regression_analysis.py (v2 - Removed Huitun Index & English Labels)
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import statsmodels.api as sm

# --- 设置中文字体 (用于报告生成，如果需要) ---
# 这一部分依然保留，以防万一其他地方需要中文
try:
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
except:
    try:
        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
        plt.rcParams['axes.unicode_minus'] = False
    except:
        print("警告：未找到SimHei或Microsoft YaHei字体。")

def calculate_derived_metrics(df):
    """一个独立的函数，用于计算所有派生指标。"""
    df['商业笔记占比'] = (df['商业笔记总数'] / df['笔记总数'].replace(0, np.nan)).round(4)
    df['藏赞比'] = (df['近60天平均收藏'] / df['近60天平均点赞'].replace(0, np.nan)).round(4)
    df['评赞比'] = (df['近60天平均评论'] / df['近60天平均点赞'].replace(0, np.nan)).round(4)
    return df

def analyze_key_factors(df, output_csv, output_report, output_heatmap_image):
    """
    通过相关性和回归分析，探索影响达人报价的关键因素。
    """
    # --- 1. 相关性分析 ---
    # **修改点**: 移除了 '灰豚指数'
    corr_features = [
        '视频笔记报价', '粉丝数', '近60天平均点赞', 
        '活跃粉丝占比', '藏赞比', '商业笔记占比'
    ]
    
    for col in corr_features:
        df[col] = df[col].fillna(df[col].median())
        
    correlation_matrix = df[corr_features].corr()

    # **修改点**: 创建英文标签用于绘图
    english_labels = {
        '视频笔记报价': 'Video Price',
        '粉丝数': 'Followers',
        '近60天平均点赞': 'Avg Likes (60d)',
        '活跃粉丝占比': 'Active Fan Ratio',
        '藏赞比': 'Save/Like Ratio',
        '商业笔记占比': 'Commercial Post Ratio'
    }
    
    # 复制相关性矩阵并重命名，以便在图表上显示英文
    corr_matrix_english = correlation_matrix.rename(columns=english_labels, index=english_labels)
    
    plt.figure(figsize=(10, 8))
    # 使用重命名后的矩阵进行绘图
    sns.heatmap(corr_matrix_english, annot=True, cmap='coolwarm', fmt=".2f")
    plt.title('Correlation Heatmap of Key Metrics')
    plt.savefig(output_heatmap_image)
    plt.close()
    print(f"✅ 相关性热力图已保存到: {output_heatmap_image}")

    # --- 2. 回归分析 ---
    df_regr = df.dropna(subset=['视频笔记报价'])
    Y = df_regr['视频笔记报价']
    
    # **修改点**: 从自变量中移除了 '灰豚指数'
    X_cols = ['粉丝数', '活跃粉丝占比', '近60天平均点赞']
    X = df_regr[X_cols]
    
    X = sm.add_constant(X)
    model = sm.OLS(Y, X).fit()
    
    # --- 3. 寻找价值偏差达人 ---
    df_regr['模型预估报价'] = model.predict(X).round(2)
    df_regr['报价偏差'] = (df_regr['视频笔记报价'] - df_regr['模型预估报价']).round(2)

    # **修改点**: 从输出列中移除了 '灰豚指数'
    df_pricing_analysis = df_regr[[
        '达人名称', '视频笔记报价', '模型预估报价', '报价偏差', '粉丝数'
    ]].sort_values(by='报价偏差', ascending=True)

    df_pricing_analysis.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"✅ 达人报价分析结果已保存到: {output_csv}")

    # --- 4. 生成分析报告 ---
    with open(output_report, 'w', encoding='utf-8') as f:
        f.write("="*50 + "\n")
        f.write("  方向三：关键成功因素探索分析报告 (V2 - 移除灰豚指数)\n")
        f.write("="*50 + "\n\n")

        f.write("--- 1. 相关性分析 ---\n")
        f.write("热力图已保存为 " + output_heatmap_image + " 文件 (标签已英文化)。\n")
        f.write(f"- '视频笔记报价' 与 '粉丝数' 的相关性为: {correlation_matrix.loc['视频笔记报价', '粉丝数']:.2f}\n")
        f.write(f"- '视频笔记报价' 与 '近60天平均点赞' 的相关性为: {correlation_matrix.loc['视频笔记报价', '近60天平均点赞']:.2f}\n\n")
        
        f.write("--- 2. 回归模型分析 ---\n")
        f.write("我们建立了一个移除 '灰豚指数' 的新模型，来观察基础数据对报价的影响。\n\n")
        f.write("模型摘要:\n")
        f.write(str(model.summary()) + "\n\n")
        
        f.write("模型解读:\n")
        f.write(f"- R-squared值为 {model.rsquared:.2f}，说明新模型可以解释约 {model.rsquared*100:.0f}% 的报价变动。\n")
        f.write("- 从 coef (系数)列可以看出：\n")
        # **修改点**: 更新了解读，移除了灰豚指数
        f.write(f"  - 粉丝数每增加1个，报价平均增加 {model.params['粉丝数']:.2f} 元。\n")
        f.write(f"  - 活跃粉丝占比每提高1个点(0.01)，报价平均增加 {model.params['活跃粉丝占比']*0.01:.2f} 元。\n")
        f.write(f"  - 近60天平均点赞每增加1个，报价平均增加 {model.params['近60天平均点赞']:.2f} 元。\n\n")

        f.write("--- 3. 价值偏差分析 ---\n")
        f.write("Top 10 可能被低估的达人 (实际报价远低于新模型预估):\n")
        f.write(df_pricing_analysis.head(10).to_markdown(index=False) + "\n\n")
        f.write("Top 10 可能被高估的达人 (实际报价远高于新模型预估):\n")
        f.write(df_pricing_analysis.tail(10).sort_values(by='报价偏差', ascending=False).to_markdown(index=False) + "\n\n")

    print(f"✅ 回归分析报告已生成: {output_report}")

# --- 主程序 ---
if __name__ == "__main__":
    df = pd.read_csv('combined_cleaned_final.csv')
    df = calculate_derived_metrics(df)
    analyze_key_factors(
        df=df,
        output_csv='daren_pricing_analysis_v2.csv',
        output_report='regression_analysis_report_v2.txt',
        output_heatmap_image='correlation_heatmap_english.png'
    )