import pandas as pd
import numpy as np
import re
import os

def preprocess_xiaohongshu_data(file_path):
    """
    对小红书达人数据进行最终的、精细化的预处理。
    """
    try:
        df = pd.read_csv(file_path)
        print(f"成功加载文件: {file_path}")
    except FileNotFoundError:
        print(f"错误：文件未找到，请检查路径 '{file_path}' 是否正确。")
        return None

    # --- 2. 初步清洗 & 移除特殊字符 ---
    df.replace('--', np.nan, inplace=True)
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()
            # 移除不寻常的行终止符并替换为标准空格
            df[col] = df[col].str.replace(r'[\u2028\u2029]', ' ', regex=True)
            # **优化：将连续的多个空格替换为单个空格**
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True)

    # --- 3. 标准化与类型转换 ---
    df['签约MCN'].fillna('独立达人', inplace=True)
    df['品牌合作人'] = df['品牌合作人'].apply(lambda x: True if x == '是' else False)
    df['更新时间'] = pd.to_datetime(df['更新时间'], errors='coerce')

    numerical_cols = [
        '灰豚指数', '粉丝数', '赞藏总数', '笔记总数', '商业笔记总数',
        '图文笔记报价', '图文CPE', '图文CPM', '视频笔记报价', '视频CPE', '视频CPM',
        '近60天平均点赞', '近60天平均收藏', '近60天平均评论', '近60天平均分享',
        '活跃粉丝占比', '水粉占比', '粉丝男/女'
    ]
    for col in numerical_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 百分比转换并四舍五入
    df[['活跃粉丝占比', '水粉占比']] = (df[['活跃粉丝占比', '水粉占比']] / 100.0).round(4)
    df['近60天爆文率'] = (pd.to_numeric(df['近60天爆文率'], errors='coerce').fillna(0) / 100.0).round(4)

    # ... 其他字段填充与类型转换 ...
    fill_zero_cols = [
        '商业笔记总数', '近60天平均点赞', '近60天平均收藏', 
        '近60天平均评论', '近60天平均分享', '图文CPE', '图文CPM', 
        '视频CPE', '视频CPM'
    ]
    df[fill_zero_cols] = df[fill_zero_cols].fillna(0)
    int_cols = ['粉丝数', '赞藏总数', '笔记总数', '商业笔记总数', '近60天平均点赞', 
                '近60天平均收藏', '近60天平均评论', '近60天平均分享']
    for col in int_cols:
        df[col] = df[col].astype('Int64')
        
    # --- 4. 特征工程 ---
    df[['省份', '城市']] = df['地域'].str.split(' ', n=1, expand=True)
    df['Top1粉丝省份'] = df['粉丝地域'].str.extract(r'(\D+)\d+')[0]
    df['Top1省份占比'] = (df['粉丝地域'].str.extract(r'(\d+\.\d+)%').astype(float) / 100.0).round(4)
    df['Top1年龄段'] = df['粉丝年龄'].str.split('|', n=1, expand=True)[0]
    df['Top1年龄段占比'] = (df['粉丝年龄'].str.extract(r'(\d+\.\d+)%').astype(float) / 100.0).round(4)
    active_time_df = df['粉丝活跃时间'].str.extract(r'(\d{2}:\d{2})\|.*? (周.)\|', expand=True)
    df['最活跃小时'] = active_time_df[0]
    df['最活跃星期'] = active_time_df[1]
    focus_df = df['粉丝关注焦点'].str.extract(r'([^|]+)\|(\d+\.\d+)%\s*([^|]*)\|?(\d*\.?\d*)%?\s*([^|]*)\|?(\d*\.?\d*)%?', expand=True)
    df['Top1关注焦点'] = focus_df[0]
    df['Top1关注焦点占比'] = (pd.to_numeric(focus_df[1], errors='coerce') / 100.0).round(4)
    df['Top2关注焦点'] = focus_df[2]
    df['Top2关注焦点占比'] = (pd.to_numeric(focus_df[3], errors='coerce') / 100.0).round(4)
    df['Top3关注焦点'] = focus_df[4]
    df['Top3关注焦点占比'] = (pd.to_numeric(focus_df[5], errors='coerce') / 100.0).round(4)
    
    # **优化：清理每个标签的前后空格**
    df['达人标签'] = df['达人标签'].apply(
        lambda x: [tag.strip() for tag in x.strip('[]').split(',')] if isinstance(x, str) else []
    )

    # --- 5. 重命名与整理 ---
    df.rename(columns={'粉丝男/女': '粉丝女/男比例'}, inplace=True)
    
    print("\n--- 数据预处理完成 ---")
    print("最终生成的数据包含 {} 行 和 {} 列。".format(df.shape[0], df.shape[1]))
    
    return df

# --- 主程序：执行并保存 ---
input_file_path = '/Users/dxk/xiaohongshu/combined.csv' 
df_processed = preprocess_xiaohongshu_data(input_file_path)

if df_processed is not None:
    directory = os.path.dirname(input_file_path)
    output_filename = 'combined_cleaned_final.csv' # 使用新文件名以作区分
    output_file_path = os.path.join(directory, output_filename)
    
    try:
        df_processed.to_csv(output_file_path, index=False, encoding='utf-8-sig')
        print(f"\n✅ 最终优化后的文件已成功保存到:\n{output_file_path}")
    except Exception as e:
        print(f"\n❌ 保存文件时出错: {e}")