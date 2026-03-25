# 4_nlp_content_analysis.py (ULTIMATE V2 - Focusing on the Core Community)

import pandas as pd
import jieba
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import networkx as nx
from networkx.algorithms import community
import ast
from collections import Counter
import itertools
import re
import os
from matplotlib.font_manager import FontProperties

def find_mac_font():
    """在Mac系统中自动查找可用的中文字体"""
    font_paths = ['/System/Library/Fonts/PingFang.ttc', '/System/Library/Fonts/STHeiti Light.ttc']
    for path in font_paths:
        if os.path.exists(path):
            print(f"找到可用Mac中文字体: {path}")
            return path
    print("警告：未在常用路径找到中文字体。")
    return None

def generate_bio_wordcloud(df, font_path):
    """生成词云图（保持不变）"""
    print("--- 正在生成达人简介词云图... ---")
    text = ''.join(df['简介'].dropna().astype(str))
    text = re.sub(r'[a-zA-Z0-9\.\/:_@]+', '', text)
    stopwords = {'的', '是', '我', '你', '了', '都', '在', '也', '我们', '一个', '不', '就', '会', '请', '看', 'VX', 'vx', '合作', '联系', '邮箱', '商务', '工作', 'v', '一个', '这里', '主页', '小红', '书'}
    words = jieba.cut(text)
    filtered_words = [word for word in words if word not in stopwords and len(word) > 1]
    if not font_path: return
    wc = WordCloud(font_path=font_path, width=800, height=600, background_color='white', max_words=100, colormap='viridis').generate(' '.join(filtered_words))
    plt.figure(figsize=(10, 8))
    plt.imshow(wc, interpolation='bilinear')
    plt.axis('off')
    title_font = FontProperties(fname=font_path, size=20)
    plt.title('高影响力达人简介高频词', fontproperties=title_font)
    output_filename = os.path.join('..', 'reports', 'bio_wordcloud.png')
    plt.savefig(output_filename)
    plt.close()
    print(f"✅ 简介词云图已成功保存到: {output_filename}")


def generate_tag_network_core(df, font_path, top_n=100):
    """
    生成终极版、聚焦于核心社群（最大连通分量）的内容网络图。
    """
    print("\n--- 正在生成聚焦核心社群的终极网络图... ---")
    
    df['达人标签_list'] = df['达人标签'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else [])
    filtered_tag_lists = df['达人标签_list'].apply(lambda tags: [tag for tag in tags if not tag.endswith('其他')])
    tag_lists = filtered_tag_lists[filtered_tag_lists.apply(len) > 1]
    pairs = [tuple(sorted(p)) for tags in tag_lists for p in itertools.combinations(tags, 2)]
    pair_counts = Counter(pairs)
    
    G_full = nx.Graph()
    for pair, weight in pair_counts.most_common(top_n):
        G_full.add_edge(pair[0], pair[1], weight=weight)
        
    if '接地气生活' in G_full:
        G_full.remove_node('接地气生活')
        print("已移除超级核心节点 '接地气生活'。")

    # **终极优化步骤1: 找出并只保留最大连通分量**
    connected_components = list(nx.connected_components(G_full))
    if not connected_components:
        print("图中没有节点，无法生成。")
        return
        
    largest_component = max(connected_components, key=len)
    G = G_full.subgraph(largest_component).copy() # 使用.copy()确保它是一个独立的图
    print(f"已聚焦于最大连通分量，核心节点数: {len(G.nodes())}/{len(G_full.nodes())}")

    # **终极优化步骤2: 在核心社群内再次进行社群发现**
    communities = community.louvain_communities(G, seed=42)
    print(f"在核心社群内自动发现 {len(communities)} 个子社群。")

    # **终极优化步骤3: 视觉编码**
    colormap = plt.get_cmap('viridis', len(communities)) # 使用离散的颜色
    community_map = {}
    for i, comm in enumerate(communities):
        color = colormap(i)
        for node in comm:
            community_map[node] = color
    node_colors = [community_map.get(node, 'grey') for node in G.nodes()]

    plt.figure(figsize=(20, 20))
    pos = nx.spring_layout(G, k=2.0, iterations=100, seed=42)
    
    node_sizes = [G.degree(node) * 300 + 200 for node in G.nodes()]
    edge_widths = [G[u][v]['weight'] * 0.5 for u, v in G.edges()]
    
    nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color=node_colors, alpha=0.8)
    nx.draw_networkx_edges(G, pos, width=edge_widths, edge_color='lightgrey', alpha=0.7)
    
    if font_path:
        label_font = FontProperties(fname=font_path, size=14)
        for node, (x, y) in pos.items():
            plt.text(x, y, node, fontproperties=label_font, ha='center', va='center', bbox=dict(facecolor='white', alpha=0.5, edgecolor='none', boxstyle='round,pad=0.2'))
            
        title_font = FontProperties(fname=font_path, size=24)
        plt.title('小红书核心内容社群网络图 (最大连通分量)', fontproperties=title_font)
    else:
        nx.draw_networkx_labels(G, pos)
        plt.title('Core Content Community Network (Largest Connected Component)', size=20)

    plt.axis('off')
    output_filename = os.path.join('..', 'reports', 'tag_network_core.png')
    plt.savefig(output_filename, bbox_inches='tight')
    plt.close()
    print(f"✅ 聚焦核心的终极版网络图已成功保存到: {output_filename}")

if __name__ == "__main__":
    input_file = os.path.join('..', 'data', 'combined_cleaned_final.csv')
    try:
        main_df = pd.read_csv(input_file)
        mac_font_path = find_mac_font()
        
        if mac_font_path:
            # 词云图可以酌情注释掉，因为它已经生成过了
            # generate_bio_wordcloud(main_df, font_path=mac_font_path) 
            generate_tag_network_core(main_df, font_path=mac_font_path, top_n=100)
        else:
            print("未能找到所需的中文字体。")
    except FileNotFoundError:
        print(f"错误：找不到输入文件 '{input_file}'。")
    except Exception as e:
        print(f"程序运行时发生错误: {e}")