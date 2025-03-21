import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 读取CSV文件
df = pd.read_csv("backup.csv")

# 计算总带宽 (prefill_tput + decode_tput)
df['total_bandwidth'] = df['prefill_tput'] + df['decode_tput']

# 获取唯一的value_size和线程数
value_sizes = sorted(df['value_size'].unique())
threads = sorted(df['threads'].unique())
engines = df['engine'].unique()

# 创建图形
fig, ax = plt.subplots(figsize=(12, 7))

# 设置颜色映射
colors = {'redis': plt.cm.Reds(np.linspace(0.4, 0.8, len(threads))),
          'mooncake': plt.cm.Blues(np.linspace(0.4, 0.8, len(threads)))}

# 每个分组的柱状图数量
n_bars = len(threads) * len(engines)
width = 0.8 / n_bars  # 每个柱状图的宽度

# 遍历不同的value_size进行绘制
for i, value_size in enumerate(value_sizes):
    # 计算当前value_size分组的中心位置
    x_center = i
    
    # 绘制每个引擎和线程组合的柱状图
    bar_index = 0
    for engine in engines:
        for j, thread in enumerate(threads):
            # 提取特定引擎、value_size和线程数的数据
            data = df[(df['engine'] == engine) & 
                      (df['value_size'] == value_size) & 
                      (df['threads'] == thread)]
            
            if not data.empty:
                # 计算柱状图的x位置
                x_pos = x_center - 0.4 + width * (bar_index + 0.5)
                
                # 绘制柱状图
                label = f'{engine}, {thread} threads' if i == 0 else ""
                ax.bar(x_pos, data['total_bandwidth'].values[0], width, 
                       label=label, color=colors[engine][j])
                
                # 在柱子上方添加数值标签
                ax.text(x_pos, data['total_bandwidth'].values[0] + 0.2, 
                        f'{data["total_bandwidth"].values[0]:.1f}', 
                        ha='center', va='bottom', fontsize=7, rotation=45)
                
            bar_index += 1

# 添加虚线表示理论带宽上限
tcp_limit = 1.8875  # TCP 带宽上限 (GB/s)
rdma_limit = 12.25  # RDMA 带宽上限 (GB/s)
ax.axhline(y=tcp_limit, color='red', linestyle='--', label="TCP Limit (1.8875GB/s)")
ax.axhline(y=rdma_limit, color='blue', linestyle='--', label="RDMA Limit (12.25GB/s)")

# 设置坐标轴标签
ax.set_xlabel("Value Size (bytes)")
ax.set_ylabel("Total Bandwidth (GB/s)")

# 设置X轴为value_size并添加刻度
ax.set_xticks(np.arange(len(value_sizes)))
ax.set_xticklabels([f"{size}" for size in value_sizes])

# 图例
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

# 美化图形
ax.set_title("Comparison of Performance: Redis vs Mooncake")
ax.grid(True, linestyle='--', alpha=0.5)

# 保存图像为PNG文件
plt.tight_layout()
plt.savefig("result.png")

# 显示图像
plt.show()
