import sys
sys.path.insert(0, "/Users/songbozhi/Documents/projects/conflux-massive-test")

from analyzer.log_metrics.log_data_manage import SingleNodeMetrics

# 传的是目录（里面要有 metrics.log）
node = SingleNodeMetrics.load("/tmp")

# 看看有哪些指标
names = node.get_all_metric_names()
for n in sorted(names):
    print(n)


import matplotlib.pyplot as plt
from datetime import datetime

# 查询某个指标的时序数据
timestamps, values = node.query_metric("txpool_pack_transactions::acquires.m15")

# 画图
dates = [datetime.fromtimestamp(ts/1000) for ts in timestamps]
plt.figure(figsize=(14, 5))
plt.plot(dates, values)
plt.title("acquires.m0")
plt.xlabel("Time")
plt.ylabel("rate")
plt.grid(True)
plt.show()