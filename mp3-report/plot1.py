import numpy as np
import matplotlib.pyplot as plt

# Data (in KiB/s and ms)
data = {
    "1KiB": [
        {"bandwidths": [75.24, 0.50, 75.30], "times": [7.9, 2, 21.24]},
        {"bandwidths": [69.26, 13.98, 51.86], "times": [10.944736, 5.698834, 8.070295]},
        {"bandwidths": [155.60, 27.45, 88.75], "times": [13.642875, 9.206113, 6.187706]},
    ],
    "10KiB": [
        {"bandwidths": [567.81, 135.38, 472.47], "times": [37.358005, 12.3804, 25.56412]},
        {"bandwidths": [95.51, 41.02, 351.33], "times": [10.312777, 5.346309, 22.092544]},
        {"bandwidths": [1013.76, 324.30, 607.28], "times": [26.537489, 23.457063, 33.535936]},
    ],
    "100KiB": [
        {"bandwidths": [5211.08, 1068.28, 3070.68], "times": [78.169006, 42.251963, 108.545919]},
        {"bandwidths": [8952.77, 2001.12, 6675.68], "times": [90.579631, 80.933016, 224.666386]},
        {"bandwidths": [7207.87, 1334.74, 5340.07], "times": [75.274998, 53.152827, 188.85129]},
    ],
    "500KiB": [
        {"bandwidths": [4665.23, 2668.17, 27992.08], "times": [175.872599, 109.809627, 893.170357]},
        {"bandwidths": [19333.41, 9998.38, 25997.20], "times": [619.126823, 339.360591, 865.149885]},
        {"bandwidths": [8677.54, 2000.36, 8675.77], "times": [299.990411, 77.50783, 305.354088]},
    ],
    "1MiB": [
        {"bandwidths": [39717.00, 10930.08, 21858.85], "times": [590.152378, 389.091074, 767.346425]},
        {"bandwidths": [83881.11, 23204.65, 48922.69], "times": [2552.79801, 786.324434, 2192.011508]},
        {"bandwidths": [65559.59, 10916.60, 2047.34], "times": [995.833776, 389.354704, 2245.278144]},
    ],
}

sizes = list(data.keys())
avg_bandwidths, std_bandwidths = [], []
avg_times, std_times = [], []

# Compute totals and statistics
for size in sizes:
    readings = data[size]
    total_bandwidths = [sum(r["bandwidths"]) for r in readings]
    max_times = [max(r["times"]) for r in readings]

    avg_bandwidths.append(np.mean(total_bandwidths))
    std_bandwidths.append(np.std(total_bandwidths))
    avg_times.append(np.mean(max_times))
    std_times.append(np.std(max_times))

# Plot dual-axis graph
fig, ax1 = plt.subplots(figsize=(9, 5))

# Left Y-axis for system bandwidth
ax1.errorbar(sizes, avg_bandwidths, yerr=std_bandwidths, fmt='-o', capsize=5, color='tab:blue', label='System Bandwidth (KiB/s)')
ax1.set_xlabel("File Size")
ax1.set_ylabel("System Bandwidth (KiB/s)", color='tab:blue')
ax1.tick_params(axis='y', labelcolor='tab:blue')

# Right Y-axis for re-replication time
ax2 = ax1.twinx()
ax2.errorbar(sizes, avg_times, yerr=std_times, fmt='-s', capsize=5, color='tab:orange', label='Re-replication Time (ms)')
ax2.set_ylabel("Re-replication Time (ms)", color='tab:orange')
ax2.tick_params(axis='y', labelcolor='tab:orange')

# Title and grid
plt.title("System Bandwidth and Re-replication Time vs File Size")
fig.tight_layout()
plt.grid(True, axis='x')

# Add combined legend
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='best')

# Save to file
plt.savefig("re_replication_dual_axis.png", dpi=300)
plt.close()

print("✅ Saved combined dual-axis plot as 're_replication_dual_axis.png'")
