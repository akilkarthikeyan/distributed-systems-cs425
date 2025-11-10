import numpy as np
import matplotlib.pyplot as plt

# -----------------------------
# Raw data (times in milliseconds)
# -----------------------------

# 4 KiB appends
data_4k_merge = {
    1: [5.276875, 4.387554, 3.877908],
    2: [8.602218, 3.87148, 7.728907],
    5: [5.970615, 8.69223, 7.611317],
    10: [5.304302, 6.900334, 9.621917],
}

data_4k_subseq = {
    1: [4.489966, 4.94618, 4.462956],
    2: [6.456369, 4.792186, 5.225802],  # note: "completed in 4.792186ms"
    5: [6.469976, 9.54105, 5.871434],
    10: [3.887562, 3.65755, 5.178065],
}

# 32 KiB appends
data_32k_merge = {
    1: [10.22315, 7.798835, 7.603465],
    2: [6.648107, 6.181962, 6.002121],
    5: [4.564181, 5.536344, 6.398227],
    10: [4.915679, 8.38575, 8.616662],
}

data_32k_subseq = {
    1: [8.187168, 8.463744, 7.828523],   # "subsquent" -> subsequent
    2: [5.012704, 7.209713, 7.60398],
    5: [7.755304, 5.266011, 4.529353],
    10: [6.95155, 4.744953, 7.703591],
}

# -----------------------------
# Helper to compute means/stds
# -----------------------------
def stats_from_dict(d):
    x = sorted(d.keys())
    means = [np.mean(d[k]) for k in x]
    stds  = [np.std(d[k]) for k in x]  # use ddof=1 for sample std dev if you prefer
    return x, means, stds

x4m, m4m, s4m = stats_from_dict(data_4k_merge)
x4s, m4s, s4s = stats_from_dict(data_4k_subseq)
x32m, m32m, s32m = stats_from_dict(data_32k_merge)
x32s, m32s, s32s = stats_from_dict(data_32k_subseq)

# Sanity: ensure x-axes match (they should be [1,2,5,10])
assert x4m == x4s == x32m == x32s, "Concurrent append x-axes differ!"
x_vals = x4m

# -----------------------------
# Plot
# -----------------------------
plt.figure(figsize=(9, 5))

# Four lines with error bars
plt.errorbar(x_vals, m4m,  yerr=s4m,  fmt='-o', capsize=5, label='4KiB merge')
plt.errorbar(x_vals, m4s,  yerr=s4s,  fmt='-s', capsize=5, label='4KiB subsequent merge')
plt.errorbar(x_vals, m32m, yerr=s32m, fmt='-^', capsize=5, label='32KiB merge')
plt.errorbar(x_vals, m32s, yerr=s32s, fmt='-D', capsize=5, label='32KiB subsequent merge')

plt.title("Merge & Subsequent-Merge Time vs Number of Concurrent Appends")
plt.xlabel("Number of concurrent appends")
plt.ylabel("Merge completion time (ms)")
plt.xticks(x_vals)
plt.grid(True, axis='both', linestyle='-', alpha=0.3)
plt.legend()
plt.tight_layout()

# Save to file
plt.savefig("merge_performance.png", dpi=300)
plt.close()

print("✅ Saved plot as 'merge_performance.png'")
