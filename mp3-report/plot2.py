import numpy as np
import matplotlib.pyplot as plt

# Data: Bandwidths in KiB/s for each X (number of 128 KiB files)
data = {
    10:  [1708.52, 1196.14, 1708.55],
    50:  [3074.66, 5804.87, 8199.11],
    100: [9224.85, 11273.13, 12640.00],
    200: [25783.24, 15714.04, 27329.77],
}

# Compute averages and standard deviations
x = sorted(data.keys())
avg_bandwidths = [np.mean(data[n]) for n in x]
std_bandwidths = [np.std(data[n]) for n in x]

# Plot
plt.figure(figsize=(8,5))
plt.errorbar(x, avg_bandwidths, yerr=std_bandwidths, fmt='-o', capsize=5, color='tab:blue')
plt.title("Rebalancing Bandwidth vs Number of 128 KiB Files")
plt.xlabel("Number of 128 KiB Files (X)")
plt.ylabel("Bandwidth (KiB/s)")
plt.grid(True)
plt.tight_layout()

# Save to file
plt.savefig("rebalancing_bandwidth.png", dpi=300)
plt.close()

print("✅ Saved plot as 'rebalancing_bandwidth.png'")
