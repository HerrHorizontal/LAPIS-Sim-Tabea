import numpy as np
import matplotlib.pyplot as plt

def walltime(h, tcpu, V, sc, sr, epsilon=1):
    print(h, [tcpu/epsilon, V*h/(sc), V*(1-h)/sr], max([tcpu/epsilon, V*h/(sc),
                                                      V*(1-h)/sr]))
    return max([tcpu/epsilon, V*h/(sc), V*(1-h)/sr])


h = np.linspace(0, 1, 1000)

sc = 1.0
sr = 0.7

fontsize=20
FIG , ax = plt.subplots(figsize=[10, 6])
plt.tick_params(labelsize=fontsize - 2)

plt.plot(h, [walltime(i, 15, 20, sc, sr) for i in h], linewidth=3)
plt.xlabel("fraction of data read from cache", fontsize=fontsize)
plt.ylabel("job walltime (min)", fontsize=fontsize)
plt.text(0.15, 17, "1", fontsize=fontsize)
plt.text(0.6, 17, "2", fontsize=fontsize)
plt.text(0.95, 17, "3", fontsize=fontsize)
plt.xlim(0,1)
plt.tight_layout()
plt.savefig("jobwalltimemodel.png")
plt.close()