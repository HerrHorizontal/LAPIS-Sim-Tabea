import seaborn as sns
import numpy as np


class SingleCopyJobs(object):
    hitrates = np.linspace(0.01, 0.99, 20)
    hitrates_sampling = np.linspace(0, 1, 1000)
    pool_file = "/home/tabea/work/testdata/single_cluster.csv"
    storage_file = "/home/tabea/work/testdata/single_cache.csv"
    storage_content_file = "/home/tabea/work/testdata/single_cache_content.csv"
    job_file = "/home/tabea/work/testdata/4_copy_job.json"