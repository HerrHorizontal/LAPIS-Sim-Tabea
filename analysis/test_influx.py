from influxdb import InfluxDBClient
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

client = InfluxDBClient('localhost', 8086, 'admin', 'katze1234', 'lapis')

tag_to_label = dict(
    tag_no_pjr_no_jr="jr = 0",
    tag_no_pjr_data_jr="jr = data",
    tag_no_pjr_data_demand_jr="jr = data + demand",
    tag_default_pjr_no_jr="pjr=default, jr = 0",
    tag_default_pjr_data_jr="pjr=default, jr = data",
    tag_default_pjr_data_demand_jr="pjr=default, jr=data + demand"
)

def get_data(tag):
    data = dict()
    # measurements = ["cobald_status", "drone_status_caching_tmp", "hitrate_evaluation",
    #                 "job_event", "pipe_data_volume", "pipe_status", "resource_status",
    #                 "user_demand_tmp"]
    # measurements = ["drone_status_caching_tmp", "hitrate_evaluation",
    #                 "job_event", "pipe_data_volume", "pipe_status"]
    measurements = [ "hitrate_evaluation", "job_event",
                     "pipe_data_volume", "pipe_status"]
    for measurement in measurements:
        print(measurement)
        query = "select * from {} where {}='{}'".format(measurement, "tardis", tag)
        print(query)
        result = client.query(query)

        for k in result:
            if measurement == "job_event":
                # uses first entry that is not NaN => uses starting timestamp but
                # that should be ok
                data[measurement] = pd.DataFrame.from_dict(k, orient='columns').groupby("job").last().reset_index()
            else:
                data[measurement] = pd.DataFrame.from_dict(k, orient='columns')

    # test that entries are disjunct or equal
    # grouped_by_job = df_result.groupby("job")
    # for job, info in grouped_by_job:
    #     print(info.iloc[0])
    #     print(info.iloc[1])0piub

    return data

def comparison_plots(jobs, id=""):
    # walltime hists
    bins = np.linspace(0, 120, 200)
    for tag, data in jobs.items():
        data["job_event"].wall_time.divide(3600.).hist(histtype="step", label=tag,
                                                       bins=bins)

    plt.xlabel("wall time (h)")
    plt.ylabel("amount of jobs")
    plt.legend()
    plt.savefig("plots/{}_comparison_walltime.pdf".format(id), transparent=True)
    plt.close()

    # walltime hists
    bins = np.linspace(0, 10, 200)
    for tag, data in jobs.items():
        data["job_event"].wall_time.divide(3600.).hist(histtype="step", label=tag,
                                                       bins=bins)

    plt.xlabel("wall time (h)")
    plt.ylabel("amount of jobs")
    plt.legend()
    plt.savefig("plots/{}_comparison_walltime_lim10h.pdf".format(id), transparent=True)
    plt.close()

    # relative to no coordination at all
    bins = np.linspace(0, 24, 101)
    heights, edges, _ = plt.hist(jobs["tag_no_pjr_no_jr"]["job_event"].wall_time.divide(
        3600.).tolist(), bins=bins)
    plt.close()

    for i, (tag, data) in enumerate(jobs.items()):
        if tag != "tag_no_pjr_no_jr":
            h, e = np.histogram(data["job_event"].wall_time.divide(3600.).tolist(),
                               bins=bins)
            plt.bar(edges[:-1], h - heights, width=24/100., align="edge", label=tag, \
                                                                         fill=False,
                    color="C{}".format(i), edgecolor="C{}".format(i))

    plt.xlabel("wall time (h)")
    plt.ylabel("difference in number of jobs")
    plt.title("base: no job rank, no pre job rank")
    plt.legend()
    plt.savefig("plots/{}_walltime_change.pdf".format(id), transparent=True)
    plt.close()

    # relative to no coordination at all
    bins = np.linspace(0, 6, 51)
    heights, edges, _ = plt.hist(jobs["tag_no_pjr_no_jr"]["job_event"].wall_time.divide(
        3600.).tolist(), bins=bins)
    plt.close()

    for i, (tag, data) in enumerate(jobs.items()):
        if tag != "tag_no_pjr_no_jr":
            h, e = np.histogram(data["job_event"].wall_time.divide(3600.).tolist(),
                                bins=bins)
            plt.bar(edges[:-1], h - heights, width=6 / 100., align="edge", label=tag, \
                    fill=False,
                    color="C{}".format(i), edgecolor="C{}".format(i))

    plt.xlabel("wall time (h)")
    plt.ylabel("difference in number of jobs")
    plt.title("base: no job rank, no pre job rank")
    plt.legend()
    plt.savefig("plots/{}_walltime_change_6h.pdf".format(id), transparent=True)
    plt.close()

    # total walltime
    walltimes = []
    labels = []
    for tag, data in jobs.items():
        walltimes.append(data["job_event"].wall_time.divide(3600.).sum())
        labels.append(tag)
    fig = plt.figure(figsize=[12, 10])
    plt.bar(range(6), walltimes)
    plt.ylabel("total walltime (h)")
    plt.ylim(75000, 80000)
    plt.xticks(range(6), labels, rotation=90)
    plt.tight_layout()
    plt.savefig("plots/{}_total_walltime.pdf".format(id), transparent=True)
    plt.close()

    # transfer hists
    for tag, data in jobs.items():
        data["job_event"].transfer_time[data["job_event"].transfer_time > 0].divide(
            60.).hist(histtype="step", label=tag,
                                                         bins=20)

    plt.xlabel("transfer time (min)")
    plt.ylabel("amount of jobs")
    plt.legend()
    plt.savefig("plots/{}_comparison_transfertime.pdf".format(id), transparent=True)
    plt.close()

    # calculation hists
    bins = np.linspace(0, 14, 100)
    for tag, data in jobs.items():
        data["job_event"].calculation_time.divide(3600.).hist(histtype="step",
                                                              label=tag,
                                                              bins=bins)

    plt.xlabel("calculation time (h)")
    plt.ylabel("amount of jobs")
    plt.legend()
    plt.savefig("plots/{}_comparison_calculationtime.pdf".format(id), transparent=True)
    plt.close()

    # calculation hists
    bins = np.linspace(0, 3, 100)
    for tag, data in jobs.items():
        data["job_event"].calculation_time.divide(3600.).hist(histtype="step",
                                                              label=tag,
                                                              bins=bins)

    plt.xlabel("calculation time (h)")
    plt.ylabel("amount of jobs")
    plt.legend()
    plt.savefig("plots/{}_comparison_calculationtime_lim3h.pdf".format(id), transparent=True)
    plt.close()

    # data throughput hists
    for tag, data in jobs.items():
        data["job_event"].data_througput.hist(histtype="step",
                                                              label=tag)

    plt.xlabel("data throughput (GB/s)")
    plt.ylabel("amount of jobs")
    plt.legend()
    plt.savefig("plots/{}_comparison_datathroughput.pdf".format(id), transparent=True)
    plt.close()

    # cached data
    for tag, data in jobs.items():
        data["job_event"].cached.hist(histtype="step", label=tag)

    plt.xlabel("potentially cached data (GB)")
    plt.ylabel("amount of jobs")
    plt.legend()
    plt.savefig("plots/{}_comparison_potentially_cached.pdf".format(id), transparent=True)
    plt.close()


def cache_hits(jobs, id=""):
    # number of cache hits
    hits = []
    labels = []
    for tag, data in jobs.items():
        hits.append(data["hitrate_evaluation"].providesfile.sum())
        labels.append(tag)

    fig = plt.figure(figsize=[12, 10])

    plt.bar(range(6), hits)
    plt.ylabel("number of jobs reading from cache")
    plt.xticks(range(6), labels, rotation=90)
    plt.tight_layout()
    plt.savefig("plots/{}_comparison_cachehits.pdf".format(id), transparent=True)
    plt.close()

    # cross check: total hitrate
    hits = []
    labels = []
    for tag, data in jobs.items():
        hits.append(data["hitrate_evaluation"].hitrate.sum())
        labels.append(tag)

    fig = plt.figure(figsize=[12, 10])
    plt.bar(range(6), hits)
    plt.ylabel("number of jobs reading from cache")
    plt.xticks(range(6), labels, rotation=90)
    plt.tight_layout()
    plt.savefig("plots/{}_comparison_sum_hitrate.pdf".format(id), transparent=True)
    plt.close()

    # cross check: total hitrate
    hits = []
    labels = []
    for tag, data in jobs.items():
        hits.append(data["hitrate_evaluation"].volume[data[
            "hitrate_evaluation"].providesfile == 1].sum())
        labels.append(tag)

    fig = plt.figure(figsize=[12, 10])
    plt.bar(range(6), hits)
    plt.ylabel("amount of data read from cache")
    plt.xticks(range(6), labels, rotation=90)
    plt.tight_layout()
    plt.savefig("plots/{}_comparison_data_read_from_cache.pdf".format(id), transparent=True)
    plt.close()

    # number of jobs with inputfiles hitting cache cluster
    hits = []
    labels = []
    for tag, data in jobs.items():
        hits.append(data["hitrate_evaluation"].providesfile.count())
        labels.append(tag)

    fig = plt.figure(figsize=[12, 10])
    plt.bar(range(6), hits)
    plt.ylim(4000, 5000)
    plt.ylabel("number of jobs hitting cache cluster")
    plt.xticks(range(6), labels, rotation=90)
    plt.tight_layout()
    plt.savefig("plots/{}_comparison_jobs_with_inputfiles_on_cachecluster.pdf".format(id),
                transparent=True)
    plt.close()


def cachehits_comparison(joblist, labels, id=""):

    fig = plt.figure(figsize=[12, 10])
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        # number of cache hits
        hits = []
        tags = []
        print(type(jobs))
        for tag, data in jobs.items():
            hits.append(data["hitrate_evaluation"].providesfile.sum())
            tags.append(tag)
        plt.bar(range(6), hits, color="C{}".format(i), edgecolor="C{}".format(i),
                label=label, fill=False)
    plt.ylabel("number of jobs reading from cache")
    plt.xticks(range(6), tags, rotation=90)
    plt.legend()
    plt.tight_layout()
    plt.savefig("plots/{}_comparison_cachehits_multiple_samples.pdf".format(id),
                transparent=True)
    plt.close()

    # control plot: cache hits from job event
    fig = plt.figure(figsize=[12, 10])

    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        # number of cache hits
        hits = []
        tags = []
        print(type(jobs))
        for tag, data in jobs.items():
            hits.append(data["job_event"].read_from_cache.sum())
            tags.append(tag)
        plt.bar(range(6), hits, color="C{}".format(i), edgecolor="C{}".format(i),
                label=label, fill=False)
    plt.ylabel("number of jobs reading from cache (via job_event)")
    plt.xticks(range(6), tags, rotation=90)
    plt.legend()
    plt.tight_layout()
    plt.savefig("plots/{}_comparison_cachehits_multiple_samples_job_event.pdf".format(
        id),
                transparent=True)
    plt.close()

    # cross check: total hitrate
    fig = plt.figure(figsize=[12, 10])
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        # number of cache hits
        volume = []
        tags = []
        for tag, data in jobs.items():
            volume.append(data["hitrate_evaluation"].volume[data["hitrate_evaluation"].providesfile == 1].sum())
            tags.append(tag)

        plt.bar(range(6), volume, color="C{}".format(i), edgecolor="C{}".format(i),
                label=label, fill=False)
    plt.ylabel("amount of data read from cache (GB)")
    plt.xticks(range(6), tags, rotation=90)
    plt.legend()
    plt.tight_layout()
    plt.savefig("plots/{}_comparison_data_read_from_cache.pdf".format(id),
                transparent=True)
    plt.close()

    # number of jobs hitting cache cluster
    fig = plt.figure(figsize=[12, 10])
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        hits = []
        tags = []
        for tag, data in jobs.items():
            print(data["job_event"].pool[data["job_event"].pool.str.endswith(
                "cache>")])
            print(data["job_event"].pool[data["job_event"].pool.str.endswith(
                "cache>")].count())
            hits.append(data["job_event"].pool[data["job_event"].pool.str.endswith(
                "cache>")].count())
            tags.append(tag)
        print(hits, tags)
        plt.bar(range(6), hits, color="C{}".format(i), edgecolor="C{}".format(i),
            label=label, fill=False)

    # plt.ylim(4000, 5000)
    plt.ylabel("number of jobs hitting cache cluster")
    plt.xticks(range(6), tags, rotation=90)
    plt.legend()
    plt.tight_layout()
    plt.savefig(
        "plots/{}_comparison_jobs_on_cachecluster.pdf".format(id),
        transparent=True)
    plt.close()


def walltime_comparison(joblist, labels):
    fig = plt.figure(figsize=[12, 10])
    bins=np.linspace(0, 12, 50)
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        jobs = jobs["tag_no_pjr_no_jr"]
        print(jobs["job_event"].wall_time)
        jobs["job_event"].wall_time[jobs["job_event"].pool.str.endswith(
            "cache>")].divide(3600.).hist(
            bins=bins, alpha=0.3, label="{} cache cluster".format(label),
            color="C{}".format(i), edgecolor="C{}".format(i), linewidth=2, normed=True)
        jobs["job_event"].wall_time[jobs["job_event"].pool.str.endswith(
            "None>")].divide(3600.).hist(
            bins=bins, histtype="step", label="{} dummy cluster".format(label),
            color="C{}".format(i), edgecolor="C{}".format(i), normed=True)

    plt.xlabel("walltime (h)")
    # plt.ylabel("number of jobs")
    plt.legend()
    plt.tight_layout()
    plt.savefig("plots/cluster_comparison_walltime_normed.png")
    plt.close()

    fig = plt.figure(figsize=[12, 10])
    bins = np.linspace(0, 6, 50)
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        jobs = jobs["tag_no_pjr_no_jr"]
        print(jobs["job_event"].wall_time)
        jobs["job_event"].wall_time[jobs["job_event"].pool.str.endswith(
            "cache>")].divide(3600.).hist(
            bins=bins, alpha=0.3, label="{} cache cluster".format(label),
            color="C{}".format(i), edgecolor="C{}".format(i), linewidth=2, normed=True)
        jobs["job_event"].wall_time[jobs["job_event"].pool.str.endswith(
            "None>")].divide(3600.).hist(
            bins=bins, histtype="step", label="{} dummy cluster".format(label),
            color="C{}".format(i), edgecolor="C{}".format(i), normed=True)

    plt.xlabel("walltime (h)")
    # plt.ylabel()
    plt.legend()
    plt.tight_layout()
    plt.savefig("plots/cluster_comparison_walltime_normed_6h.png")
    plt.close()

    fig = plt.figure(figsize=[12, 10])
    bins = 50
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        jobs = jobs["tag_no_pjr_no_jr"]
        print(jobs["job_event"].columns.values)
        jobs["job_event"][jobs["job_event"].pool.str.endswith(
            "cache>")]["diff"].divide(60.).hist(
            bins=bins, alpha=0.3, label="{} cache cluster".format(label),
            color="C{}".format(i), edgecolor="C{}".format(i))
        if jobs["job_event"][jobs["job_event"].pool.str.endswith(
            "None>")]["diff"].divide(60.).mean() != 0:
            print(jobs["job_event"][jobs["job_event"].pool.str.endswith(
            "None>")]["diff"].divide(60.).mean())

    plt.xlabel("difference original and simulated walltime (min)")
    plt.ylabel("number of jobs")
    plt.legend()
    plt.tight_layout()
    plt.savefig("plots/cluster_comparison_walltime_difference.png")
    plt.close()

    # comparison cpuh
    fig = plt.figure(figsize=[12, 10])
    cpuh_cache = []
    cpuh_dummy = []
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        # number of cache hits
        data = jobs["tag_no_pjr_no_jr"]
        print(data["job_event"][data["job_event"].pool.str.endswith(
            "cache>")])
        cpuh_cache.append(data["job_event"][data["job_event"].pool.str.endswith(
            "cache>")].wall_time.divide(3600.).sum())
        cpuh_dummy.append(data["job_event"][data["job_event"].pool.str.endswith(
            "None>")].wall_time.divide(3600.).sum())
    width = 0.4
    plt.bar(np.arange(3) - width/2, cpuh_cache, width=width, label="cache")
    plt.bar(np.arange(3) + width/2, cpuh_dummy, width=width, label="dummy")
    plt.ylabel("cpuh of jobs running on resource type")
    plt.xticks(range(3), labels, rotation=90)
    plt.legend()
    plt.tight_layout()
    plt.savefig("plots/{}_comparison_cpuh_clusters.pdf".format(id),
        transparent=True)
    plt.close()



jobs_16_480 = {k: get_data(v) for k, v in tags_25_1_0_16.items()}
# jobs_504cores = {k: get_data(v) for k, v in tags_25_1_0_16_504cores.items()}
jobs_648cores = {k: get_data(v) for k, v in tags_25_1_0_16_648cores.items()}
jobs_888cores = {k: get_data(v) for k, v in tags_25_1_0_16_888cores.items()}
# cache_hits(jobs_504, 504)
# cachehits_comparison([jobs_16, jobs_648cores, jobs_888cores], [480, 648, 960],
#                      id="different_clustersizes")
# cachehits_comparison([jobs_888cores, jobs_648cores], [960, 648], id="oversized_cluster")
walltime_comparison([jobs_16, jobs_648cores, jobs_888cores], [480, 648, 960])
input()

# jobs_16 = {k: get_data(v) for k, v in tags_25_1_0_16.items()}
jobs_29 = {k: get_data(v) for k, v in tags_25_1_0_29.items()}
jobs_10 = {k: get_data(v) for k, v in tags_25_1_0_10.items()}

# cache_hits(jobs_16, "fullcluster_16")
# cache_hits(jobs_29, "fullcluster_29")
# cache_hits(jobs_10, "fullcluster_10")
cachehits_comparison([jobs_16, jobs_29, jobs_10], [16, 29, 10], id="different_samples")
# cachehits_comparison([jobs_16], [16])

# comparison_plots(jobs)
# cache_hits(jobs)
# data = get_data(["tardis", "lapis-1582302051.5985017"])
# print(data)
