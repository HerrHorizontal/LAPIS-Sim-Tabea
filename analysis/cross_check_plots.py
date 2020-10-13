import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from influxdb import InfluxDBClient
from tag_mapping import TAGS
from multiprocessing import Pool

import seaborn as sns
sns.set_style("ticks")
sns.set_palette(sns.color_palette("deep"))

client = InfluxDBClient('localhost', 8086, 'admin', 'katze1234', 'lapis')

fontsize = 24

tags_to_label={
        "tag_no_pjr_no_jr": "no coordination",
        "tag_no_pjr_data_jr": "no PJR, JR cached data",
        "tag_no_pjr_data_demand_jr": "no PJR, JR cached data,\nlimited cache demand",
        "tag_default_pjr_no_jr": "default PJR",
        "tag_default_pjr_data_jr": "default PJR, JR cached data",
        "tag_default_pjr_data_demand_jr": "default PJR, JR cached data, \nlimited cache demand",
        "no-pjr_jad_default": "no coordination",
        "no-pjr_no-jr": "no coordination",
        "no-pjr_jad-data-demand-75": "no PJR, JR cached data,\nlimited cache demand, "
                                     "\nn=54",
        "no-pjr_jad-data-demand-25": "no PJR, JR cached data,\nlimited cache demand, "
                                     "\nn=18",
        "no-pjr_jad-data-demand-50": "no PJR, JR cached " \
                                     "data,\nlimited cache demand, "
                                     "\nn=36",
        "default-pjr_jad-data-demand-75": "default PJR, JR cached data,\nlimited " \
                                          "cache demand, \nn=54",
        "default-pjr_jad-data-demand-25": "default PJR, JR cached data,\nlimited " \
                                          "cache demand, \nn=18",
        "default-pjr_jad-data-demand-50": "default PJR, JR cached data,\nlimited " \
                                          "cache demand, \nn=36",
        "no-pjr_jad-data": "no PJR, JR cached data",
        "default-pjr_jad-data": "default PJR, JR cached data",

}
tags_to_id = {
        "tag_no_pjr_no_jr": "no coordination",
        "tag_no_pjr_data_jr": "no PJR JR cached data",
        "tag_no_pjr_data_demand_jr": "no PJR JR cached data limited cache "
                                     "demand",
        "tag_default_pjr_no_jr": "default PJR",
        "tag_default_pjr_data_jr": "default PJR JR cached data",
        "tag_default_pjr_data_demand_jr": "default PJR JR cached data "
                                       "limited cache demand",
        "no-pjr_jad_default": "no coordination",
        "no-pjr_no-jr": "no coordination",
        "no-pjr_jad-data-demand-75": "no PJR, JR cached data,limited cache demand, "
                                     "n=54",
        "no-pjr_jad-data-demand-25": "no PJR, JR cached data,limited cache demand, "
                                     "n=18",
        "no-pjr_jad-data-demand-50": "no PJR, JR cached data, limited cache " \
                                      "demand, n=36",
        "default-pjr_jad-data-demand-75": "default PJR, JR cached data,limited " \
                                          "cache demand, n=54",
        "default-pjr_jad-data-demand-25": "default PJR, JR cached data, limited " \
                                          "cache demand, n=18",
        "default-pjr_jad-data-demand-50": "default PJR, JR cached data,limited " \
                                          "cache demand, n=36",
        "no-pjr_jad-data": "no PJR, JR cached data",
        "default-pjr_jad-data": "default PJR, JR cached data",
}

jobs_with_cached_data={10: 35249., 16: 31674., 29: 29291.}

TAGS2 = eval(eval(open("tagmapping2.py", "r").read()))
TAGS3 = eval(eval(open("tagmapping3.py", "r").read()))
TAGS_new = eval(eval(open("tagmapping_28-02-2020.txt", "r").read()))


def get_data(tag,
             measurements=["hitrate_evaluation", "job_event", "pipe_data_volume"]):
    data = dict()
    for measurement in measurements:
        print(measurement)
        if measurement == "job_event":
            query = "select waiting_time, wall_time, original_walltime, pool, job, " \
                    "read_from_cache from {} where {}='{}'".format(measurement,
                                                                  "tardis", tag)
        else:
            query = "select * from {} where {}='{}'".format(measurement, "tardis", tag)
        print(query)
        result = client.query(query)
        if not result:
            print(result)

        for k in result:
            if measurement == "job_event":
                # uses first entry that is not NaN => uses finishing timestamp but
                # that should be ok
                data[measurement] = pd.DataFrame.from_dict(k, orient='columns')
                data["job_event_start"] = data[measurement][data[measurement][
                    'waiting_time'].notna()]
                data["job_event_complete"] = data[measurement][data[measurement][
                    'wall_time'].notna()]
                # print(data[measurement].shape[0], data["job_event_start"].shape[0],
                #       data["job_event_complete"].shape[0],
                #       data["job_event_start"].shape[0] +
                #       data["job_event_complete"].shape[0]
                #       )
                data[measurement] = data[measurement].groupby("job").last().reset_index()
            elif measurement == "hitrate_evaluation":
                data[measurement] = pd.DataFrame.from_dict(k, orient='columns')
                if data[measurement].empty:
                    print(query)

            else:
                data[measurement] = pd.DataFrame.from_dict(k, orient='columns')
    return data


def number_of_jobs_on_caching_cluster(input, ID, clusters=[480, 720, 960]):

    fig, ax = plt.subplots(figsize=[14, 10])
    plt.tick_params(labelsize=fontsize - 2)
    # Set position of bar on X axis
    width = 0.8 / 3.
    n_clusters = len(clusters)
    x_points = [np.arange(n_clusters), [x + width for x in np.arange(n_clusters)],
                [x + 2*width for x in np.arange(n_clusters)],
                [x + 3*width for x in np.arange(n_clusters)]]
    for i, (scenario, clustersizes) in enumerate(input.items()):
        print(scenario)

        # if scenario == "original jobs with caching":
        #     for j, n_cores in enumerate(clusters):
        #         print(j, (j + 1) * (1. / len(clusters)),
        #               72. / n_cores * data["job_event_complete"].shape[0])
        #         ax.axhline(xmin=j * (1. / len(clusters)),
        #                    xmax=(j + 1) * (1. / len(clusters)),
        #                    y=72. / n_cores * data["job_event_complete"].shape[0],
        #                    color="k")
        #     ax.axhline(xmin=0, xmax=0, y=0, color="k", label="expected number of jobs")
        #     plt.xlabel("number of cores in resources", fontsize=fontsize)
        #     plt.ylabel("number of jobs on caching cluster", fontsize=fontsize)
        #     plt.legend(fontsize=fontsize)
        #     # plt.xticks(range(3), [492, 648, 924])
        #     plt.xticks([i + 0.25 for i in range(n_clusters)], clusters)
        #     plt.tight_layout()
        #     plt.savefig("plots/crosscheck/number_of_jobs_on_caching_cluster_{"
        #                 "}_without_cached.pdf".format(ID),
        #                 transparent=True)

        number_of_jobs_cachecluster = []
        number_of_jobs_dummycluster = []
        for data in clustersizes:
            number_of_jobs_cachecluster.append(
                data["job_event_complete"][data[
                "job_event_complete"].pool.str.endswith("cache>")].shape[0])
            # number_of_jobs_dummycluster.append(
            #     data["job_event_complete"][data[
            #     "job_event_complete"].pool.str.endswith("None>")].shape[0])

        ax.bar(x_points[i], number_of_jobs_cachecluster, width=width,
                label=scenario, color="C{}".format(i), edgecolor="C{}".format(i),
                fill=True, linewidth=4)
    for i,  n_cores in enumerate(clusters):
        print(i, (i+1) * (1./len(clusters)), 72./n_cores*data["job_event_complete"].shape[0])
        expected = 72./n_cores*data["job_event_complete"].shape[0]
        ax.axhline(xmin=i * (1./len(clusters)), xmax=(i+1) * (1./len(clusters)),
                   y=expected, color="k")
        ax.axhspan(xmin=i * (1. / len(clusters)), xmax=(i + 1) * (1. / len(clusters)),
                   ymin=expected - np.sqrt(expected), ymax=expected + np.sqrt(expected),
                   facecolor="k", alpha=0.35, edgecolor="w", zorder=0)
    ax.axhspan(xmin=0, xmax=0, ymin=0, ymax=0, color="k", alpha=0.25,
               label="statistical uncertainty")
    ax.axhline(xmin=0, xmax=0, y=0, color="k", label="expected number of jobs \n on "
                                                     "caching cluster ")
    plt.xlabel("number of cores in resources", fontsize=fontsize)
    plt.ylabel("number of jobs on caching cluster", fontsize=fontsize)
    plt.legend(fontsize=fontsize)
    # plt.xticks(range(3), [492, 648, 924])
    plt.xticks([i + 0.25 for i in range(n_clusters)] , clusters)
    plt.tight_layout()
    plt.savefig("plots/crosscheck/number_of_jobs_on_caching_cluster_{}.pdf".format(ID),
                transparent=True)
    plt.close()


def cachehits_comparison_excess_study(input, ID, clusters=[480, 720, 960]):
    fig, ax = plt.subplots(figsize=[14, 10])
    plt.tick_params(labelsize=fontsize - 2)
    # Set position of bar on X axis
    width = 0.8 / 2.5
    n_clusters = len(clusters)
    n_scenarios = len(list(input.keys()))
    x_points = [np.arange(n_clusters), [x + width for x in np.arange(n_clusters)],
                [x + 2 * width for x in np.arange(n_clusters)],
                ]
    for i, (scenario, clustersizes) in enumerate(input.items()):
    # for i, (jobs, label) in enumerate(zip(joblist, labels)):
        print(scenario)
        # number of cache hits
        hits = []
        jobs_on_caching_cluster = []
        for data in clustersizes:
            hits.append(data["hitrate_evaluation"].providesfile.sum())
            jobs_on_caching_cluster.append(data["job_event_complete"].pool[data[
                "job_event_complete"].pool.str.endswith("cache>")].count())
        ax.bar(x_points[i], hits, color="C{}".format(i), edgecolor="C{}".format(i),
                label=scenario, fill=True, width=width,  linewidth=3)
    # for j, n_cores in enumerate(clusters):
    #     ax.axhline(xmin=j * (1. / len(clusters)),
    #                xmax=(j + 1) * (1. / len(clusters)),
    #                (jobs_with_cached_data[ID] / data[
    #                    "job_event_complete"].shape[0]) * jobs_on_caching_cluster,
    #                label="expected number of cache hits ({})".format(label),
    #                color="k")
    #     ax.axhline(xmin=i * (1. / n_scenarios),
    #                xmax=(i + 1) * (1. / n_scenarios),
    #                y=(jobs_with_cached_data[ID] / data[
    #                    "job_event_complete"].shape[0]) * jobs_on_caching_cluster[i],
    #                color="k")
    for j,  n_cores in enumerate(clusters):
        print(j, (j+1) * (1./len(clusters)), 72./n_cores*data["job_event_complete"].shape[0])
        expected = (jobs_with_cached_data[ID] / data["job_event_complete"].shape[0]) \
                   * jobs_on_caching_cluster[j]
        ax.axhline(xmin=j * (1./len(clusters)), xmax=(j+1) * (1./len(clusters)),
                   y=expected, color="k")
        ax.axhspan(xmin=j * (1. / len(clusters)), xmax=(j + 1) * (1. / len(clusters)),
                   ymin=expected - np.sqrt(expected), ymax=expected + np.sqrt(expected),
                   facecolor="k", alpha=0.35, edgecolor="w", zorder=0)
    ax.axhline(xmin=0, xmax=0, y=0, color="k", label="expected number of cache hits ")
    ax.axhspan(xmin=0, xmax=0, ymin=0, ymax=0, color="k", alpha=0.25,
               label="statistical uncertainty")
    plt.xlabel("number of cores in resources", fontsize=fontsize)
    plt.ylabel("number of jobs reading from cache", fontsize=fontsize)
    plt.xticks([i + 0.15 for i in range(n_clusters)], clusters)
    plt.tick_params(labelsize=fontsize - 2)
    plt.legend(fontsize=fontsize)
    plt.tight_layout()
    plt.savefig("plots/crosscheck/comparison_cachehits_excess_study_{}.pdf".format(ID),
                transparent=True)
    plt.close()


def make_plots_jobs_on_caching_cluster_old(ID):
    job_480_cache = get_data(TAGS["tags_25_1_0_{}_480cores".format(ID)][
                                 "tag_no_pjr_no_jr"], measurements=["job_event"])
    job_720_cache = get_data(
        TAGS["tags_25_1_0_{}_720cores".format(ID)]["tag_no_pjr_no_jr"],
        measurements=["job_event"])
    job_960_cache = get_data(
        TAGS["tags_25_1_0_{}_960cores".format(ID)]["tag_no_pjr_no_jr"], measurements=[
            "job_event"])
    job_480_equalized = get_data(
        TAGS["tags_25_{}_equalized".format(ID)]["tag_no_pjr_no_jr"], measurements=["job_event"])
    job_720_equalized = get_data(
        TAGS["tags_25_{}_equalized_720".format(ID)]["tag_no_pjr_no_jr"], measurements=["job_event"])
    job_960_equalized = get_data(
        TAGS["tags_25_{}_equalized_960".format(ID)]["tag_no_pjr_no_jr"],
        measurements=["job_event"])
    job_480_equalized_orig_qdate = get_data(
        TAGS["tags_25_{}_equalized_orig_qdate".format(ID)][
            "tag_no_pjr_no_jr"], measurements=["job_event"])
    job_720_equalized_orig_qdate = get_data(
        TAGS["tags_25_{}_equalized_orig_qdate_720".format(ID)][
            "tag_no_pjr_no_jr"], measurements=["job_event"])
    job_960_equalized_orig_qdate = get_data(
        TAGS["tags_25_{}_equalized_orig_qdate_960".format(ID)][
            "tag_no_pjr_no_jr"], measurements=["job_event"])
    job_480_equalized_orig_qdate_walltime = get_data(
        TAGS["tags_25_{}_equalized_orig_qdate_walltime".format(ID)][
            "tag_no_pjr_no_jr"], measurements=["job_event"])
    job_720_equalized_orig_qdate_walltime = get_data(
        TAGS["tags_25_{}_equalized_orig_qdate_walltime_720".format(ID)][
            "tag_no_pjr_no_jr"], measurements=["job_event"])
    job_960_equalized_orig_qdate_walltime = get_data(
        TAGS["tags_25_{}_equalized_orig_qdate_walltime_960".format(ID)][
            "tag_no_pjr_no_jr"], measurements=["job_event"])

    input = {
        "identical jobs": [
            job_480_equalized,
            job_720_equalized,
            job_960_equalized,
        ],
        "identical jobs with original QDate": [
            job_480_equalized_orig_qdate,
            job_720_equalized_orig_qdate,
            job_960_equalized_orig_qdate,
        ],
        "original jobs without caching": [
            job_480_equalized_orig_qdate_walltime,
            job_720_equalized_orig_qdate_walltime,
            job_960_equalized_orig_qdate_walltime,
        ],
        "original jobs with caching": [
            job_480_cache,
            job_720_cache,
            job_960_cache,
        ],
    }

    # walltime_comparison(input, ID)
    number_of_jobs_on_caching_cluster(input, ID, clusters=[492  , 720, 1032])


def make_plots_number_of_cachehits_excess_old(ID):
    jobs_480_cache_ranks = get_data(
        TAGS["tags_25_1_0_{}_480cores".format(ID)]["tag_no_pjr_no_jr"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_720_cache_ranks = get_data(
        TAGS["tags_25_1_0_{}_720cores".format(ID)]["tag_no_pjr_no_jr"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_960_cache_ranks = get_data(
        TAGS["tags_25_1_0_{}_960cores".format(ID)]["tag_no_pjr_no_jr"],
        measurements=["job_event", "hitrate_evaluation"])
    # jobs_480_cache_ranks_uniform_qdate = get_data(
    #     TAGS2["week_25_0.0_0.0_{}_480_uniform_qdate".format(ID)]["no-pjr_jad_default"],
    #     measurements=["job_event", "hitrate_evaluation"])
    # jobs_720_cache_ranks_uniform_qdate = get_data(
    #     TAGS2["week_25_0.0_0.0_{}_720_uniform_qdate".format(ID)]["no-pjr_jad_default"],
    #     measurements=["job_event", "hitrate_evaluation"])
    # jobs_960_cache_ranks_uniform_qdate = get_data(
    #     TAGS2["week_25_0.0_0.0_{}_960_uniform_qdate".format(ID)]["no-pjr_jad_default"],
    #     measurements=["job_event", "hitrate_evaluation"])
    jobs_480_cache_ranks_shuffled_uniform_qdate = get_data(
        TAGS3["week_25_0.0_0.0_{}_480_shuffled_uniform_qdate".format(ID)]["no-pjr_no-jr"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_720_cache_ranks_shuffled_uniform_qdate = get_data(
        TAGS3["week_25_0.0_0.0_{}_720_shuffled_uniform_qdate".format(ID)]["no-pjr_no-jr"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_960_cache_ranks_shuffled_uniform_qdate = get_data(
        TAGS3["week_25_0.0_0.0_{}_960_shuffled_uniform_qdate".format(ID)]["no-pjr_no-jr"],
        measurements=["job_event", "hitrate_evaluation"])

    input = {
        "original jobs with caching": [
            jobs_480_cache_ranks,
            jobs_720_cache_ranks,
            jobs_960_cache_ranks
        ],
        # "uniform submission": [
        #     jobs_480_cache_ranks_uniform_qdate,
        #     jobs_720_cache_ranks_uniform_qdate,
        #     jobs_960_cache_ranks_uniform_qdate
        # ],
        "shuffled and uniform submission": [
            jobs_480_cache_ranks_shuffled_uniform_qdate,
            jobs_720_cache_ranks_shuffled_uniform_qdate,
            jobs_960_cache_ranks_shuffled_uniform_qdate
        ]
    }

    cachehits_comparison_excess_study(input, ID)


def make_plots_jobs_on_caching_cluster(ID):
    job_384_cache = get_data(TAGS_new["week_25_1.0_0.0_{}_384".format(ID)][
                                 "no-pjr_no-jr"], measurements=["job_event"])
    job_480_cache = get_data(TAGS_new["week_25_1.0_0.0_{}_480".format(ID)][
                                 "no-pjr_no-jr"], measurements=["job_event"])
    job_720_cache = get_data(
        TAGS_new["week_25_1.0_0.0_{}_720".format(ID)]["no-pjr_no-jr"],
        measurements=["job_event"])
    job_960_cache = get_data(
        TAGS_new["week_25_1.0_0.0_{}_960".format(ID)]["no-pjr_no-jr"],
        measurements=[
            "job_event"])
    job_384_equalized = get_data(
        TAGS_new["week_25_0.0_0.0_{}_384_equalized".format(ID)]["no-pjr_no-jad"],
        measurements=["job_event"])
    job_480_equalized = get_data(
        TAGS_new["week_25_0.0_0.0_{}_480_equalized".format(ID)]["no-pjr_no-jad"],
        measurements=["job_event"])
    job_720_equalized = get_data(
        TAGS_new["week_25_0.0_0.0_{}_720_equalized".format(ID)]["no-pjr_no-jad"],
        measurements=["job_event"])
    job_960_equalized = get_data(
        TAGS_new["week_25_0.0_0.0_{}_960_equalized".format(ID)]["no-pjr_no-jad"],
        measurements=["job_event"])
    job_384_equalized_orig_qdate = get_data(
        TAGS_new["week_25_0.0_0.0_{}_384_equalized_orig_qdate".format(ID)][
            "no-pjr_no-jad"], measurements=["job_event"])
    job_480_equalized_orig_qdate = get_data(
        TAGS_new["week_25_0.0_0.0_{}_480_equalized_orig_qdate".format(ID)][
            "no-pjr_no-jad"], measurements=["job_event"])
    job_720_equalized_orig_qdate = get_data(
        TAGS_new["week_25_0.0_0.0_{}_720_equalized_orig_qdate".format(ID)][
            "no-pjr_no-jad"], measurements=["job_event"])
    job_960_equalized_orig_qdate = get_data(
        TAGS_new["week_25_0.0_0.0_{}_960_equalized_orig_qdate".format(ID)][
            "no-pjr_no-jad"], measurements=["job_event"])
    job_384_equalized_orig_qdate_walltime = get_data(
        TAGS_new["week_25_0.0_0.0_{}_384_equalized_orig_qdate_walltime".format(ID)][
            "no-pjr_no-jad"], measurements=["job_event"])
    job_480_equalized_orig_qdate_walltime = get_data(
        TAGS_new["week_25_0.0_0.0_{}_480_equalized_orig_qdate_walltime".format(ID)][
            "no-pjr_no-jad"], measurements=["job_event"])
    job_720_equalized_orig_qdate_walltime = get_data(
        TAGS_new["week_25_0.0_0.0_{}_720_equalized_orig_qdate_walltime".format(ID)][
            "no-pjr_no-jad"], measurements=["job_event"])
    job_960_equalized_orig_qdate_walltime = get_data(
        TAGS_new["week_25_0.0_0.0_{}_960_equalized_orig_qdate_walltime".format(ID)][
            "no-pjr_no-jad"], measurements=["job_event"])

    input = {
        "original jobs with caching": [
            job_384_cache,
            job_480_cache,
            job_720_cache,
            job_960_cache,
        ],
        # "identical jobs": [
        #     job_384_equalized,
        #     job_480_equalized,
        #     job_720_equalized,
        #     job_960_equalized,
        # ],
        "identical jobs without caching": [
            job_384_equalized_orig_qdate,
            job_480_equalized_orig_qdate,
            job_720_equalized_orig_qdate,
            job_960_equalized_orig_qdate,
        ],
        "original jobs without caching": [
            job_384_equalized_orig_qdate_walltime,
            job_480_equalized_orig_qdate_walltime,
            job_720_equalized_orig_qdate_walltime,
            job_960_equalized_orig_qdate_walltime,
        ],



    }

    # walltime_comparison(input, ID)
    number_of_jobs_on_caching_cluster(input, "{}_new".format(ID),
                                      clusters=[384, 480, 720, 960])


def make_plots_number_of_cachehits_excess(ID):
    jobs_480_cache_ranks = get_data(
        TAGS_new["week_25_1.0_0.0_{}_480".format(ID)]["no-pjr_no-jr"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_384_cache_ranks = get_data(
        TAGS_new["week_25_1.0_0.0_{}_384".format(ID)]["no-pjr_no-jr"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_720_cache_ranks = get_data(
        TAGS_new["week_25_1.0_0.0_{}_720".format(ID)]["no-pjr_no-jr"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_960_cache_ranks = get_data(
        TAGS_new["week_25_1.0_0.0_{}_960".format(ID)]["no-pjr_no-jr"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_384_cache_ranks_uniform_qdate = get_data(
        TAGS_new["week_25_0.0_0.0_{}_384_uniform_qdate".format(ID)]["no-pjr_no-jad"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_480_cache_ranks_uniform_qdate = get_data(
        TAGS_new["week_25_0.0_0.0_{}_480_uniform_qdate".format(ID)]["no-pjr_no-jad"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_720_cache_ranks_uniform_qdate = get_data(
        TAGS_new["week_25_0.0_0.0_{}_720_uniform_qdate".format(ID)]["no-pjr_no-jad"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_960_cache_ranks_uniform_qdate = get_data(
        TAGS_new["week_25_0.0_0.0_{}_960_uniform_qdate".format(ID)]["no-pjr_no-jad"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_384_cache_ranks_shuffled_uniform_qdate = get_data(
        TAGS_new["week_25_0.0_0.0_{}_384_shuffled_uniform_qdate".format(ID)][
            "no-pjr_no-jad"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_480_cache_ranks_shuffled_uniform_qdate = get_data(
        TAGS_new["week_25_0.0_0.0_{}_480_shuffled_uniform_qdate".format(ID)][
            "no-pjr_no-jad"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_720_cache_ranks_shuffled_uniform_qdate = get_data(
        TAGS_new["week_25_0.0_0.0_{}_720_shuffled_uniform_qdate".format(ID)][
            "no-pjr_no-jad"],
        measurements=["job_event", "hitrate_evaluation"])
    jobs_960_cache_ranks_shuffled_uniform_qdate = get_data(
        TAGS_new["week_25_0.0_0.0_{}_960_shuffled_uniform_qdate".format(ID)][
            "no-pjr_no-jad"],
        measurements=["job_event", "hitrate_evaluation"])

    input = {
        "original jobs with caching": [
            jobs_384_cache_ranks,
            jobs_480_cache_ranks,
            jobs_720_cache_ranks,
            jobs_960_cache_ranks
        ],
        # "uniform submission": [
        #     jobs_384_cache_ranks_uniform_qdate,
        #     jobs_480_cache_ranks_uniform_qdate,
        #     jobs_720_cache_ranks_uniform_qdate,
        #     jobs_960_cache_ranks_uniform_qdate
        # ],
        "shuffled jobs with caching": [
            jobs_384_cache_ranks_shuffled_uniform_qdate,
            jobs_480_cache_ranks_shuffled_uniform_qdate,
            jobs_720_cache_ranks_shuffled_uniform_qdate,
            jobs_960_cache_ranks_shuffled_uniform_qdate
        ]
    }

    cachehits_comparison_excess_study(input, ID, clusters=[384, 480, 720, 960])

pools = Pool(6)

pools.map(make_plots_number_of_cachehits_excess, [16, 10, 29])
# pools.map(make_plots_number_of_cachehits_excess, [16])
pools.map(make_plots_jobs_on_caching_cluster, [16, 10, 29])
# pools.map(make_plots_jobs_on_caching_cluster, [16])
