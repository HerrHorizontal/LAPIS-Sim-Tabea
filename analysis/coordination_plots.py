import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from influxdb import InfluxDBClient
from tag_mapping import TAGS
from multiprocessing import Pool
from pprint import pprint

import seaborn as sns
sns.set_style("ticks")
sns.set_palette(sns.color_palette("deep"))


client = InfluxDBClient('localhost', 8086, 'admin', 'katze1234', 'lapis')

fontsize = 29

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
        "default-pjr_no-jr": "default PJR, default JR",

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
        "default-pjr_no-jr": "default PJR, default JR",

}

tag_to_label_input={16: "job input sample 1",
                    10: "job input sample 2",
                    29: "job input sample 3",
                    31674.: "job input sample 1, \nh=1.0",
                    10452.42: "job input sample 1, \nh=0.33",
                    35249.: "job input sample 2, \nh=1.0",
                    29291.: "job input sample 3, \nh=1.0",
                    21221.58: "job input sample 1, \nh=0.67"}

jobs_with_cached_data={10: 35249., 16: 31674., 29: 29291.,
                       "10": 35249., "16": 31674., "29": 29291.}
potentially_cached_data={10: 39580.75, 16: 33295.2, 29: 26527.74,
                         "10": 39580.75, "16": 33295.2, "29": 26527.74  }


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
                    "read_from_cache, cache_probability, expectation_cached_data from {} where {}='{}'".format(
                measurement,
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
                print("jobmixscore before separation",
                      data["job_event"].cache_probability.sum())
                data["job_event_start"] = data[measurement][data[measurement][
                    'waiting_time'].notna()]
                print("jobmixscore start",
                      data["job_event_start"].cache_probability.sum())
                data["job_event_complete"] = data[measurement][data[measurement][
                    'wall_time'].notna()]
                print("jobmixscore complete",
                      data["job_event_complete"].cache_probability.sum())
                df = data[measurement].groupby(["job", "pool"]).size()
                data[measurement] = data[measurement].groupby("job").last().reset_index()
            elif measurement == "hitrate_evaluation":
                data[measurement] = pd.DataFrame.from_dict(k, orient='columns')
                if data[measurement].empty:
                    print(query)

            else:
                data[measurement] = pd.DataFrame.from_dict(k, orient='columns')
    return data


def cachehits_comparison(input, ID, clusters=[480, 720, 960]):

    # cache hits
    fig, ax = plt.subplots(figsize=[2*14, 10])
    plt.tick_params(labelsize=fontsize - 2)
    n_clusters = len(clusters)
    n_scenarios = len(list(input.keys()))
    width = 0.8 / n_scenarios
    x_points = [[x + i*width for x in np.arange(n_clusters)] for i in range(n_scenarios)]
    prefix = list(input.keys())[0].split("_")[0]
    print(prefix)
    sorted_keys = ["{}_{}".format(prefix, jr) for jr in ['no-jr', 'jad-data',
                   'jad-data-demand-25', 'jad-data-demand-50', 'jad-data-demand-75']]
    print(sorted_keys)
    for i, (scenario, clustersizes) in enumerate(zip(sorted_keys,
                                                     [input[k] for k in sorted_keys])):
        print(i, scenario)

        hits = []
        for data in clustersizes:
            hits.append(
                data["hitrate_evaluation"].providesfile.sum())
        print(hits, n_clusters, x_points)
        ax.bar(x_points[i], hits, width=width,
               label=tags_to_label[scenario], color="C{}".format(i),
               fill=True, linewidth=4)

    plt.xlabel("number of cores in resources", fontsize=fontsize)
    plt.ylabel("number of jobs reading from cache", fontsize=fontsize)
    plt.xticks([i + 0.25 for i in range(n_clusters)], clusters)
    plt.tick_params(labelsize=fontsize - 2)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.,
               fontsize=fontsize)
    # fig.tight_layout()
    plt.savefig("plots/coordination/comparison_cachehits_{}.pdf".format(ID),
                transparent=True, bbox_inches="tight")
    plt.close()

    # control plot cache hits
    fig, ax = plt.subplots(figsize=[2*14, 10])
    plt.tick_params(labelsize=fontsize - 2)
    n_clusters = len(clusters)
    n_scenarios = len(list(input.keys()))
    width = 0.8 / n_scenarios
    x_points = [[x + i * width for x in np.arange(n_clusters)] for i in
                range(n_scenarios)]
    for i, (scenario, clustersizes) in enumerate(zip(sorted_keys,
                                                     [input[k] for k in sorted_keys])):

        hits = []
        for data in clustersizes:
            hits.append(
                data["job_event_complete"].read_from_cache.sum())
        ax.bar(x_points[i], hits, width=width,
               label=tags_to_label[scenario], color="C{}".format(i),
               fill=True, linewidth=4)

    plt.xlabel("number of cores in resources", fontsize=fontsize)
    plt.ylabel("number of jobs reading from cache (via job_event)", fontsize=fontsize)
    plt.xticks([i + 0.25 for i in range(n_clusters)], clusters)
    plt.tick_params(labelsize=fontsize - 2)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.,
              fontsize=fontsize)
    plt.savefig("plots/coordination/comparison_cachehits_job_event_{}.pdf".format(ID),
                transparent=True, bbox_inches="tight")
    plt.close()

    # data volume
    fig, ax = plt.subplots(figsize=[2*14, 10])
    plt.tick_params(labelsize=fontsize - 2)
    n_clusters = len(clusters)
    n_scenarios = len(list(input.keys()))
    width = 0.8 / n_scenarios
    x_points = [[x + i * width for x in np.arange(n_clusters)] for i in
                range(n_scenarios)]
    for i, (scenario, clustersizes) in enumerate(zip(sorted_keys,
                                                     [input[k] for k in sorted_keys])):
        volume = []
        for data in clustersizes:
            volume.append(
                data["hitrate_evaluation"].volume[data["hitrate_evaluation"].providesfile == 1].sum())
        ax.bar(x_points[i], volume, width=width,
               label=tags_to_label[scenario], color="C{}".format(i),
               fill=True, linewidth=4)

    plt.xlabel("number of cores in resources", fontsize=fontsize)
    plt.ylabel("amount of data read from cache (GB)", fontsize=fontsize)
    plt.xticks([i + 0.25 for i in range(n_clusters)], clusters)
    plt.tick_params(labelsize=fontsize - 2)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.,
              fontsize=fontsize)
    plt.savefig("plots/coordination/comparison_data_read_from_cache_{}.pdf".format(ID),
                transparent=True, bbox_inches="tight")
    plt.close()

    # number of jobs hitting cache cluster
    fig, ax = plt.subplots(figsize=[2*14, 10])
    plt.tick_params(labelsize=fontsize - 2)
    n_clusters = len(clusters)
    n_scenarios = len(list(input.keys()))
    width = 0.8 / n_scenarios
    x_points = [[x + i * width for x in np.arange(n_clusters)] for i in
                range(n_scenarios)]
    for i, (scenario, clustersizes) in enumerate(zip(sorted_keys,
                                                     [input[k] for k in sorted_keys])):
        hits = []
        for data in clustersizes:
            hits.append(data["job_event_complete"].pool[data[
                "job_event_complete"].pool.str.endswith("cache>")].count())

        ax.bar(x_points[i], hits, width=width,
               label=tags_to_label[scenario], color="C{}".format(i),
               fill=True, linewidth=4)

    plt.xlabel("number of cores in resources", fontsize=fontsize)
    plt.ylabel("number of jobs hitting cache cluster", fontsize=fontsize)
    plt.xticks([i + 0.25 for i in range(n_clusters)], clusters)
    plt.tick_params(labelsize=fontsize - 2)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.,
              fontsize=fontsize)
    plt.savefig(
        "plots/coordination/comparison_jobs_on_cachecluster_{}.pdf".format(ID),
        transparent=True, bbox_inches="tight")
    plt.close()

    # cpuh
    fig, ax = plt.subplots(figsize=[2*14, 10])
    plt.tick_params(labelsize=fontsize - 2)
    n_clusters = len(clusters)
    n_scenarios = len(list(input.keys()))
    width = 0.8 / n_scenarios
    x_points = [[x + i * width for x in np.arange(n_clusters)] for i in
                range(n_scenarios)]
    for i, (scenario, clustersizes) in enumerate(zip(sorted_keys,
                                                     [input[k] for k in sorted_keys])):
        cpuh = []
        for data in clustersizes:
            cpuh.append(data["job_event_complete"].wall_time.divide(3600.).sum())

        ax.bar(x_points[i], cpuh, width=width,
               label=tags_to_label[scenario], color="C{}".format(i),
               fill=True, linewidth=4)

    plt.xlabel("number of cores in resources", fontsize=fontsize)
    plt.ylabel("total cpuh of jobs", fontsize=fontsize)
    plt.xticks([i + 0.25 for i in range(n_clusters)], clusters)
    plt.tick_params(labelsize=fontsize - 2)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.,
              fontsize=fontsize)
    plt.savefig(
        "plots/coordination/comparison_total_cpuh_{}.pdf".format(ID),
        transparent=True, bbox_inches="tight")
    plt.close()


def scenario_comparison_scores(input, ID, clusters, baseline_key="no coordination"):

    width = 0.8 / 4.
    n_clusters = len(clusters)
    n_clusters = len(clusters)
    n_scenarios = len(list(input.keys()))
    width = 0.8 / n_scenarios
    x_points = [[x + i * width for x in np.arange(n_clusters)] for i in
                range(n_scenarios)]
    scores = {}
    scores_data = {}
    for i, (scenario, clustersizes) in enumerate(input.items()):
        score = []
        score_data = []
        for data in clustersizes:
            score.append(data["hitrate_evaluation"].providesfile.sum())
            score_data.append(data["pipe_data_volume"][data[
                "pipe_data_volume"]["pipe"].str.endswith("shared>>")].current_total.max())
        scores[tags_to_id[scenario]] = score
        scores_data[tags_to_id[scenario]] = score_data
    caching_suited_jobs = jobs_with_cached_data[ID[:2]]
    cachable_data = potentially_cached_data[ID[:2]]

    scores_tmp = {}
    scores_data_tmp = {}
    sorted_keys = sorted(list(scores.keys()), key=str.casefold)
    for tag in scores.keys():
        print(caching_suited_jobs)
        print(scores[tag], scores[baseline_key])
        print([(score_value-score_nocoord)
                       for score_value, score_nocoord in zip(scores[tag],
                                                       scores[baseline_key])])
        scores_tmp[tag] = [(score_value-score_nocoord)/caching_suited_jobs
                       for score_value, score_nocoord in zip(scores[tag],
                                                       scores[baseline_key])]
        scores_data_tmp[tag] = [(score_value - score_nocoord)/cachable_data
                       for score_value, score_nocoord in zip(scores_data[tag],
                                                             scores_data[baseline_key])]
    scores = scores_tmp
    scores_data = scores_data_tmp

    fig, ax = plt.subplots(figsize=[2*14, 10])
    plt.tick_params(labelsize=fontsize - 2)
    for i, (scenario, score_list) in enumerate(scores.items()):
        if scenario != "no coordination":
            ax.bar(x_points[i], score_list, width=width,
               label=scenario, color="C{}".format(i),
               fill=True, linewidth=4)

    plt.xlabel("number of cores in resources", fontsize=fontsize)
    plt.ylabel("cache hit score", fontsize=fontsize)
    plt.xticks([i + 0.25 for i in range(n_clusters)], clusters)
    plt.tick_params(labelsize=fontsize - 2)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.,
              fontsize=fontsize)
    plt.savefig(
        "plots/coordination/comparison_cachehit_score_{}.pdf".format(ID),
        transparent=True, bbox_inches="tight")
    plt.close()

    fig, ax = plt.subplots(figsize=[2*14, 10])
    plt.tick_params(labelsize=fontsize - 2)
    for i, (scenario, score_list) in enumerate(scores_data.items()):
        # print(score_list)
        if scenario != "no coordination":
            ax.bar(x_points[i], score_list, width=width,
               label=scenario, color="C{}".format(i),
               fill=True, linewidth=4)
    plt.xlabel("number of cores in resources", fontsize=fontsize)
    plt.ylabel("cached data score", fontsize=fontsize)
    plt.xticks([i + 0.25 for i in range(n_clusters)], clusters)
    plt.tick_params(labelsize=fontsize - 2)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.,
              fontsize=fontsize)
    plt.savefig(
        "plots/coordination/comparison_cached_data_score_{}.pdf".format(ID),
        transparent=True, bbox_inches="tight")
    plt.close()

def get_minmax_nested_dict(nested_dict):
    current_max = -1 * float("inf")
    current_min = float("inf")
    for key1, value1 in nested_dict.items():
        for key2, value2 in value1.items():
            if value2 > current_max:
                current_max = value2
            if value2 < current_min:
                current_min = value2
    return current_min, current_max


def scatter_performance(joblist, identifier="", baseline_key="no coordination",
                        hitrates=False):
    scores = {}
    scores_data = {}
    for jobs, clustersize, ID in joblist:
        caching_suited_jobs = jobs_with_cached_data[ID]
        # jobmix_score = {"njobs": caching_suited_jobs, "data": 0}
        for scenario, data in jobs.items():
            needed_cores = data["job_event_complete"].original_walltime.sum() / (
                    3600. * 24 * 7)
            cluster_score = round(clustersize / needed_cores, 4)
            jobmix_score = round(data["job_event_complete"].cache_probability.sum(), 3)

            if not scores.get(tags_to_id[scenario], 0):
                scores[tags_to_id[scenario]] = {}
            scores[tags_to_id[scenario]][(cluster_score, jobmix_score,
                                          ID, clustersize)] = \
                (data["hitrate_evaluation"].providesfile.sum(), jobmix_score)
        # print(scores)

    scores_tmp = {scenario: {} for scenario in scores.keys()}
    scores_data_tmp = {scenario: {} for scenario in scores_data.keys()}
    baseline_scenario = scores[baseline_key]
    # print(scores[baseline_key])
    for scenario, entries in scores.items():
        # print(scenario)
        # print(scores[scenario], scores[baseline_key])
        for k, v in entries.items():
            # print(scores[baseline_key][k], k)
            # print(v[0])
            # print(baseline_scenario[k][0])
            # print(k[2])
            scores_tmp[scenario][k] \
                = (v[0] - baseline_scenario[k][0]) \
                  / v[1]
            # print(scores_tmp[scenario][k], "\n")
            # scores_data_tmp[tag] = [(score_value - score_nocoord) / cachable_data
            #                         for score_value, score_nocoord in zip(
            #                         scores_data[tag], scores_data[baseline_key])]
        # print(scores_tmp[scenario])
    scores = scores_tmp
    scores_data = scores_data_tmp

    if hitrates:
        plot_score_comparison_hitrates(scores, identifier=identifier)
    else:
        plot_score_comparison(scores, scores_data, identifier=identifier)

    with open('scores_{}.txt'.format(identifier), 'w') as file:
        file.write(str(scores))
        file.close()

    with open('scores_data_{}.txt'.format(identifier), 'w') as file:
        file.write(str(scores_data))
        file.close()


def plot_scatter_performance(identifier=""):

    scores_dict = eval(open('scores_{}.txt'.format(identifier), 'r').read())
    scores_data_dict = eval(open('scores_data_{}.txt'.format(identifier), 'r').read())

    print(scores_dict)
    min_score, max_score = get_minmax_nested_dict(scores_dict)
    print("min, max:", min_score, max_score)

    s = plt.cm.ScalarMappable()
    s.set_clim(min_score, max_score)
    s.set_cmap(plt.cm.coolwarm)

    for scenario, data in scores_dict.items():
        cluster_scores = []
        jobmix_scores = []
        scores = []
        for key, value in data.items():
            cluster_scores.append(key[0])
            jobmix_scores.append(key[1])
            scores.append(value)
        df = pd.DataFrame({"clusterscore": cluster_scores, "jobmixscore":
            jobmix_scores, "score": scores})
        print(df)

        fig, ax = plt.subplots(figsize=[14, 10])
        plt.tick_params(labelsize=fontsize - 2)
        cm = plt.cm.get_cmap('coolwarm')
        im = ax.scatter(df['jobmixscore'], df['clusterscore'], c=s.to_rgba(df["score"]),
                        s=100, alpha=0.75, cmap=cm)

        cbar = plt.colorbar(s, ax=ax)
        cbar.set_label("cachehit score", fontsize=fontsize)
        cbar.set_clim(min_score, max_score)
        cbar.ax.tick_params(labelsize=fontsize-2)
        cbar.minorticks_on()
        ax.set_xlabel("jobmix score", fontsize=fontsize)
        ax.set_ylabel("cluster score", fontsize=fontsize)
        # box = ax.get_position()
        # ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.,
        #           fontsize=fontsize)
        plt.savefig("plots/coordination/scatter/cachehitscore_{}_{}.pdf".format(
            scenario, identifier),
                    transparent=True, bbox_inches="tight")
        plt.close()
        plt.clf()

def plot_score_comparison_hitrates(scores, scores_data=None, clusters=[384, 480, 720,
                                                                       960],
                          identifier=""):
    print("PLOT SCORE COMPARISON HITRATES\n")
    print(scores)

    IDs = [16, 10, 29]
    n_scenarios = len(IDs)

    for scenario in scores.keys():
        print(scenario)
        print(scores[scenario])
        _IDs = set([key[1] for key in scores[scenario].keys()])
        print(_IDs)
        ID_labels = []
        scores_list = []

        for current_ID in _IDs:
            print(current_ID)
            ID_labels.append(current_ID)
            _scores = []
            for key, score in scores[scenario].items():
                print(current_ID, key, score)
                if key[1] != current_ID:
                    continue
                print("dict", current_ID, key, score)
                _scores.append((key[3], score))

                # print("\n", _scores)
            # print(current_ID, sorted(_scores, key=lambda x: x[0]))
            scores_list.append([v for (k, v) in sorted(_scores, key=lambda x: x[0])])
            print(scores_list)

        fig, ax = plt.subplots(figsize=[2 * 14, 10])
        plt.tick_params(labelsize=fontsize - 2)
        n_clusters = len(clusters)
        width = 0.8 / (n_scenarios)
        x_points = [[x + i * width for x in np.arange(n_clusters)] for i in
                    range(n_scenarios)]
        print("xpoints:", x_points, "scoreslist", scores_list)
        for j, (ID, _scores) in enumerate(zip(ID_labels, scores_list)):
            print("\n", j, ID, _scores)
            print(x_points[j], _scores, width)
            ax.bar(x_points[j], _scores, width=width,
                   label=tag_to_label_input[ID]
                   , color="C{}".format(j),
                   fill=True, linewidth=4)

        plt.xlabel("number of cores in resources", fontsize=fontsize)
        plt.ylabel("cache hit score", fontsize=fontsize)
        plt.xticks([i + 0.25 for i in range(n_clusters)], clusters)
        plt.tick_params(labelsize=fontsize - 2)
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.,
                  fontsize=fontsize)
        # fig.tight_layout()
        plt.savefig("plots/coordination/comparison_cachescore_sameID_{}_{"
                    "}.pdf".format(
            identifier, scenario),
            transparent=True, bbox_inches="tight")
        plt.close()

def plot_score_comparison(scores, scores_data=None, clusters=[384, 480, 720, 960],
                          identifier="", select_id=0):
    print("PLOT SCORE COMPARISON\n")
    print(scores, scores_data)

    IDs = [16, 10, 29]
    n_scenarios=len(IDs)

    for scenario in scores.keys():
        print(scenario)
        print(scores[scenario])
        _IDs = set([key[2] for key in scores[scenario].keys()])
        print(_IDs)
        ID_labels = []
        scores_list = []

        for current_ID in _IDs:
            print(current_ID)
            ID_labels.append(current_ID)
            _scores = []
            for key, score in scores[scenario].items():
                print(current_ID, key, score)
                if key[2] != current_ID:
                    continue
                print("dict", current_ID, key, score)
                _scores.append((key[3], score))

                # print("\n", _scores)
            # print(current_ID, sorted(_scores, key=lambda x: x[0]))
            scores_list.append([v for (k, v) in sorted(_scores, key=lambda x: x[0])])
            print(scores_list)

        fig, ax = plt.subplots(figsize=[2 * 14, 10])
        plt.tick_params(labelsize=fontsize - 2)
        n_clusters = len(clusters)
        width = 0.8 / (n_scenarios)
        x_points = [[x + i * width for x in np.arange(n_clusters)] for i in
                    range(n_scenarios)]
        print("xpoints:", x_points, "scoreslist", scores_list)
        for j, (ID, _scores) in enumerate(zip(ID_labels, scores_list)):
            print("\n", j,  ID, _scores)
            print(x_points[j], _scores, width)
            ax.bar(x_points[j], _scores, width=width,
                   label=tag_to_label_input[ID]
                   , color="C{}".format(j),
                   fill=True, linewidth=4)

        plt.xlabel("number of cores in resources", fontsize=fontsize)
        plt.ylabel("cache hit score", fontsize=fontsize)
        plt.xticks([i + 0.25 for i in range(n_clusters)], clusters)
        plt.tick_params(labelsize=fontsize - 2)
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.,
                  fontsize=fontsize)
        # fig.tight_layout()
        plt.savefig("plots/coordination/comparison_cachescore_{}_{}.pdf".format(
            identifier, scenario),
                    transparent=True, bbox_inches="tight")
        plt.close()

    print("\nDATA SCORE\n")

    for scenario in scores_data.keys():
        print(scenario)
        print(scores[scenario])
        _IDs = set([key[2] for key in scores[scenario].keys()])
        print(_IDs)
        ID_labels = []
        scores_list = []

        for current_ID in _IDs:
            print(current_ID)
            ID_labels.append(current_ID)
            _scores = []
            for key, score in scores[scenario].items():
                print(current_ID, key, score)
                if key[2] != current_ID:
                    continue
                print("dict", current_ID, key, score)
                _scores.append((key[3], score))

                # print("\n", _scores)
            # print(current_ID, sorted(_scores, key=lambda x: x[0]))
            scores_list.append([v for (k, v) in sorted(_scores, key=lambda x: x[0])])
            print(scores_list)

        fig, ax = plt.subplots(figsize=[2 * 14, 10])
        plt.tick_params(labelsize=fontsize - 2)
        n_clusters = len(clusters)
        width = 0.8 / (n_scenarios)
        x_points = [[x + i * width for x in np.arange(n_clusters)] for i in
                    range(n_scenarios)]
        print("xpoints:", x_points, "scoreslist", scores_list)
        for j, (ID, _scores) in enumerate(zip(ID_labels, scores_list)):
            print("\n", j,  ID, _scores)
            print(x_points[j], _scores, width)
            ax.bar(x_points[j], _scores, width=width,
                   label=tag_to_label_input[ID]
                   , color="C{}".format(j),
                   fill=True, linewidth=4)

        plt.xlabel("number of cores in resources", fontsize=fontsize)
        plt.ylabel("transferred data score", fontsize=fontsize)
        plt.xticks([i + 0.25 for i in range(n_clusters)], clusters)
        plt.tick_params(labelsize=fontsize - 2)
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.,
                  fontsize=fontsize)
        # fig.tight_layout()
        plt.savefig("plots/coordination/comparison_datascore_{}.pdf".format(
            scenario),
                    transparent=True, bbox_inches="tight")
        plt.close()






def make_scatter_plot_performance_old(IDs):
    jobinput = []
    for ID in IDs:
        for hitrate_repeated in [1]:
            for hitrate_other in [0]:
                jobs_480_cache_ranks = {k: get_data(v)
                                  for k, v in TAGS["week_25_{}_{}_{}_480".format(
                                  hitrate_repeated, hitrate_other, ID)].items()}
                jobinput.append((jobs_480_cache_ranks, 480, ID))
                jobs_720_cache_ranks = {k: get_data(v)
                                      for k, v in
                                      TAGS["week_25_{}_{}_{}_720".format(
                                          hitrate_repeated, hitrate_other, ID)].items()}
                jobinput.append((jobs_720_cache_ranks, 720, ID))
                jobs_960_cache_ranks = {k: get_data(v)
                                    for k, v in TAGS["week_25_{}_{}_{}_960".format(
                                    hitrate_repeated, hitrate_other, ID)].items()}
                jobinput.append((jobs_960_cache_ranks, 960, ID))

    print(len(jobinput))
    scatter_performance(jobinput)


def make_scatter_plot_performance(IDs, reload=True):
    if reload:
        print("reloading no pjr")
        jobinput = []
        for ID in IDs:
            for hitrate_repeated in [1.0]:
            # for hitrate_repeated in [0.33, 0.67, 1.0]:
                for hitrate_other in [0.0]:
                    jobs_384_cache_ranks = {k: get_data(v,
                                                        measurements=["hitrate_evaluation",
                                                                      "job_event",
                                                                      "pipe_data_volume"])
                                            for k, v in TAGS_new["week_25_{}_{}_{}_384".format(
                            hitrate_repeated, hitrate_other, ID)].items() if "no_pjr" in k or "no-pjr" in k}
                    jobinput.append((jobs_384_cache_ranks, 384, ID))
                    jobs_480_cache_ranks = {k: get_data(v, measurements=["hitrate_evaluation", "job_event", "pipe_data_volume"])
                                      for k, v in TAGS_new["week_25_{}_{}_{}_480".format(
                                      hitrate_repeated, hitrate_other, ID)].items() if "no_pjr" in k or "no-pjr" in k}
                    jobinput.append((jobs_480_cache_ranks, 480, ID))
                    jobs_720_cache_ranks = {k: get_data(v, measurements=["hitrate_evaluation", "job_event", "pipe_data_volume"])
                                          for k, v in
                                          TAGS_new["week_25_{}_{}_{}_720".format(
                                              hitrate_repeated, hitrate_other,
                                              ID)].items() if "no_pjr" in k or "no-pjr" in k}
                    jobinput.append((jobs_720_cache_ranks, 720, ID))
                    jobs_960_cache_ranks = {k: get_data(v, measurements=["hitrate_evaluation", "job_event", "pipe_data_volume"])
                                        for k, v in TAGS_new["week_25_{}_{}_{}_960".format(
                                        hitrate_repeated, hitrate_other, ID)].items() if "no_pjr" in k or "no-pjr" in k}
                    jobinput.append((jobs_960_cache_ranks, 960, ID))


        scatter_performance(jobinput, "no_pjr", hitrates=False)
        # scatter_performance(jobinput, "no_pjr", hitrates=True)


    # plot_scatter_performance("no_pjr")
    # input()
    if reload:
        print("reloading default pjr")

        jobinput = []
        for ID in IDs:
            for hitrate_repeated in [1.0]:
            # for hitrate_repeated in [0.33, 0.67, 1.0]:
                for hitrate_other in [0.0]:
                    jobs_384_cache_ranks = {k: get_data(v,
                                                        measurements=["hitrate_evaluation",
                                                                      "job_event",
                                                                      "pipe_data_volume"])
                                            for k, v in
                                            TAGS_new["week_25_{}_{}_{}_384".format(
                                                hitrate_repeated, hitrate_other,
                                                ID)].items() if
                                            "default_pjr" in k or "default-pjr" in k}
                    jobinput.append((jobs_384_cache_ranks, 384, ID))
                    jobs_480_cache_ranks = {k: get_data(v,
                                                        measurements=["hitrate_evaluation",
                                                                      "job_event",
                                                                      "pipe_data_volume"])
                                            for k, v in
                                            TAGS_new["week_25_{}_{}_{}_480".format(
                                                hitrate_repeated, hitrate_other,
                                                ID)].items() if
                                            "default_pjr" in k or "default-pjr" in k}
                    jobinput.append((jobs_480_cache_ranks, 480, ID))
                    jobs_720_cache_ranks = {k: get_data(v,
                                                        measurements=["hitrate_evaluation",
                                                                      "job_event",
                                                                      "pipe_data_volume"])
                                            for k, v in
                                            TAGS_new["week_25_{}_{}_{}_720".format(
                                                hitrate_repeated, hitrate_other,
                                                ID)].items() if
                                            "default_pjr" in k or "default-pjr" in k}
                    jobinput.append((jobs_720_cache_ranks, 720, ID))
                    jobs_960_cache_ranks = {k: get_data(v,
                                                        measurements=["hitrate_evaluation",
                                                                      "job_event",
                                                                      "pipe_data_volume"])
                                            for k, v in
                                            TAGS_new["week_25_{}_{}_{}_960".format(
                                                hitrate_repeated, hitrate_other,
                                                ID)].items() if
                                            "default_pjr" in k or "default-pjr" in k}
                    jobinput.append((jobs_960_cache_ranks, 960, ID))
    #
    #     print(len(jobinput))
        scatter_performance(jobinput, "default_pjr", baseline_key="default PJR, "
                                                                  "default JR",
                            hitrates=False)
        # scatter_performance(jobinput, "default_pjr", baseline_key="default PJR, "
        #                                                           "default JR",
        #                     hitrates=True)
    # plot_scatter_performance("default_pjr")


def make_plots_different_scenarios_old(ID):
    jobs_480_cache_ranks_nopjr = {k: get_data(v)
                            for k, v in TAGS["tags_25_1_0_{}_480cores".format(
            ID)].items() if "no_pjr" in k or "no-pjr" in k}
    jobs_720_cache_ranks_nopjr = {k: get_data(v)
                            for k, v in
                            TAGS["tags_25_1_0_{}_720cores".format(ID)].items() if
                                  "no_pjr" in k or "no-pjr" in k}
    jobs_960_cache_ranks_nopjr = {k: get_data(v)
                            for k, v in TAGS["tags_25_1_0_{}_960cores".format(
            ID)].items() if "no_pjr" in k or "no-pjr" in k}

    joblist = [
        jobs_480_cache_ranks_nopjr,
        jobs_720_cache_ranks_nopjr,
        jobs_960_cache_ranks_nopjr
    ]

    input = {
        key: [jobs[key] for jobs in joblist] for key in
        jobs_480_cache_ranks_nopjr.keys()
    }
    cachehits_comparison(input, "{}_nopjr".format(ID), clusters=[492, 720, 1032])
    scenario_comparison_scores(input, "{}_nopjr".format(ID), clusters=[492, 720, 1032])

    jobs_480_cache_ranks_defaultpjr = {k: get_data(v)
                                  for k, v in
                                  TAGS["tags_25_1_0_{}_480cores".format(ID)].items()
                                       if "default_pjr" in k or "default-pjr" in k}
    jobs_720_cache_ranks_defaultpjr = {k: get_data(v)
                                  for k, v in
                                  TAGS["tags_25_1_0_{}_720cores".format(ID)].items()
                                       if "default_pjr" in k or "default-pjr" in k}
    jobs_960_cache_ranks_defaultpjr = {k: get_data(v)
                                  for k, v in
                                  TAGS["tags_25_1_0_{}_960cores".format(ID)].items()
                                       if "default_pjr" in k or "default-pjr" in k}
    joblist = [
            jobs_480_cache_ranks_defaultpjr,
            jobs_720_cache_ranks_defaultpjr,
            jobs_960_cache_ranks_defaultpjr
        ]
    input = {
        key: [jobs[key] for jobs in joblist] for key in
        jobs_480_cache_ranks_defaultpjr.keys()
    }
    cachehits_comparison(input, "{}_defaultpjr".format(ID), clusters=[492, 720, 1032])
    scenario_comparison_scores(input, "{}_defaultpjr".format(ID), clusters=[492, 720,
                                                                            1032],
                               baseline_key="default PJR, default JR")

def make_plots_different_scenarios(ID):
    jobs_384_cache_ranks_nopjr = {k: get_data(v, measurements=["hitrate_evaluation",
                                                                "job_event",
                                                               "pipe_data_volume"])
                                  for k, v in TAGS_new["week_25_1.0_0.0_{}_384".format(
            ID)].items() if "no_pjr" in k or "no-pjr" in k}
    jobs_480_cache_ranks_nopjr = {k: get_data(v, measurements=["hitrate_evaluation",
                                                                "job_event",
                                                               "pipe_data_volume"])
                            for k, v in TAGS_new["week_25_1.0_0.0_{}_480".format(
            ID)].items() if "no_pjr" in k or "no-pjr" in k}
    jobs_720_cache_ranks_nopjr = {k: get_data(v, measurements=["hitrate_evaluation",
                                                                "job_event",
                                                               "pipe_data_volume"])
                            for k, v in
                            TAGS_new["week_25_1.0_0.0_{}_720".format(ID)].items() if
                                  "no_pjr" in k or "no-pjr" in k}
    jobs_960_cache_ranks_nopjr = {k: get_data(v, measurements=["hitrate_evaluation",
                                                                "job_event",
                                                               "pipe_data_volume"])
                            for k, v in TAGS_new["week_25_1.0_0.0_{}_960".format(
            ID)].items() if "no_pjr" in k or "no-pjr" in k}

    joblist = [
        jobs_384_cache_ranks_nopjr,
        jobs_480_cache_ranks_nopjr,
        jobs_720_cache_ranks_nopjr,
        jobs_960_cache_ranks_nopjr
    ]

    input = {
        key: [jobs[key] for jobs in joblist] for key in
        jobs_480_cache_ranks_nopjr.keys()
    }
    cachehits_comparison(input, "{}_nopjr".format(ID), clusters=[384, 480, 720, 960])
    scenario_comparison_scores(input, "{}_nopjr".format(ID), clusters=[384, 480, 720,
                                                                       960])

    jobs_384_cache_ranks_defaultpjr = {k: get_data(v, measurements=["hitrate_evaluation",
                                                                "job_event",
                                                               "pipe_data_volume"])
                                       for k, v in
                                       TAGS_new[
                                           "week_25_1.0_0.0_{}_384".format(ID)].items()
                                       if "default_pjr" in k or "default-pjr" in k}
    jobs_480_cache_ranks_defaultpjr = {k: get_data(v, measurements=["hitrate_evaluation",
                                                                "job_event",
                                                               "pipe_data_volume"])
                                  for k, v in
                                  TAGS_new["week_25_1.0_0.0_{}_480".format(ID)].items()
                                       if "default_pjr" in k or "default-pjr" in k}
    jobs_720_cache_ranks_defaultpjr = {k: get_data(v, measurements=["hitrate_evaluation",
                                                                "job_event",
                                                               "pipe_data_volume"])
                                  for k, v in
                                  TAGS_new["week_25_1.0_0.0_{}_720".format(ID)].items()
                                       if "default_pjr" in k or "default-pjr" in k}
    jobs_960_cache_ranks_defaultpjr = {k: get_data(v, measurements=["hitrate_evaluation",
                                                                "job_event",
                                                               "pipe_data_volume"])
                                  for k, v in
                                  TAGS_new["week_25_1.0_0.0_{}_960".format(ID)].items()
                                       if "default_pjr" in k or "default-pjr" in k}
    joblist = [
            jobs_384_cache_ranks_defaultpjr,
            jobs_480_cache_ranks_defaultpjr,
            jobs_720_cache_ranks_defaultpjr,
            jobs_960_cache_ranks_defaultpjr
        ]
    input = {
        key: [jobs[key] for jobs in joblist] for key in
        jobs_480_cache_ranks_defaultpjr.keys()
    }
    cachehits_comparison(input, "{}_defaultpjr".format(ID), clusters=[384, 480, 720,
                                                                      960])
    scenario_comparison_scores(input, "{}_defaultpjr".format(ID), clusters=[384, 480,
                                                                            720,
                                                                            960],
                               baseline_key="default PJR, default JR")


pools = Pool(6)
# pools.map(make_plots_different_scenarios, [16, 10, 29])
# pools.map(make_plots_different_scenarios, [16])
make_scatter_plot_performance([16, 10, 29])
# make_scatter_plot_performance([16])
