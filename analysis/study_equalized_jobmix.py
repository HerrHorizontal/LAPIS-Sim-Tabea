import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from influxdb import InfluxDBClient
from tag_mapping import TAGS
from multiprocessing import Pool
from pprint import pprint

client = InfluxDBClient('localhost', 8086, 'admin', 'katze1234', 'lapis')

fontsize = 18

tags_to_label={
        "tag_no_pjr_no_jr": "no coordination",
        "tag_no_pjr_data_jr": "no PJR \n  JR cached data",
        "tag_no_pjr_data_demand_jr": "no PJR \n  JR cached data \n limited cache "
                                     "demand",
        "tag_default_pjr_no_jr": "default PJR",
        "tag_default_pjr_data_jr": "default PJR \n  JR cached data",
        "tag_default_pjr_data_demand_jr": "default PJR \n  JR cached data "
                                       "\n limited cache demand",
        "no-pjr_jad_default": "no coordination",
        "no-pjr_no-jr": "no coordination"}
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
        "no-pjr_no-jr": "no coordination"}


jobs_with_cached_data={10: 35249., 16: 31674., 29: 29291.}

TAGS2 = eval(eval(open("tagmapping2.py", "r").read()))
TAGS3 = eval(eval(open("tagmapping3.py", "r").read()))

def get_data(tag,
             measurements=["hitrate_evaluation", "job_event", "pipe_data_volume",
                           "pipe_status"]):
    data = dict()
    for measurement in measurements:
        print(measurement)
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
                df = data[measurement].groupby(["job", "pool"]).size()
                data[measurement] = data[measurement].groupby("job").last().reset_index()
            elif measurement == "hitrate_evaluation":
                data[measurement] = pd.DataFrame.from_dict(k, orient='columns')
                if data[measurement].empty:
                    print(query)

            else:
                data[measurement] = pd.DataFrame.from_dict(k, orient='columns')
    return data


def cachehits_comparison_excess_study(joblist, labels, id):
    fig, ax = plt.subplots(figsize=[14, 10])
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        print(label)
        # number of cache hits
        hits = []
        tags = []
        for tag, data in jobs.items():
            print(tag, data["hitrate_evaluation"].providesfile.sum())
            if tag == "tag_no_pjr_no_jr" or tag == "no-pjr_jad_default" or tag == \
                    "no-pjr_no-jr":
                print(tag)
                # print(data["hitrate_evaluation"])
                hits.append(data["hitrate_evaluation"].providesfile.sum())
                tags.append(tag)
                if tag == "tag_no_pjr_no_jr" and label in [492, 720, 1032]:
                    # print(data["job_event_complete"])
                    jobs_on_caching_cluster = data["job_event_complete"].pool[data[
                        "job_event_complete"].pool.str.endswith("cache>")].count()
                    print(id, jobs_on_caching_cluster, jobs_with_cached_data[id],
                          data["job_event_complete"].shape[0])
                    ax.axhline(xmin=0, xmax=1 / 6., y=(jobs_with_cached_data[id] / data[
                        "job_event_complete"].shape[0]) * jobs_on_caching_cluster,
                               label="expected number of cache hits ({})".format(label),
                               color="k")
        # hits.extend([0, 0, 0, 0, 0])
        # tags.extend([0, 0, 0, 0, 0])
        nbins = len(tags)
        plt.bar(range(nbins), hits, color="C{}".format(i), edgecolor="C{}".format(i),
                label=label, fill=False, linewidth=3)
    plt.ylabel("number of jobs reading from cache", fontsize=fontsize)
    plt.xticks(range(nbins), [tags_to_label[tag] for tag in tags],
               fontsize=fontsize - 2)
    plt.tick_params(labelsize=fontsize - 2)
    plt.legend(fontsize=fontsize)
    plt.tight_layout()
    plt.savefig("plots/comparison_cachehits_excess_study_{}.pdf".format(id),
                transparent=True)
    plt.close()


def cachehits_comparison(joblist, labels, id):
    fig, ax = plt.subplots(figsize=[14, 10])
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        # number of cache hits
        hits = []
        tags = []
        for tag, data in jobs.items():
            # print(data["hitrate_evaluation"])
            hits.append(data["hitrate_evaluation"].providesfile.sum())
            tags.append(tag)
            if tag == "tag_no_pjr_no_jr" :
                # print(data["job_event_complete"])
                jobs_on_caching_cluster = data["job_event_complete"].pool[data[
                    "job_event_complete"].pool.str.endswith("cache>")].count()
                print(id, jobs_on_caching_cluster, jobs_with_cached_data[id],
                      data["job_event_complete"].shape[0])
                ax.axhline(xmin=0, xmax=1/6., y=(jobs_with_cached_data[id]/data[
                    "job_event_complete"].shape[0])*jobs_on_caching_cluster,
                           label="expected number of cache hits ({})".format(label),
                           color="k")

        print(hits, tags)
        nbins = len(tags)
        plt.bar(range(nbins), hits, color="C{}".format(i), edgecolor="C{}".format(i),
                label=label, fill=False, linewidth=3)
    plt.ylabel("number of jobs reading from cache", fontsize=fontsize)
    plt.xticks(range(nbins),  [tags_to_label[tag] for tag in tags], fontsize=fontsize-2)
    plt.tick_params(labelsize=fontsize - 2)
    plt.legend(fontsize=fontsize)
    plt.tight_layout()
    plt.savefig("plots/comparison_cachehits_{}.pdf".format(id),
                transparent=True)
    plt.close()

    # control plot: cache hits from job event
    fig, ax = plt.subplots(figsize=[14, 10])
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        # number of cache hits
        hits = []
        tags = []
        for tag, data in jobs.items():
            hits.append(data["job_event_complete"].read_from_cache.sum())
            tags.append(tag)
        plt.bar(range (6), hits, color="C{}".format(i), edgecolor="C{}".format(i),
                label=label, fill=False, linewidth=3)
    plt.ylabel("number of jobs reading from cache (via job_event)", fontsize=fontsize)
    plt.tick_params(labelsize=fontsize - 2)
    plt.xticks(range(6), [tags_to_label[tag] for tag in tags], fontsize=fontsize-2)
    plt.legend(fontsize=fontsize)
    plt.tight_layout()
    plt.savefig("plots/comparison_cachehits_job_event_{}.pdf".format(id),
                transparent=True)
    plt.close()

    fig, ax = plt.subplots(figsize=[14, 10])
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        # number of cache hits
        volume = []
        tags = []
        for tag, data in jobs.items():
            volume.append(data["hitrate_evaluation"].volume[data["hitrate_evaluation"].providesfile == 1].sum())
            tags.append(tag)

        plt.bar(range(6), volume, color="C{}".format(i), edgecolor="C{}".format(i),
                label=label, fill=False, linewidth=3)
    plt.ylabel("amount of data read from cache (GB)", fontsize=fontsize)
    plt.tick_params(labelsize=fontsize - 2)
    plt.xticks(range(6), [tags_to_label[tag] for tag in tags], fontsize=fontsize-2)
    plt.legend(fontsize=fontsize)
    plt.tight_layout()
    plt.savefig("plots/comparison_data_read_from_cache_{}.pdf".format(id),
                transparent=True)
    plt.close()

    # number of jobs hitting cache cluster
    fig, ax = plt.subplots(figsize=[14, 10])
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        hits = []
        tags = []
        for tag, data in jobs.items():
            hits.append(data["job_event_complete"].pool[data[
                "job_event_complete"].pool.str.endswith("cache>")].count())
            tags.append(tag)
            print(tag, label, hits[-1])
        plt.bar(range(6), hits, color="C{}".format(i), edgecolor="C{}".format(i),
            label=label, fill=False, linewidth=3)
    plt.ylabel("number of jobs hitting cache cluster", fontsize=fontsize)
    plt.tick_params(labelsize=fontsize - 2)
    plt.xticks(range(6), [tags_to_label[tag] for tag in tags], fontsize=fontsize-2)
    plt.legend(fontsize=fontsize)
    plt.tight_layout()
    plt.savefig(
        "plots/comparison_jobs_on_cachecluster_{}.pdf".format(id),
        transparent=True)
    plt.close()

    # number of jobs hitting cache cluster
    fig, ax = plt.subplots(figsize=[14, 10])
    for i, (jobs, label) in enumerate(zip(joblist, labels)):
        cpuh = []
        tags = []
        for tag, data in jobs.items():
            cpuh.append(data["job_event_complete"].wall_time.divide(3600.).sum())
            tags.append(tag)
        plt.bar(range(6), cpuh, color="C{}".format(i), edgecolor="C{}".format(i),
            label=label, fill=False, linewidth=3)
    plt.ylabel("total cpuh of jobs", fontsize=fontsize)
    plt.tick_params(labelsize=fontsize - 2)
    plt.xticks(range(6), [tags_to_label[tag] for tag in tags], fontsize=fontsize-2)
    plt.legend(fontsize=fontsize)
    plt.tight_layout()
    plt.savefig(
        "plots/comparison_total_cpuh_{}.pdf".format(id),
        transparent=True)
    plt.close()


def walltime_comparison(input, ID):

    # fig, ax = plt.figure(figsize=[14, 10])
    # plt.tick_params(labelsize=fontsize - 2)
    #
    # bins = np.linspace(0, 12, 50)
    # for i, (jobs, label) in enumerate(zip(joblist, labels)):
    #     jobs = jobs["tag_no_pjr_no_jr"]
    #     jobs["job_event_complete"].wall_time[jobs[
    #         "job_event_complete"].pool.str.endswith(
    #         "cache>")].divide(3600.).hist(
    #         bins=bins, alpha=0.3, label="{} cache cluster".format(label),
    #         color="C{}".format(i), edgecolor="C{}".format(i), linewidth=2, normed=True)
    #     jobs["job_event_complete"].wall_time[jobs[
    #         "job_event_complete"].pool.str.endswith(
    #         "None>")].divide(3600.).hist(
    #         bins=bins, histtype="step", label="{} dummy cluster".format(label),
    #         color="C{}".format(i), edgecolor="C{}".format(i), normed=True)
    #
    # plt.xlabel("walltime (h)")
    # # plt.ylabel("number of jobs")
    # plt.legend()
    # plt.tight_layout()
    # plt.savefig("plots/cluster_comparison_walltime_normed_no_coordination.png")
    # plt.close()
    #
    # fig = plt.figure(figsize=[12, 10])
    # bins = np.linspace(0, 6, 50)
    # for i, (jobs, label) in enumerate(zip(joblist, labels)):
    #     jobs = jobs["tag_no_pjr_no_jr"]
    #     jobs["job_event_complete"].wall_time[jobs[
    #         "job_event_complete"].pool.str.endswith(
    #         "cache>")].divide(3600.).hist(
    #         bins=bins, alpha=0.3, label="{} cache cluster".format(label),
    #         color="C{}".format(i), edgecolor="C{}".format(i), linewidth=2, normed=True)
    #     jobs["job_event_complete"].wall_time[jobs[
    #         "job_event_complete"].pool.str.endswith(
    #         "None>")].divide(3600.).hist(
    #         bins=bins, histtype="step", label="{} dummy cluster".format(label),
    #         color="C{}".format(i), edgecolor="C{}".format(i), normed=True)
    #
    # plt.xlabel("walltime (h)")
    # # plt.ylabel()
    # plt.legend()
    # plt.tight_layout()
    # plt.savefig("plots/cluster_comparison_walltime_normed_6h.png")
    # plt.close()

    fig = plt.figure(figsize=[12, 10])
    bins = 50
    for i, (jobs, label) in enumerate(zip(input, ID)):
        jobs = jobs["tag_no_pjr_no_jr"]
        jobs["job_event_complete"][jobs["job_event_complete"].pool.str.endswith(
            "cache>")]["diff"].divide(60.).hist(
            bins=bins, alpha=0.3, label="{} cache cluster".format(label),
            color="C{}".format(i), edgecolor="C{}".format(i))
        if jobs["job_event_complete"][jobs["job_event_complete"].pool.str.endswith(
            "None>")]["diff"].divide(60.).mean() != 0:
            print(jobs["job_event"][jobs["job_event_complete"].pool.str.endswith(
            "None>")]["diff"].divide(60.).mean())

    plt.xlabel("difference original and simulated walltime (min)")
    plt.ylabel("number of jobs")
    plt.legend()
    plt.tight_layout()
    plt.savefig("plots/cluster_comparison_walltime_difference_no_coordination.png")
    plt.close()

    # comparison cpuh
    fig, ax = plt.subplots(figsize=[14, 10])
    width = 0.4
    plt.tick_params(labelsize=fontsize - 2)
    for i, (scenario, clustersizes) in enumerate(input.items()):
        cpuh_cache = []
        cpuh_dummy = []
        for data in clustersizes:
            cpuh_cache.append(data["job_event_complete"][data[
                "job_event_complete"].pool.str.endswith(
                "cache>")].wall_time.divide(3600.).sum())
            cpuh_dummy.append(data["job_event_complete"][data[
                "job_event_complete"].pool.str.endswith(
                "None>")].wall_time.divide(3600.).sum())
            total_cpuh = data["job_event_complete"].wall_time.divide(3600.).sum()
        plt.bar(np.arange(3), cpuh_cache,
                label="cache "+ scenario, color="C{}".format(i),
                edgecolor="C{}".format(i), fill=False, linewidth=4)
        # plt.bar(np.arange(3) + width/2, cpuh_dummy, width=width,
        #         label="dummy "+ scenario, color="C{}".format(i),
        #         edgecolor="C{}".format(i), fill=False, linewidth=4)
    total_cpuh = data["job_event_complete"].wall_time.divide(3600.).sum()
    ax.axhline(xmin=0, xmax=0.33, y=72 / 492. * total_cpuh,
               color="k", label="expected cpuh")
    ax.axhline(xmin=0.33, xmax=0.67, y=72 / 720. * total_cpuh, color="k")
    ax.axhline(xmin=0.67, xmax=1.0, y=72 / 1032. * total_cpuh, color="k")
    plt.ylabel("total cpuh of jobs running on resource type", fontsize=fontsize)
    plt.xlabel("number of cores in resources", fontsize=fontsize)
    plt.xticks(range(3), [492, 720, 1032])
    plt.legend(fontsize=fontsize)
    plt.tight_layout()
    plt.savefig("plots/comparison_cpuh_clusters_{}.pdf".format(ID),
                transparent=True)
    plt.close()


def number_of_jobs_on_caching_cluster(input, ID):

    fig, ax = plt.subplots(figsize=[14, 10])
    plt.tick_params(labelsize=fontsize - 2)
    for i, (scenario, clustersizes) in enumerate(input.items()):
        print(scenario)
        number_of_jobs_cachecluster = []
        number_of_jobs_dummycluster = []
        # print(len(clustersizes))
        for data in clustersizes:
            number_of_jobs_cachecluster.append(
                data["job_event_complete"][data[
                "job_event_complete"].pool.str.endswith("cache>")].shape[0])
            number_of_jobs_dummycluster.append(
                data["job_event_complete"][data[
                "job_event_complete"].pool.str.endswith("None>")].shape[0])
        width = 0.8

        ax.bar(range(3), number_of_jobs_cachecluster, width=width,
                label=scenario, color="C{}".format(i), edgecolor="C{}".format(i),
                fill=False, linewidth=4)
    ax.axhline(xmin=0, xmax=0.33, y=72/492.*data["job_event_complete"].shape[0],
                                  color="k",
               label="expected number of jobs")
    ax.axhline(xmin=0.33, xmax=0.67, y=72/720.*data["job_event_complete"].shape[0],
               color="k")
    ax.axhline(xmin=0.67, xmax=1.0, y=72/1032.*data["job_event_complete"].shape[0],
               color="k")
    plt.xlabel("number of cores in resources", fontsize=fontsize)
    plt.ylabel("number of jobs on caching cluster", fontsize=fontsize)
    plt.legend(fontsize=fontsize)
    # plt.xticks(range(3), [492, 648, 924])
    plt.xticks(range(3), [492, 720, 1032])
    plt.tight_layout()
    plt.savefig("plots/number_of_jobs_on_caching_cluster_{}.pdf".format(ID),
                transparent=True)
    plt.close()


def performance_plots(joblists, clustersizes, IDs,
                      scenarios=["no coordination", "no PJR JR cached data",
                                 "no PJR JR cached data limited cache demand",
                                 "default PJR", "default PJR JR cached data",
                                 "default PJR JR cached data limited cache demand"]):
    scores = {scenario: {} for scenario in scenarios}
    scores_data = {scenario: {} for scenario in scenarios}
    for ID, joblist in zip(IDs, joblists):
        caching_suited_jobs = jobs_with_cached_data[ID]
        jobmix_score = {"njobs": caching_suited_jobs, "data": 0}
        for i, (jobs, clustersize) in enumerate(zip(joblist, clustersizes)):
            for tag, data in jobs.items():
                needed_cores = data["job_event"].original_walltime.sum()/(3600. * 24 * 7)
                cluster_score = clustersize / needed_cores
                scores[tags_to_id[tag]][(cluster_score, jobmix_score["njobs"])] = data[
                    "hitrate_evaluation"].providesfile.sum()
                # scores_data[tags_to_id[tag]][(cluster_score, jobmix_score["njobs"])] = \
                #     data["pipe_data_volume"][data[
                #         "pipe_data_volume"].pipe.str.endswith].current_total.max()
                print(data["pipe_data_volume"]["pipe"].unique())
                # datavolume.append(data["pipe_data_volume"])
            for tag, hits in scores.items():
                if tag != "no coordination":
                    for key, hit in hits.items():
                        scores[tag][key] = (scores[tag][key] - scores["no coordination"][
                            key])/caching_suited_jobs
                    # score_data[tag] = (datavolume[tag] - datavolume["no coordination"])

    print(scores)
    for scenario, data in scores.items():
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
        plt.scatter(df['jobmixscore'], df['clusterscore'], c=df["score"], alpha=0.5)
        cbar = plt.colorbar("cachehit score")
        cbar.set_label()
        ax.set_xlabel("jobmix score")
        ax.set_ylabel("cluster score")
        plt.legend()
        plt.savefig("plots/cachehitscore_{}.pdf".format(scenario), transparent=True)
        plt.close()

    #     # number of cache hits
    #
    #     for tag, data in jobs.items():
    #         print(data["hitrate_evaluation"])
    #         hits.append(data["hitrate_evaluation"].providesfile.sum())
    #         tags.append(tag)
    #         if tag == "tag_no_pjr_no_jr":
    #             # print(data["job_event_complete"])
    #             jobs_on_caching_cluster = data["job_event_complete"].pool[data[
    #                 "job_event_complete"].pool.str.endswith("cache>")].count()
    #             print(id, jobs_on_caching_cluster, jobs_with_cached_data[id],
    #                   data["job_event_complete"].shape[0])
    #             ax.axhline(xmin=0, xmax=1 / 6., y=(jobs_with_cached_data[id] / data[
    #                 "job_event_complete"].shape[0]) * jobs_on_caching_cluster,
    #                        label="expected number of cache hits ({})".format(label),
    #                        color="k")
    #     plt.bar(range(6), hits, color="C{}".format(i), edgecolor="C{}".format(i),
    #             label=label, fill=False, linewidth=3)
    # plt.ylabel("number of jobs reading from cache", fontsize=fontsize)
    # plt.xticks(range(6), [tags_to_label[tag] for tag in tags], fontsize=fontsize - 2)
    # plt.tick_params(labelsize=fontsize - 2)
    # plt.legend(fontsize=fontsize)
    # plt.tight_layout()
    # plt.savefig("plots/comparison_cachehits_{}.pdf".format(id),
    #             transparent=True)
    # plt.close()


def make_plots_different_scenarios(ID):
    jobs_480_cache_ranks = {k: get_data(v)
                            for k, v in TAGS["tags_25_1_0_{}_480cores".format(ID)].items()}
    jobs_720_cache_ranks = {k: get_data(v)
                            for k, v in
                            TAGS["tags_25_1_0_{}_720cores".format(ID)].items()}
    jobs_960_cache_ranks = {k: get_data(v)
                            for k, v in TAGS["tags_25_1_0_{}_960cores".format(ID)].items()}
    print(TAGS2.keys())
    jobs_480_cache_ranks_uniform_qdate = {k:get_data(v)
                                          for k,v in TAGS2["week_25_0.0_0.0_{}" \
                                                           "_480_uniform_qdate".format(ID)].items()}
    jobs_720_cache_ranks_uniform_qdate = {k: get_data(v)
        for k, v in TAGS2["week_25_0.0_0.0_{}_720_uniform_qdate".format(ID)].items()}
    jobs_960_cache_ranks_uniform_qdate = {k: get_data(v)
        for k, v in TAGS2["week_25_0.0_0.0_{}_960_uniform_qdate".format(ID)].items()}

    print(TAGS3.keys())
    jobs_480_cache_ranks_shuffled_uniform_qdate = {k: get_data(v)
                                          for k, v in TAGS3["week_25_0.0_0.0_{}" \
                                                            "_480_shuffled_uniform_qdate".format(
            ID)].items()}
    print(jobs_480_cache_ranks_shuffled_uniform_qdate)
    jobs_720_cache_ranks_shuffled_uniform_qdate = {k: get_data(v)
                                          for k, v in TAGS3[
                                              "week_25_0.0_0.0_{}_" \
                                              "720_shuffled_uniform_qdate".format(
                                                  ID)].items()}
    jobs_960_cache_ranks_shuffled_uniform_qdate = {k: get_data(v)
                                          for k, v in TAGS3[
                                              "week_25_0.0_0.0_{}_" \
                                              "960_shuffled_uniform_qdate".format(
                                                  ID)].items()}

    cachehits_comparison_excess_study(
        [
         jobs_480_cache_ranks, jobs_720_cache_ranks, jobs_960_cache_ranks,
         jobs_480_cache_ranks_uniform_qdate, jobs_720_cache_ranks_uniform_qdate,
         jobs_960_cache_ranks_uniform_qdate,
         jobs_480_cache_ranks_shuffled_uniform_qdate,
         jobs_720_cache_ranks_shuffled_uniform_qdate,
         jobs_960_cache_ranks_shuffled_uniform_qdate],
        [
         492, 720, 1032, "492 uniform qdate", "720 uniform qdate",
         "1032 uniform qdate",
         "492 shuffled uniform qdate",
         "720 shuffled uniform qdate", "1032 shuffled uniform qdate"],
        id=ID)

    cachehits_comparison([jobs_480_cache_ranks, jobs_720_cache_ranks, jobs_960_cache_ranks],
                         [492, 720, 1032],
                         id=ID)



def make_plots_no_coordination(ID):
    job_480_cache = get_data(TAGS["tags_25_1_0_{}_480cores".format(ID)][
                                 "tag_no_pjr_no_jr"])
    job_720_cache = get_data(
        TAGS["tags_25_1_0_{}_720cores".format(ID)]["tag_no_pjr_no_jr"])
    job_960_cache = get_data(
        TAGS["tags_25_1_0_{}_960cores".format(ID)]["tag_no_pjr_no_jr"])
    job_480_equalized = get_data(
        TAGS["tags_25_{}_equalized".format(ID)]["tag_no_pjr_no_jr"])
    job_720_equalized = get_data(
        TAGS["tags_25_{}_equalized_720".format(ID)]["tag_no_pjr_no_jr"])
    job_960_equalized = get_data(
        TAGS["tags_25_{}_equalized_960".format(ID)]["tag_no_pjr_no_jr"])
    job_480_equalized_orig_qdate = get_data(
        TAGS["tags_25_{}_equalized_orig_qdate".format(ID)][
            "tag_no_pjr_no_jr"])
    job_720_equalized_orig_qdate = get_data(
        TAGS["tags_25_{}_equalized_orig_qdate_720".format(ID)][
            "tag_no_pjr_no_jr"])
    job_960_equalized_orig_qdate = get_data(
        TAGS["tags_25_{}_equalized_orig_qdate_960".format(ID)][
            "tag_no_pjr_no_jr"])
    job_480_equalized_orig_qdate_walltime = get_data(
        TAGS["tags_25_{}_equalized_orig_qdate_walltime".format(ID)]["tag_no_pjr_no_jr"])
    job_720_equalized_orig_qdate_walltime = get_data(
        TAGS["tags_25_{}_equalized_orig_qdate_walltime_720".format(ID)][
            "tag_no_pjr_no_jr"])
    job_960_equalized_orig_qdate_walltime = get_data(
        TAGS["tags_25_{}_equalized_orig_qdate_walltime_960".format(ID)][
            "tag_no_pjr_no_jr"])

    input = {
        "all cacheable jobs cached": [
            job_480_cache,
            job_720_cache,
            job_960_cache,
        ],
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
    }

    # walltime_comparison(input, ID)
    number_of_jobs_on_caching_cluster(input, ID)


pools = Pool(6)
# pools.map(make_plots_no_coordination, [16])
# for ID in [10, 29, 16]:
#     make_plots_different_scenarios(ID)
pools.map(make_plots_different_scenarios, [16, 10])

# ID=[16, 10, 29]
# jobs = []
# for id in ID:
#     jobs_480_cache_ranks = {k: get_data(v)
#                          for k, v in TAGS["tags_25_1_0_{}_480cores".format(id)].items()}
#     # jobs_720_cache_ranks = {k: get_data(v)
#     #                         for k, v in
#     #                         TAGS["tags_25_1_0_{}_720cores".format(id)].items()}
#     # jobs_960_cache_ranks = {k: get_data(v)
#     #                         for k, v in TAGS["tags_25_1_0_{}_960cores".format(id)].items()}
#     jobs.append([
#         jobs_480_cache_ranks])
#         # jobs_720_cache_ranks, jobs_960_cache_ranks])
# performance_plots(jobs,
#                   [492],
#                    # 720, 960
#                    # ],
#                   IDs=ID)