from tools.submit_simulation import SimulationSubmitterCustom

machine_ad_defaults = "requirements = target.requestcpus <= my.cpus rank = 0"

submitter = SimulationSubmitterCustom(
    remote_throughput=0.75,
    identifier="testsimulation",
    path_to_jobmix="/ceph/tfesenbecker/simulation_environment/fullsimulation"
                      "/jobmix/modified",
    path_to_resources="/ceph/tfesenbecker/simulation_environment/fullsimulation"
                      "/resources",
    calculation_efficiency=0.99
)


htcondor_pre_job_rank = "1000000 - 100000 * my.cpus"
job_ad_defaults = "requirements = my.requestcpus <= target.cpus rank = 0"
job_ad_data = "requirements = my.requestcpus <= target.cpus rank = target.cached_data"
job_ad_data_demand_25 = "requirements = my.requestcpus <= target.cpus rank = " \
                      "target.cached_data * (target.cache_demand < 18.0)"
job_ad_data_demand_50 = "requirements = my.requestcpus <= target.cpus rank = " \
                      "target.cached_data * (target.cache_demand < 36.0)"
job_ad_data_demand_75 = "requirements = my.requestcpus <= target.cpus rank = " \
                      "target.cached_data * (target.cache_demand < 54.0)"
pre_job_ranks = {"no-pjr": "0", "default-pjr": htcondor_pre_job_rank}
job_ads = {"no-jr": job_ad_defaults, "jad-data": job_ad_data,
           "jad-data-demand-25": job_ad_data_demand_25, "jad-data-demand-50":
               job_ad_data_demand_50, "jad-data-demand-75": job_ad_data_demand_75}

for jobmixID in [16, 29, 10]:
    for pjr, pre_job_rank in pre_job_ranks.items():
        for jad, job_ad in {"no-jad": job_ad_defaults}.items():
        # for jad, job_ad in job_ads.items():
            jobfiles = [
                        "week_25_{}_equalized_jobs_no_inputfiles"
                        "_input.json".format(jobmixID),
                        "week_25_{}_equalized_jobs_no_inputfiles_orig_qdate"
                        "_input.json".format(jobmixID),
                        "week_25_{}_equalized_jobs_no_inputfiles_orig_qdate_orig_"
                        "walltime_input.json".format(jobmixID),
                        "week_25_{}_uniform_qdate"
                        "_input.json".format(jobmixID),
                        "week_25_{}_shuffled_uniform_qdate"
                        "_input.json".format(jobmixID)]
            scenarios = [
                        "equalized",
                        "equalized_orig_qdate",
                        "equalized_orig_qdate_walltime",
                        "uniform_qdate",
                        "shuffled_uniform_qdate"]
            for jobfile, identifier in zip(jobfiles, scenarios):
                print(jobfile, "\n")
                submitter.submit(
                        jobfile=jobfile,
                        pool_cache="sg_machines_shared_cache.csv",
                        pool_dummy="dummycluster_648cores_split24.csv",
                        storage_file="sg_caches_shared.csv",
                        initial_dir_add="sim-2020-02-28",
                        identifier="week_25_0.0_0.0_{}_720_{}_{}_{}".format(jobmixID,
                                                                           pjr,
                                                                    jad, identifier),
                        pre_job_rank=pre_job_rank,
                        job_ads=job_ad
                    )
                submitter.submit(
                        jobfile=jobfile,
                        pool_cache="sg_machines_shared_cache.csv",
                        pool_dummy="dummycluster_888cores_split24.csv",
                        storage_file="sg_caches_shared.csv",
                        initial_dir_add="sim-2020-02-28",
                        identifier="week_25_0.0_0.0_{}_960_{}_{}_{}".format(jobmixID,
                                                                           pjr,
                                                                    jad, identifier),
                        pre_job_rank=pre_job_rank,
                        job_ads=job_ad
                    )
                submitter.submit(
                        jobfile=jobfile,
                        pool_cache="sg_machines_shared_cache.csv",
                        pool_dummy="dummycluster_408cores_split24.csv",
                        storage_file="sg_caches_shared.csv",
                        initial_dir_add="sim-2020-02-28",
                        identifier="week_25_0.0_0.0_{}_480_{}_{}_{}".format(jobmixID,
                                                                           pjr,
                                                                    jad, identifier),
                        pre_job_rank=pre_job_rank,
                        job_ads=job_ad

                )
                submitter.submit(
                    jobfile=jobfile,
                    pool_cache="sg_machines_shared_cache.csv",
                    pool_dummy="dummycluster_312cores_split24.csv",
                    storage_file="sg_caches_shared.csv",
                    initial_dir_add="sim-2020-02-28",
                    identifier="week_25_0.0_0.0_{}_384_{}_{}_{}".format(jobmixID,
                                                                        pjr,
                                                                        jad,
                                                                        identifier),
                    pre_job_rank=pre_job_rank,
                    job_ads=job_ad

                )

# for jobmixID in [16, 29, 10]:
#     for h_r in [1.0]:
#         for h_o in [0.0]:
#             for pjr, pre_job_rank in pre_job_ranks.items():
#                 for jad, job_ad in job_ads.items():
#                     submitter.submit(
#                         jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o,
#                                                                      jobmixID),
#                         pool_cache="sg_machines_shared_cache.csv",
#                         pool_dummy="dummycluster_408cores_split24.csv",
#                         storage_file="sg_caches_shared.csv",
#                         initial_dir_add="sim-2020-02-25",
#                         identifier="week_25_{}_{}_{}_480_{}_{}".format(h_r, h_o,
#                                                                        jobmixID,
#                                                                        pjr, jad),
#                         pre_job_rank=pre_job_rank,
#                         job_ads=job_ad
#
#                     )
#                     submitter.submit(
#                         jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o,
#                                                                      jobmixID),
#                         pool_cache="sg_machines_shared_cache.csv",
#                         pool_dummy="dummycluster_648cores_split24.csv",
#                         storage_file="sg_caches_shared.csv",
#                         initial_dir_add="sim-2020-02-25",
#                         identifier="week_25_{}_{}_{}_720_{}_{}".format(h_r, h_o,
#                                                                        jobmixID,
#                                                                        pjr, jad),
#                         pre_job_rank=pre_job_rank,
#                         job_ads=job_ad
#
#                     )
#                     submitter.submit(
#                         jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o,
#                                                                      jobmixID),
#                         pool_cache="sg_machines_shared_cache.csv",
#                         pool_dummy="dummycluster_888cores_split24.csv",
#                         storage_file="sg_caches_shared.csv",
#                         initial_dir_add="sim-2020-02-25",
#                         identifier="week_25_{}_{}_{}_888_{}_{}".format(h_r, h_o,
#                                                                        jobmixID,
#                                                                        pjr, jad),
#                         pre_job_rank=pre_job_rank,
#                         job_ads=job_ad
#
#                     )

