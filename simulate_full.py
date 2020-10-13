from tools.submit_simulation import SimulationSubmitterCustom
import numpy as np
import time

duration = "day"

job_ad_defaults = "requirements = my.requestcpus <= " \
                  "target.cpus && my.requestmemory " \
                  "<= target.memory rank = 0"
machine_ad_defaults = "requirements = target.requestcpus <= my.cpus rank = 0"

#
# submitter = SimulationSubmitterCustom(
#     remote_throughput=0.75,
#     identifier="testsimulation",
#     path_to_jobmix="  ",
#     path_to_resources="/ceph/tfesenbecker/simulation_environment/fullsimulation"
#                       "/resources",
#     calculation_efficiency=0.99
# )
# #
#
# submitter.submit(
#     jobfile="job_list_minimal_only_cpu.json",
#     pool_cache="minimal_pool.csv",
#     pool_dummy="dummycluster.csv",
#     storage_file="sg_caches_shared.csv",
#     identifier="cachetest",
#     pre_job_rank="0",
#     job_ads=job_ad_defaults,
#     machine_ads=machine_ad_defaults
# )

# submitter.submit(
#     jobfile="test_12h_jobinput.json",
#     pool_cache="sg_machines_shared_cache.csv",
#     pool_dummy="dummycluster.csv",
#     storage_file="sg_caches_shared.csv",
#     identifier="performancetest_12h"
# )

# submitter.submit(
#     jobfile="test_24h_jobinput.json",
#     pool_cache="sg_machines_shared_cache.csv",
#     pool_dummy="dummycluster.csv",
#     storage_file="sg_caches_shared.csv",
#     identifier="performancetest_24h"
# )
#
# submitter.submit(
#     jobfile="resampled_reduced_025week_16_jobinput.json",
#     pool_cache="sg_machines_shared_cache.csv",
#     pool_dummy="dummycluster.csv",
#     storage_file="sg_caches_shared.csv",
#     identifier="resampled_reduced_025week_16"
# )

# submitter.submit(
#     jobfile="resampled_week_16_jobinput.json",
#     pool_cache="sg_machines_shared_cache.csv",
#     pool_dummy="dummycluster.csv",
#     storage_file="sg_caches_shared.csv",
#     identifier="resampled_week_16"
# )
#
submitter = SimulationSubmitterCustom(
    remote_throughput=0.75,
    identifier="testsimulation",
    path_to_jobmix="/ceph/tfesenbecker/simulation_environment/fullsimulation/jobmix/modified",
    path_to_resources="/ceph/tfesenbecker/simulation_environment/fullsimulation/resources",
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

# for ID in [16, 10, 29]:
for ID in [29]:
    for h_r in [0.0, 1.0, 0.33, 0.5, 0.67]:
        for h_o in [0.0, 0.01, 0.05]:
            for pjr, pre_job_rank in pre_job_ranks.items():
                for jad, job_ad in job_ads.items():
                    submitter.submit(
                        jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o, ID),
                        pool_cache="sg_machines_shared_cache.csv",
                        pool_dummy="dummycluster_408cores_split24.csv",
                        storage_file="sg_caches_shared.csv",
                        initial_dir_add="sim-2020-02-28",
                        identifier="week_25_{}_{}_{}_480_{}_{}".format(h_r, h_o,
                                                                          ID, pjr,
                                                                          jad),
                        pre_job_rank=pre_job_rank,
                        job_ads=job_ad

                    )
                    submitter.submit(
                        jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o, ID),
                        pool_cache="sg_machines_shared_cache.csv",
                        pool_dummy="dummycluster_648cores_split24.csv",
                        storage_file="sg_caches_shared.csv",
                        initial_dir_add="sim-2020-02-28",
                        identifier="week_25_{}_{}_{}_720_{}_{}".format(h_r, h_o,
                                                                   ID, pjr,
                                                                   jad),
                        pre_job_rank=pre_job_rank,
                        job_ads=job_ad

                    )
                    submitter.submit(
                        jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o, ID),
                        pool_cache="sg_machines_shared_cache.csv",
                        pool_dummy="dummycluster_888cores_split24.csv",
                        storage_file="sg_caches_shared.csv",
                        initial_dir_add="sim-2020-02-28",
                        identifier="week_25_{}_{}_{}_960_{}_{}".format(h_r, h_o,
                                                                   ID, pjr,
                                                                   jad),
                        pre_job_rank=pre_job_rank,
                        job_ads=job_ad

                    )
                    submitter.submit(
                        jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o, ID),
                        pool_cache="sg_machines_shared_cache.csv",
                        pool_dummy="dummycluster_312cores_split24.csv",
                        storage_file="sg_caches_shared.csv",
                        initial_dir_add="sim-2020-02-28",
                        identifier="week_25_{}_{}_{}_384_{}_{}".format(h_r, h_o,
                                                                   ID, pjr,
                                                                   jad),
                        pre_job_rank=pre_job_rank,
                        job_ads=job_ad

                    )


# for ID in [16, 29, 10]:
#     for h_r in [0.0]:
#         for h_o in [0.0]:
#                 for pjr, pre_job_rank in pre_job_ranks.items():
#                     for jad, job_ad in job_ads.items():
#                         for jobfile, ident in zip([
#                             "week_25_{}_{}_{"
#                             "}_equalized_jobs_no_inputfiles_input.json".format(
#                                 h_r, h_o, ID),
#                             "week_25_{}_equalized_jobs_no_inputfiles_orig_qdate_input.json"
#                             "".format(ID),
#                             "week_25_{}_equalized_jobs_no_inputfiles_orig_qdate_orig_"
#                             "walltime_input.json".format(ID)], ["equalized",
#                                                                 "equalized_orig_qdate",
#                                                                 "equalized_orig_qdate_walltime"]):
#                             print(jobfile, "\n")
                            # submitter.submit(
                            #             jobfile=jobfile,
                            #             # jobfile="week_25_{}_{}_{}_equalized_jobs_no_inputfiles_input.json".format(h_r, h_o, ID),
                            #             # jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o, ID),
                            #             pool_cache="sg_machines_shared_cache.csv",
                            #             pool_dummy="dummycluster_648cores_split.csv",
                            #             storage_file="sg_caches_shared.csv",
                            #             initial_dir_add="sim-2020-02-24",
                            #             identifier="week_25_{}_{}_{}_{}_{}_{}"
                            #                        "_648".format(h_r,
                            #                                                          h_o,
                            #                                                               ID, pjr,
                            #                                                               jad, ident),
                            #             pre_job_rank=pre_job_rank,
                            #             job_ads=job_ad
                            #         )
                            # submitter.submit(
                            #             jobfile=jobfile,
                            #             # jobfile="week_25_{}_{}_{}_equalized_jobs_no_inputfiles_input.json".format(h_r, h_o, ID),
                            #             # jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o, ID),
                            #             pool_cache="sg_machines_shared_cache.csv",
                            #             pool_dummy="dummycluster_888cores_split.csv",
                            #             storage_file="sg_caches_shared.csv",
                            #             initial_dir_add="sim-2020-02-24",
                            #             identifier="week_25_{}_{}_{}_{}_{}_{}"
                            #                        "_888".format(
                            #                 h_r,
                            #                                                            h_o,
                            #                                                               ID, pjr,
                            #                                                               jad, ident),
                            #             pre_job_rank=pre_job_rank,
                            #             job_ads=job_ad
                            #         )
                            # submitter.submit(
                            #         jobfile=jobfile,
                            #         # jobfile="week_25_{}_{}_{}_equalized_jobs_no_inputfiles_input.json".format(h_r, h_o, ID),
                            #         # jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o, ID),
                            #         pool_cache="sg_machines_shared_cache.csv",
                            #         pool_dummy="dummycluster_408cores_split48.csv",
                            #         storage_file="sg_caches_shared.csv",
                            #         initial_dir_add="sim-2020-02-24",
                            #         identifier="week_25_{}_{}_{}_{}_{}_{}".format(
                            #             h_r,
                            #                                                         h_o,
                            #                                                    ID, pjr,
                            #                                                    jad,
                            #             ident),
                            #         pre_job_rank=pre_job_rank,
                            #         job_ads=job_ad
                            #
                            # )


# for ID in [16, 29, 10]:
#     for h_r in [0.0, 0.5, 1.0]:
#         for h_o in [0.0, 0.05]:
#                 for pjr, pre_job_rank in pre_job_ranks.items():
#                     for jad, job_ad in job_ads.items():
#                         submitter.submit(
#                                     jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o, ID),
#                                     pool_cache="sg_machines_shared_cache.csv",
#                                     pool_dummy="dummycluster_648cores_split.csv",
#                                     storage_file="sg_caches_shared.csv",
#                                     initial_dir_add="sim-2020-02-23",
#                                     identifier="week_25_{}_{}_{}_{}_{}_648".format(h_r,
#                                                                                  h_o,
#                                                                                       ID, pjr,
#                                                                                       jad),
#                                     pre_job_rank=pre_job_rank,
#                                     job_ads=job_ad
#                                 )
#                         submitter.submit(
#                                     jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o, ID),
#                                     pool_cache="sg_machines_shared_cache.csv",
#                                     pool_dummy="dummycluster_888cores_split.csv",
#                                     storage_file="sg_caches_shared.csv",
#                                     initial_dir_add="sim-2020-02-23",
#                                     identifier="week_25_{}_{}_{}_{}_{}_888".format(h_r,
#                                                                                  h_o,
#                                                                                       ID, pjr,
#                                                                                       jad),
#                                     pre_job_rank=pre_job_rank,
#                                     job_ads=job_ad
#                                 )

# time.sleep(60)
#
# #
# for ID in [16, 10, 29]:
#     for h_r in [0.0, 0.33, 0.5, 0.67, 1.0]:
#         for h_o in [0.0, 0.01, 0.05]:
#             for pjr, pre_job_rank in pre_job_ranks.items():
#                 for jad, job_ad in job_ads.items():
#                     print(job_ad)
#                     # submitter.submit(
#                     #     jobfile="week_100_{}_{}_{}_input.json".format(h_r, h_o, ID),
#                     #     pool_cache="sg_machines_shared_cache.csv",
#                     #     pool_dummy="dummycluster_1428cores_split48.csv",
#                     #     storage_file="sg_caches_shared.csv",
#                     #     initial_dir_add="sim-2020-02-20",
#                     #     identifier="week_100_{}_{}_{}_{}_{}".format(h_r, h_o, ID,
#                     #                                                        pjr, jad),
#                     #     pre_job_rank=pre_job_rank,
#                     #     job_ads=job_ad
#                     #
#                     # )
#                     submitter.submit(
#                         jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o, ID),
#                         pool_cache="sg_machines_shared_cache.csv",
#                         pool_dummy="dummycluster_408cores_split48.csv",
#                         storage_file="sg_caches_shared.csv",
#                         initial_dir_add="sim-2020-02-23",
#                         identifier="week_25_{}_{}_{}_{}_{}".format(h_r, h_o,
#                                                                           ID, pjr,
#                                                                           jad),
#                         pre_job_rank=pre_job_rank,
#                         job_ads=job_ad
#
#                     )

    #

# time.sleep(5*60.)
#
# for ID in [16, 10, 29]:
#     for h_r in [0.0, 0.1, 0.33, 0.5, 0.67, 1.0]:
#         for h_o in [0.0, 0.01, 0.05]:
#             for pjr, pre_job_rank in pre_job_ranks.items():
#                 for jad, job_ad in job_ads.items():
#                     print(job_ad)
#                     submitter.submit(
#                         jobfile="week_100_{}_{}_{}_input.json".format(h_r, h_o, ID),
#                         pool_cache="sg_machines_shared_cache.csv",
#                         pool_dummy="dummycluster_1428cores_split48.csv",
#                         storage_file="sg_caches_shared.csv",
#                         initial_dir_add="sim-2020-02-20",
#                         identifier="week_100_{}_{}_{}_{}_{}".format(h_r, h_o, ID,
#                                                                            pjr, jad),
#                         pre_job_rank=pre_job_rank,
#                         job_ads=job_ad

                    # )
                    # submitter.submit(
                    #     jobfile="week_25_{}_{}_{}_input.json".format(h_r, h_o, ID),
                    #     pool_cache="sg_machines_shared_cache.csv",
                    #     pool_dummy="dummycluster_408cores_split48.csv",
                    #     storage_file="sg_caches_shared.csv",
                    #     initial_dir_add="sim-2020-02-20",
                    #     identifier="week_25_{}_{}_{}_{}_{}".format(h_r, h_o,
                    #                                                       ID, pjr,
                    #                                                       jad),
                    #     pre_job_rank=pre_job_rank,
                    #     job_ads=job_ad
                    #
                    # )
