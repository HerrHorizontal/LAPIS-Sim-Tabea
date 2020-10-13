import htcondor
import htcondor
import numpy as np
from copy import deepcopy
import os


machine_ad_defaults = "requirements = target.requestcpus <= my.cpus rank = 0"
job_ad_defaults = "requirements = my.requestcpus <= target.cpus && my.requestmemory " \
                  "<= target.memory rank = 0"
pre_job_rank_defaults = "0"


class SimulationSubmitterCustom(object):
    SUBMISSION_TEMPLATE = dict(
       executable="tools/run_lapis_custom_simulation.sh",
       universe="docker",
       docker_image="mschnepf/slc7-condocker:python37",
       should_transfer_files="YES",
       when_to_transfer_output="ON_EXIT_OR_EVICT",
       transfer_input_files="/home/tfesenbecker/simulation_environment/tools/"
                            "run_lapis_custom_simulation.sh, ",
       # transfer_output_files="lapis/fullsim.log",
       initialdir="/portal/ekpbms3/home/tfesenbecker/simulation/",
       request_disk="1000000",
       output="out.$(Process)",
       error="err.$(Process)",
       log="log.$(Process)",
       request_memory="2048",
       # Requirements="(TARGET.CloudSite == 'BWFORCLUSTER')",
       arguments="")

    def __init__(self, remote_throughput, identifier, path_to_jobmix,
                 path_to_resources, calculation_efficiency):
        self.remote_throughput = remote_throughput
        self.identifier = identifier
        self.prefix_jobmix = path_to_jobmix
        self.prefix_resources = path_to_resources
        self.calculation_efficiency = calculation_efficiency

    def submit(self, jobfile, pool_cache, pool_dummy, storage_file,
               pre_job_rank=pre_job_rank_defaults, machine_ads=machine_ad_defaults,
               job_ads=job_ad_defaults, identifier=False, initial_dir_add=False):
        if identifier:
            self.identifier=identifier
        print(self.identifier)

        job_description = deepcopy(self.SUBMISSION_TEMPLATE)
        job_description["transfer_input_files"] += "{ptj}/{jobmix}, " \
                                                   "{ptr}/{pool_cache}, " \
                                                   "{ptr}/{pool_dummy}, " \
                                                   "{ptr}/{storage}".format(
            ptj=self.prefix_jobmix, ptr=self.prefix_resources, jobmix=jobfile,
            pool_dummy=pool_dummy, pool_cache=pool_cache, storage=storage_file)
        job_description["transfer_output_files"] = "lapis/{}.log".format(
            self.identifier)

        if initial_dir_add:
            job_description["initialdir"] += "/{}".format(initial_dir_add)

        job_description["initialdir"] += "/{}".format(self.identifier)
        job_description["output"] = "out.{}".format(self.identifier)
        job_description["error"] = "err.{}".format(self.identifier)
        job_description["log"] = "log.{}".format(self.identifier)
        job_description["JobBatchName"] = self.identifier

        schedd = htcondor.Schedd()
        print(job_description["initialdir"])
        try:
            os.mkdir(job_description["initialdir"])
        except OSError as e:
            # print(e)
            pass

        # print(
        #     "scheduler configuration: \n "
        #     "\tpre job rank: {} \n\n"
        #     "\tmachine classad:\n {}\n\n"
        #     "\tjob classad: {}".format(pre_job_rank, machine_ads, job_ads)
        # )

        with schedd.transaction() as txn:
            job_description["arguments"] = '"{remote} {log_id} {jobs} ' \
                                           '{pool_cache} {pool_dummy} {caches} ' \
                                           '{calc_eff} \'{pre_job_rank}\' ' \
                                           '\'{job_ads}\' \'{machine_ads}\' "'.format(
                remote=self.remote_throughput,
                log_id=self.identifier, jobs=jobfile,
                pool_cache=pool_cache, pool_dummy=pool_dummy, caches=storage_file,
                calc_eff=self.calculation_efficiency, pre_job_rank=pre_job_rank,
                job_ads=job_ads, machine_ads=machine_ads)

            # print(job_description)
            sub = htcondor.Submit(job_description)
            # print(sub)
            print(sub.queue(txn))
