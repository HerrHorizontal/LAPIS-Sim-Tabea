import htcondor
import numpy as np
import os

class SimulationSubmitter(object):
    SUBMISSION_TEMPLATE = dict(executable="tools/run_lapis-hitrate_based.sh",
                               universe="docker",
                               docker_image="mschnepf/slc7-condocker",
                               should_transfer_files="YES",
                               when_to_transfer_output="ON_EXIT_OR_EVICT",
                               transfer_input_files="../../../tools/run_lapis-hitrate_based.sh",
                               # transfer_output_files="",
                               initialdir="/home/tfesenbecker/simulation_environment"
                                          "/workdir/simulation",
                               output="out.$(Process)",
                               error="err.$(Process)",
                               log="log.$(Process)",
                               request_memory="2048",
                               # Requirements="(TARGET.CLOUD_SITE == 'condocker')",
                               arguments="")

    hitrates = np.linspace(0.01, 0.99, 2)

    def __init__(self, remote_throughput, identifier, job_file, pool_file,
                 storage_file, storage_content_file, calculation_efficiency=0.8,
                 steps_per_job=1):
        self.remote_throughput = remote_throughput
        self.identifier = identifier
        self.job_file = job_file
        self.pool_file = pool_file
        self.storage_file = storage_file
        self.storage_content_file = storage_content_file
        self.calculation_efficiency = calculation_efficiency
        self.steps_per_job = steps_per_job

    def submit(self):
        job_description = self.SUBMISSION_TEMPLATE
        job_description["transfer_input_files"] = ", " \
                                                   "../../../testdata/single_copy_job.json, " \
                                                   "../../../testdata/single_cluster.csv, " \
                                                   "../../../testdata/single_cache.csv," \
                                                   "../../../testdata/single_cache_content.csv"
        job_description["initialdir"] += "/{}".format(self.identifier)
        schedd = htcondor.Schedd()

        try:
            os.mkdir("/home/tfesenbecker/simulation_environment/workdir/simulation/"
                 "{}".format(self.identifier))
        except OSError:
            pass

        with schedd.transaction() as txn:
            for hitrate in self.hitrates:
                job_description["arguments"] = "{hitrate} {remote} {log_id} {jobs} " \
                                               "{pools} {caches} {cache_ini} " \
                                               "{calc_eff}".format(
                    hitrate=hitrate, remote=self.remote_throughput,
                    log_id=self.identifier, jobs=self.job_file,
                    pools=self.pool_file, caches=self.storage_file,
                    cache_ini=self.storage_content_file,
                    calc_eff=self.calculation_efficiency)

                print(job_description)
                sub = htcondor.Submit(job_description)
                print(sub.queue(txn))

