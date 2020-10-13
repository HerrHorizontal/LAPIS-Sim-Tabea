#!/usr/bin/env bash
python3.6 /home/tabea/work/lapis/lapis/cli/simulate.py --log-file $log_file  \
                                                --log-telegraf \
                                                --calculation-efficiency $calculation_efficiency \
                                                static \
                                                --job-file $job_file htcondor\
                                                --pool-file $pool_file htcondor\
                                                --storage-files $storage_file $storage_content_file standard \
                                                --remote-throughput $remote_throughput \
                                                --cache-hitrate $cache_hitrate