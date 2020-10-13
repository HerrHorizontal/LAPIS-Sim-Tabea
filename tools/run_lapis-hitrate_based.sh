#!/usr/bin/env bash

echo "arguments:"
    for a in ${BASH_ARGV[*]} ; do
            echo -n "$a "
    done

cache_hitrate=$1
remote_throughput=$2
log_file="${3}_${1}.log"
job_file=$4
pool_file=$5
storage_file=$6
storage_content_file=$7
calculation_efficiency=$8

# get lapis
# git config --global user.email "tabea.fessenbecker@student.kit.edu"
# git config --global user.name "tfesenbecker"
git clone https://github.com/MatterMiners/lapis.git
echo 'after git clone'

python3.6 -m venv lapis-venv && source lapis-venv/bin/activate
cd lapis
git fetch origin
git checkout feature/caching
git status

cd ..
pwd
ls -la lapis/
pip install --upgrade --no-cache-dir pip
pip install --no-cache-dir lapis/

# execute simulation

echo $log_file

locale -a
export LC_ALL=en_US.utf8
export LANG=en_US.utf8

cd $3run_lapis-hitrate_based.sh

python3.6 lapis/lapis/cli/simulate.py --log-file $log_file  \
                                                --log-telegraf \
                                                --calculation-efficiency $calculation_efficiency \
                                                static \
                                                --job-file $job_file htcondor\
                                                --pool-file $pool_file htcondor\
                                                --storage-files $storage_file $storage_content_file standard \
                                                --remote-throughput $remote_throughput \
                                                --cache-hitrate $cache_hitrate
