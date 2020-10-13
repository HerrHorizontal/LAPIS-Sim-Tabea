#!/usr/bin/env bash

#echo "arguments:"
#    for a in ${BASH_ARGV[*]} ; do
#            echo -n "$a "
#    done

ls

remote_throughput=${1}
log_file="${2}.log"
job_file=${3}
pool_cache=${4}
pool_dummy=${5}
storage_file=${6}
calculation_efficiency=${7}
pre_job_rank="${8}"
job_ads="${9}"
machine_ads="${10}"


#arguments=`awk '{a = $1 " " a} END {print a}' config_classad.txt`
#cat config_classad.txt

#echo "remote_throughput" $remote_throughput
#echo "log_file" $log_file
#echo "job_file" $job_file
#echo "pool_cache" $pool_cache
#echo "pool_dummy" $pool_dummy
#echo "storage_file" $storage_file
#echo "calculation_efficiency" $calculation_efficiency
#echo "pre job rank" $pre_job_rank
#echo "machine ads" $machine_ads
#echo "job ads" $job_ads
#
#ls $job_file
#ls $pool_cache
#ls $pool_dummy
#ls $storage_file

# get lapis
#git config --global user.email "tabea.fessenbecker@student.kit.edu"
#git config --global user.name "tfesenbecker"
source /cvmfs/cms.cern.ch/slc7_amd64_gcc700/external/git/2.13.0-omkpbe/etc/profile.d/init.sh
source /cvmfs/cms.cern.ch/slc7_amd64_gcc700/external/pcre/8.37-omkpbe/etc/profile.d/init.sh
git clone https://github.com/MatterMiners/lapis.git
#git clone https://github.com/MaineKuehn/classad.git
git clone https://github.com/MaineKuehn/usim.git
echo 'after git clone'


python3.7 -m venv lapis-venv && source lapis-venv/bin/activate
cd lapis
git fetch origin
git checkout feature/filebasedhitratecaching
git status

cd ..

#ls -la lapis/
python3.7 -m  pip install --upgrade --no-cache-dir pip
python3.7 -m  pip install --no-cache-dir usim/
#python3.7 -m  pip install --no-cache-dir classad/
python3.7 -m  pip install --no-cache-dir lapis/
python3.7 -m  pip install numpy
# execute simulation

echo "log: " $log_file

locale -a
export LC_ALL=en_US.utf8
export LANG=en_US.utf8

#echo $1, $2, $3, $4, $5

cd lapis

#echo $job_file $pool_cache $pool_dummy $log_file $remote_throughput $calculation_efficiency $pre_job_rank $machine_ads $job_ads

python3.7 custom_simulate_batchsystem.py "../${job_file}" "../${pool_cache}" "../${pool_dummy}" "../${storage_file}" "$log_file" "$remote_throughput" "$calculation_efficiency" "$pre_job_rank" "$machine_ads" "$job_ads"

#ls -lah
#cd ..
#ls -lah