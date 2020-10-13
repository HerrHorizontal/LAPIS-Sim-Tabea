import numpy as np
import pandas as pd
import json


def get_jobs_as_dataframe(job_input_file):
    if job_input_file.split(".")[-1] == "json":
        dataframe = pd.read_json(job_input_file, orient="records")
    elif job_input_file.split(".")[-1] == "csv":
        dataframe = pd.read_csv(job_input_file, delimiter=",")
    else:
        print("job input file format was not recognized: ", job_input_file)
        return None
    return dataframe


def log_to_dataframe(log_file):
    dataframe = pd.read_json(log_file, orient="records")
    return dataframe


def get_job_events_from_log(log_file):
    with open("/home/tabea/work/simulation_logs/copy_jobs_{}.log".format(i),
              "r") as log:
        for line in log:
            readline = json.loads(line)
            if readline["message"] == "job_event" and readline.get("wall_time",
                                                                   None):

    return


def get_job_data_volume(inputfileinfo):
    tot = 0
    for key, specs in inputfileinfo.items():
        try:
            tot += specs["usedsize"]
        except KeyError:
            tot += specs["filesize"]

    return tot


def get_total_data_volume(jobs: pd.DataFrame):
    return jobs.Inputfiles.apply(get_job_data_volume).sum()


def get_number_of_files(inputfileinfo):
    return len(inputfileinfo.keys())


def get_total_number_of_files(jobs:pd.DataFrame):
    return jobs.Inputfiles.apply(get_number_of_files).sum()





