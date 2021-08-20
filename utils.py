import pandas as pd
from datetime import datetime, timedelta, time, date
import numpy as np
from scipy.spatial import distance
import matplotlib.pyplot as plt
import json, pickle
import time as tick

######################################################################################
# time testing function
######################################################################################
def timeit(f):
    def timed(*args, **kw):
        ts = tick.time()
        result = f(*args, **kw)
        te = tick.time()

        print('func: {} took: {:2.4f} sec'.format(f.__name__, te - ts))
        return result

    return timed

######################################################################################
# split and merge the time interval across the two days
######################################################################################

def generate_time_range(row):
    start_date = row['start_time'].date()
    end_date = row['end_time'].date()

    if (start_date < end_date):
        result = []
        divide_line_dt = datetime.combine(start_date + timedelta(days=1), time(0, 0))
        result.append((row['start_time'], divide_line_dt))

        while (divide_line_dt.date() < end_date):
            result.append((divide_line_dt, divide_line_dt + timedelta(days=1)))
            divide_line_dt = divide_line_dt + timedelta(days=1)

        result.append((divide_line_dt, row['end_time']))

        return result

    else:
        return [(row['start_time'], row['end_time'])]


def split_continuous_time(df):
    # cross time range
    cross = df[df['start_time'].dt.date != df['end_time'].dt.date].copy()
    non_cross = df[df['start_time'].dt.date == df['end_time'].dt.date]

    cross['time_range'] = cross.apply(lambda row: generate_time_range(row), axis=1)

    cross = cross.explode('time_range')
    cross['start_time'] = cross.apply(lambda row: row['time_range'][0], axis=1)
    cross['end_time'] = cross.apply(lambda row: row['time_range'][1], axis=1)

    cross = cross.drop('time_range', axis=1)

    df = pd.concat([non_cross, cross])

    return df

#######################################################################################################
# File read and write
#######################################################################################################
def write_pickle(data, path):
    with open(path, "wb") as f:
        pickle.dump(data, f)


def read_pickle(path):
    with open(path, "rb") as f:
        return pickle.load(f)


def write_json(data, path):
    with open(path, "w") as f:
        json.dump(data, f)


def read_json(path):
    with open(path, "rb") as f:
        return json.load(f)