from os import listdir
from os.path import isfile, join
from datetime import datetime, timedelta, time
import pandas as pd
import json
import re
from utils import timeit


class RawFileProcessor:

    def __init__(self, dir_path, out_dir="./data/data"):
        self.raw_file_dir = dir_path
        self.out_dir = out_dir
        self.invalid_buildings = dict()

    # get all file names and information within a date range
    @timeit
    def get_all_file_names(self, start_date=datetime(2020, 7, 1), end_date=datetime(2020, 12, 1), verbose=True):
        """
        Get raw files names within the specified date range
        @param start_date: begin date
        @param end_date: end date
        @return: List of (file names, time_begin, time_end)
        """
        raw_data_dir = self.raw_file_dir
        files = [f for f in listdir(raw_data_dir) if isfile(join(raw_data_dir, f))]

        selected_file = []

        for file_name in files:

            if verbose == True:
                print("===========================================")
                print(file_name)

            campus = file_name.split('_')[1]
            time_created_utc = int(file_name.split('_')[3].split('.')[0])
            time_created_dt = datetime.fromtimestamp(time_created_utc)
            time_end_dt = time_created_dt.replace(hour=0, second=0, minute=0, microsecond=0)
            time_start_dt = time_end_dt - timedelta(days=1)

            if (time_end_dt >= start_date and time_end_dt < end_date):
                selected_file.append((file_name, time_start_dt, time_end_dt, campus))

            if verbose == True:
                print(time_created_dt)
                print(campus, ' | ', time_start_dt, " | ", time_end_dt, " | ", time_start_dt.strftime("%A"))

        # (len(selected_file) == (date_range[1] - date_range[0]).days)

        return selected_file

    # process a raw file
    @timeit
    def process_raw_data(self, raw_file_path, time_start_dt, time_end_dt, campus, ap_building_mapping,
                         ap_campus_mapping, building_name_collection):

        """
        Given a raw file data frame convert it to cleaned dataframe by following steps
        1. check start_time, end_time and duration to fill the missing value
        2. generate building, campus field by ap_name

        @param raw_file_path: file path of the raw file
        @param time_start_dt: start time of raw file
        @param time_end_dt: end time of the day of raw file
        @return:
        """
        df = pd.read_csv(raw_file_path,
                         names=['device_id', 'user_id', 'ap_name', 'start_time', 'end_time', 'duration', 'ssid'])

        # convert data types
        df['device_id'] = df['device_id'].astype('string')
        df['user_id'] = df['user_id'].astype('string')
        # process the user id
        df['user_id'].loc[df['user_id'] == df['device_id']] = 'None'

        df['ap_name'] = df['ap_name'].astype('string')
        df['ssid'] = df['ssid'].astype('string')

        # process time
        df['start_time'] = df['start_time'].apply(lambda row: time_start_dt
        if row == '\t-' else datetime.strptime(row.replace(' PDT', '').replace(' PST', ''), "%m/%d/%Y %I:%M %p"))

        df['end_time'] = df['end_time'].apply(lambda row: time_end_dt
        if row == '\t-' else datetime.strptime(row.replace(' PDT', '').replace(' PST', ''), "%m/%d/%Y %I:%M %p"))

        # generate building and ap information
        df['building'] = df['ap_name'].apply(
            lambda ap_name: self.get_building_name(ap_name, building_name_collection, ap_building_mapping))
        df['campus'] = df['ap_name'].apply(lambda ap_name: ap_campus_mapping.get(ap_name, campus.lower()))

        # drop duration
        df = df.drop(['duration'], axis=1)

        df.to_csv(self.out_dir + campus + time_start_dt.strftime("%Y-%m-%d") + ".csv", index = False)

        return df

    # merge time interval
    @timeit
    def merge_time_interval(self, df, time_start_dt, campus, connect_interval=1, by_user=True):

        result = []
        if by_user:
            grouped = df.groupby(['device_id', 'user_id', 'building'])
        else:
            grouped = df.groupby(['device_id', 'building'])

        for idx, (name, group) in enumerate(grouped):

            # print(idx, name)
            # sort by start time
            group.sort_values(by=['start_time', 'end_time'], inplace=True)  # sort by start time

            if by_user:
                start1, end1, device_id, user_id, building = None, None, name[0], name[1], name[2]
            else:
                start1, end1, device_id, building, user_id = None, None, name[0], name[1], None

            for index, (device_id, user_id, _, start_time, end_time, _, building, _) in enumerate(
                    group.itertuples(index=False, name=None)):

                if index == 0:
                    start1, end1 = start_time, end_time
                else:
                    start2, end2 = start_time, end_time
                    if start2 > end1 + timedelta(minutes=connect_interval):
                        if by_user == True:
                            result.append((device_id, user_id, building, start1, end1))
                        else:
                            result.append((device_id, building, start1, end1))
                        start1, end1 = start2, end2
                    else:
                        if end2 >= end1:
                            end1 = end2

            if by_user:
                result.append((device_id, user_id, building, start1, end1))
            else:
                result.append((device_id, building, start1, end1))

        # print(len(result))

        if by_user:
            df_new = pd.DataFrame(result, columns= ['device_id', 'user_id', 'building', 'start_time', 'end_time'])
        else:
            df_new = pd.DataFrame(result, columns= ['device_id', 'building', 'start_time', 'end_time'])

        df_new.to_csv(self.out_dir + campus + time_start_dt.strftime("%Y-%m-%d") + "_merged.csv", index=False)

        return df_new

    # process ap name to fetch building name
    def get_building_name(self, ap_name, building_name_collection, ap_building_mapping):

        """
        Get building name from ap name
        #param ap_name: the name of the ap
        @param building_name_collection: a collection of building names
        @param ap_building_mapping: ap building mappings
        @return:
        """

        # if in mapping return directly
        if ap_name in ap_building_mapping:
            return ap_building_mapping[ap_name]

        # to lowercase
        ap_name = ap_name.lower()

        # if part of ap_name in building name collection return that
        for ele in ap_name.split('-'):
            ele = ele.strip()
            if ele in building_name_collection:
                return ele

        # if cannot find it process it to get building name
        # remove digital and ap
        pattern1 = r'^[0-9]+$'
        prog1 = re.compile(pattern1)
        pattern2 = r'front|east|west|south|north|outdoor|southeast|northeast|northwest|southwest|patio'
        prog2 = re.compile(pattern2)

        result = []
        for item in ap_name.split('-'):
            if (item != 'ap' and prog1.match(item) == None and prog2.match(item) == None):
                result.append(item)

        # add these to a dictionary to further investigate
        if (not (ap_name in self.invalid_buildings)):
            self.invalid_buildings[ap_name] = '-'.join(result)

        if ':' in '-'.join(result):
            return 'None'
        if ('-'.join(result) == '') or (':' in '-'.join(result)):
            return 'None'
        else:
            return '-'.join(result)

    # generate time interval range list contains continuous days
    def generate_time_range(self, row):
        """
        Given a row of df, generate a tiem interval range list contains continuous days
        @param row: a row of a dataframe
        @return: a list of range
        """
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

    # expand continuous across day time range to every day
    def split_continuous_time(self, df):
        # cross time range
        cross = df[df['start_time'].dt.date != df['end_time'].dt.date].copy()
        non_cross = df[df['start_time'].dt.date == df['end_time'].dt.date]

        cross['time_range'] = cross.apply(lambda row: self.generate_time_range(row), axis=1)

        cross = cross.explode('time_range')
        cross['start_time'] = cross.apply(lambda row: row['time_range'][0], axis=1)
        cross['end_time'] = cross.apply(lambda row: row['time_range'][1], axis=1)

        cross = cross.drop('time_range', axis=1)

        df = pd.concat([non_cross, cross])

        return df


