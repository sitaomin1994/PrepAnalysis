from os import listdir
from os.path import isfile, join
from datetime import datetime, timedelta
import pandas as pd
from RawFileProcessor import RawFileProcessor
from Mappings import Mappings
import time as tick

####################################################################################################
# Main program
####################################################################################################

if __name__ == '__main__':

    # raw data
    raw_data_dir = "./data/raw/"
    footprint_dir = "./data/footprint/"
    output_dir = './data/data/'

    # raw file processor
    rawFileProcessor = RawFileProcessor(raw_data_dir, output_dir)
    mappings = Mappings(footprint_dir)

    # get file_list
    file_list = rawFileProcessor.get_all_file_names(verbose=False)
    print("Total number of file: {}".format(len(file_list)))

    # process raw file
    ap_building_mapping = mappings.generate_ap_building_mapping()
    ap_campus_mapping = mappings.generate_ap_campus_mapping()
    building_list = [item for _, buildings in mappings.generate_building_name_collection().items() for item in
                     buildings]

    # print("Number of buildings: {}".format(len(building_list)))

    for idx, file in enumerate(file_list):
        raw_file_path, time_start_dt, time_end_dt, campus = tuple(file)
        if idx > 75:
            print("====================================================================================================")
            print('Processing file {} - {} Date: {}'.format(idx, raw_file_path, time_start_dt.strftime("%Y-%m-%d")))

            df = rawFileProcessor.process_raw_data(raw_data_dir + raw_file_path, time_start_dt, time_end_dt, campus,
                                               ap_building_mapping, ap_campus_mapping, building_list)
            rawFileProcessor.merge_time_interval(df, time_start_dt, campus, by_user=False)

    #
    #     #remove device whos duration large than 20 hours in a day
    #     df_new.to_csv('./data/device_agg.csv')
    #
    #
    # #clean_raw_data(df)
    #
    # df_d_agg = pd.read_csv('./data/device_agg.csv', index_col=0)
    #
    # df_d_agg['device_id'] = df_d_agg['device_id'].astype('string')
    # df_d_agg['building_name'] = df_d_agg['building_name'].astype('string')
    # df_d_agg['user_id_major'] = df_d_agg['user_id_major'].astype('string')
    # df_d_agg['start_time'] = df_d_agg['start_time'].astype('datetime64')
    # df_d_agg['end_time'] = df_d_agg['end_time'].astype('datetime64')
    # df_d_agg['duration'] = df_d_agg['end_time'] - df_d_agg['start_time']
    #
    # print(df_d_agg.shape, df_d_agg.columns, df_d_agg.dtypes)
    #
    # g = df_d_agg.groupby(['device_id', 'building_name'], as_index=False)
    #
    # print(g.ngroups)
    #
    # df_device = g.agg(duration = ('duration', 'sum'),
    #       start_time = ('start_time', 'min'),
    #       end_time = ('end_time', 'max'),
    #       n_records = ('n_records', 'sum'))
    #
    # df_device.to_csv('./data/temp_dev_bld.csv', index = False)
