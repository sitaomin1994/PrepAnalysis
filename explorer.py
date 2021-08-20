
import pandas as pd
import matplotlib.pyplot as plt

def convert_types_new(df):
    df['user_id'] = df['user_id'].astype('string')
    df['device_id'] = df['device_id'].astype('string')
    df['building_name'] = df['building_name'].astype('string')
    df['start_time'] = df['start_time'].astype('datetime64')
    df['end_time'] = df['end_time'].astype('datetime64')
    df['duration'] = df['end_time'] - df['start_time']
    print(df.dtypes)
    return df

def convert_types_raw(df):
    df['user_id'] = df['user_id'].astype('string')
    df['device_id'] = df['device_id'].astype('string')
    df['building'] = df['building'].astype('string')
    df['ap_name'] = df['ap_name'].astype('string')
    df['ssid'] = df['ssid'].astype('string')
    df['start_time'] = df['start_time'].astype('datetime64')
    df['end_time'] = df['end_time'].astype('datetime64')
    df['duration'] = df['end_time'] - df['start_time']
    print(df.dtypes)
    return df

df = pd.read_csv('./data/new_raw_data.csv')
df = convert_types_raw(df)

def fetch_one_device_user(df):

    # get device with at least one user id
    df_has_user = df[df['user_id'] != df['device_id']]

    # group by user id to get num of unique devices of each user
    df_g = df_has_user.groupby('user_id').agg(unique_device=('device_id', lambda x: x.nunique()))

    # get list of user has one device
    user_id_one_device = list(set(df_g[df_g['unique_device'] == 1].index))

    # using merge to get 1-1 mapping of user and device
    df_mapping = pd.Series(data=user_id_one_device, name='user_id')
    df_mapping = pd.merge(df_mapping, df[['user_id','device_id']], on='user_id', how = 'left')
    df_mapping.drop_duplicates(ignore_index=True, inplace=True)

    print(df_mapping.shape)
    # merge on device to label all device without user_id
    df_result = pd.merge(df, df_mapping, on = 'device_id', suffixes= ['_x', ""])
    return  df_result.drop('user_id_x', axis = 1)

df_fetched = fetch_one_device_user(df)

print(df_fetched.shape)
print(df.shape)
print(df_fetched.columns)
print(len(df_fetched['user_id'].unique()))

#df_fetched.to_csv('./data/raw_data_1device.csv', index = False)

df_g = df_fetched.groupby('user_id', as_index=False).agg(unique_ap = ('ap_name', lambda x:x.nunique()),
                                 unique_bld = ('building', lambda x:x.nunique()),
                                 n_records = ('user_id', 'count'),
                                 duration = ('duration', 'sum'),
                                 start_time = ('start_time', 'min'),
                                 end_time = ('end_time', 'max'))

df_g['connect_percent'] = df_g['duration']/(df_g['end_time'] - df_g['start_time'])

print(df_g.head(), df_g.shape)

#df_g.to_csv('./data/aggre_raw_data_1device.csv')
