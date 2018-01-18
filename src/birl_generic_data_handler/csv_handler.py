# -*- coding: utf-8 -*-
from datetime import datetime

class CsvHandler(object):
    def __init__(self):
        pass

    def extract_anomaly_data(
        self,
        data_df,
        anomaly_flag_df,
        anomaly_window_size_in_sec,
        anomaly_resample_hz,
    ):
        import numpy as np

        # process time
        from dateutil import parser
        data_df['time'] = data_df['time'].apply(lambda x: parser.parse(x))
        anomaly_flag_df['time'] = anomaly_flag_df['time'].apply(lambda x: parser.parse(x))

        trial_start_datetime = data_df['time'][0]
        data_df['time'] -= trial_start_datetime
        data_df['time'] = data_df['time']\
            .apply(lambda x: x/np.timedelta64(1, 's'))
        anomaly_flag_df['time'] -= trial_start_datetime
        anomaly_flag_df['time'] = anomaly_flag_df['time']\
            .apply(lambda x: x/np.timedelta64(1, 's'))

        list_of_anomaly_start_time = \
            self._get_anomaly_range(anomaly_flag_df)

        list_of_resampled_anomaly_df = []
        for anomaly_idx, anomaly_t in \
            enumerate(list_of_anomaly_start_time):

            # keep 1 more sec each side for interpolation
            search_start = anomaly_t-anomaly_window_size_in_sec/2-1
            search_end = anomaly_t+anomaly_window_size_in_sec/2+1
            search_df = data_df[\
                (data_df['time'] >= search_start) &\
                (data_df['time'] <= search_end)\
            ]
            search_df = search_df.set_index('time')
            new_time_index = np.linspace(anomaly_t-anomaly_window_size_in_sec/2, anomaly_t+anomaly_window_size_in_sec/2, anomaly_window_size_in_sec*anomaly_resample_hz)
            old_time_index = search_df.index
            resampled_anomaly_df = search_df.reindex(old_time_index.union(new_time_index)).interpolate(method='linear', axis=0).ix[new_time_index]
            list_of_resampled_anomaly_df.append(resampled_anomaly_df)

        return list_of_resampled_anomaly_df

    def _get_anomaly_range(self, flag_df):
        list_of_anomaly_start_time = [flag_df['time'][0]]
        
        flag_df_length = flag_df.shape[0]
        for idx in range(1, flag_df_length):
            now_time = flag_df['time'][idx]
            last_time = flag_df['time'][idx-1]
            if now_time-last_time > 2:
                list_of_anomaly_start_time.append(now_time) 

        return list_of_anomaly_start_time
