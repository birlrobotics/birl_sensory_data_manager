# -*- coding: utf-8 -*-
"""This is a module that extracts anomaly data from CSV 

This module holds two assumptions, one for CSV and one for anomaly extraction.
For CSV, this module assumes there are one data CSV that contains data and one
flag CSV that contains anomaly flags. Both CSVs contain a \"time\" column. For
anomaly extraction, this module assumes values of \"time\" column in the flag
CSV indicate anomalous moments. Therefore, to extract anomalies, we need to
collect subsets of the data CSV based on \"time\" in the flag CSV. 

"""
from datetime import datetime

class CsvHandler(object):
    """To extract anomalies from CSV.
    
    An instance of CsvHandler helps you extract anomalies from CSV. One data CSV
    and one flag CSV are needed for anomaly extraction. Both CSVs should contain a
    \"time\" column. The data CSV contains data while the flag CSV contains anomaly
    flags. The extracted anomalies are effectively subsets of the data CSV collected
    according to the flag CSV. 

    Examples:
        >>> ch = csv_handler.CsvHandler()
        >>> ch.extract_anomaly_data(data_df, anomaly_flag_df, 4, 2)
        [pandas.DataFrame, ..., pandas.DataFrame]

    """

    def __init__(self):
        pass

    def extract_anomaly_data(
        self,
        data_df,
        anomaly_flag_df,
        anomaly_window_size_in_sec,
        anomaly_resample_hz,
    ):
        """Get anomaly data as CSV.
        
        The length of an anomaly, e.g. the windows size of an anomaly, should be
        specified.  Since the amount of timesteps of an anomaly may vary, we will
        resample each anomaly so that they're of the same size. Thus the rate of
        resampling should be specified too.

        Args:
            data_df (pandas.Dataframe): The data CSV.
            anomaly_flag_df (pandas.Dataframe): The flag CSV.
            anomaly_window_size_in_sec: Time length of an anomaly.
            anomaly_resample_hz: Rate of resampling for anomaly data.

        Returns:
            A list of pandas.Dataframe. Here a pandas.Dataframe represents a CSV 
            of anomaly data.
        """


        import numpy as np

        # process time
        from dateutil import parser
        data_df['time'] = data_df['time']\
            .apply(lambda x: parser.parse(x))
        anomaly_flag_df['time'] = anomaly_flag_df['time']\
            .apply(lambda x: parser.parse(x))

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
            new_time_index = np.linspace(
                anomaly_t-anomaly_window_size_in_sec/2, 
                anomaly_t+anomaly_window_size_in_sec/2, 
                anomaly_window_size_in_sec*anomaly_resample_hz
            )
            old_time_index = search_df.index
            resampled_anomaly_df = search_df\
                .reindex(old_time_index.union(new_time_index))\
                .interpolate(method='linear', axis=0).ix[new_time_index]
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
