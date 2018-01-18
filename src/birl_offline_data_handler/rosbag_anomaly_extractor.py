# -*- coding: utf-8 -*-
"""B

D

"""


from rosbag_handler import RosbagHandler
import os
import glob

class RosbagAnomalyExtractor(RosbagHandler):
    """B
    
    D

    Args:
        path_to_rosbag: *.
        use_cached_result: *.
        
    Raises:
        *

    Examples:
        *
    """

    def __init__(self, path_to_rosbag, use_cached_result=True):
        super(RosbagAnomalyExtractor, self)\
            .__init__(path_to_rosbag, use_cached_result)

    def get_anomaly_csv(
        self,
        data_topic_name,
        anomaly_topic_name,
        anomaly_window_size_in_sec,
        anomaly_resample_hz,
    ):
        """B
        
        D

        Args:
            data_topic_name: *.
            anomaly_topic_name: *.
            anomaly_window_size_in_sec: *.
            anomaly_resample_hz: *.

        Returns:
            *

        Examples:
            *
        """
        ret = []

        _list_of_bag_paths = self._list_of_bag_paths 

        for bag_path in _list_of_bag_paths:
            ret.append((
                bag_path, 
                self._get_anomaly_csv_of_one_bag(
                    bag_path,
                    data_topic_name,
                    anomaly_topic_name,
                    anomaly_window_size_in_sec,
                    anomaly_resample_hz,
                ),
            ))

    def _get_anomaly_csv_of_one_bag(
        self, 
        bag_path,
        data_topic_name,
        anomaly_topic_name,
        anomaly_window_size_in_sec,
        anomaly_resample_hz,
    ):
        import pandas as pd

        anomaly_csv_dir_path = self._get_anomaly_csv_dir_path(bag_path)
        cache_flag_path = os.path.join(anomaly_csv_dir_path, "SUCCESS")
        if self._use_cache and os.path.isfile(cache_flag_path):
            # Approved to use cache and cached csv 
            # is found.
            pass
        else:
            # Generate a csv for this topic and stored
            # it at csv_path.
            try:
                os.makedirs(anomaly_csv_dir_path)
            except OSError as exc: # Guard against race condition
                import errno
                if exc.errno != errno.EEXIST:
                    raise 
            data_df = super(RosbagAnomalyExtractor, self)\
                ._get_csv_of_a_topic_of_one_bag(
                bag_path,
                data_topic_name,
            )

            anomaly_flag_df = super(RosbagAnomalyExtractor, self)\
                ._get_csv_of_a_topic_of_one_bag(
                bag_path,
                anomaly_topic_name,
            )

            from birl_generic_data_handler import csv_handler
            ch = csv_handler.CsvHandler()
            list_of_anomaly_df = ch.extract_anomaly_data(
                data_df, 
                anomaly_flag_df,
                anomaly_window_size_in_sec,
                anomaly_resample_hz,
            )

            fname = os.path.basename(bag_path)[:-4]
            for anomaly_idx, df in \
                enumerate(list_of_anomaly_df):
                df.to_csv(os.path.join(
                    anomaly_csv_dir_path,
                    'resampled_%shz_no_%s_from_trial_%s.csv'\
                        %(anomaly_resample_hz, anomaly_idx, fname)
                ))
            tmp = open(cache_flag_path, "w")
            tmp.close() 

        list_of_anomaly_csv_paths = glob.glob(os.path.join(
            anomaly_csv_dir_path,
            "*.csv"
        ))
        ret = []
        for csv_path in list_of_anomaly_csv_paths:
            anomaly_id = os.path.basename(csv_path)[:-4]
            ret.append((
                anomaly_id,
                pd.read_csv(csv_path, sep=','),
            ))
        return ret

    def _get_anomaly_csv_dir_path(self, bag_path):
        # Strip .bag extention
        fname = os.path.basename(bag_path)[:-4]

        return os.path.join(
            os.path.dirname(bag_path),
            fname,
            "extracted_anomalies",
        )

