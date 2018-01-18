# -*- coding: utf-8 -*-
"""This is a module that extracts anomaly data from rosbag

This module holds two assumptions, one for rosbag files and one for anomaly
extraction. For rosbag files, this module assumes there are one data topic and
one flag topic in every rosbag file. And the message types of them both contain
a std_msgs/Header field, which means every message sent to these two topics is
timestamped. For anomaly extraction, this module assumes a message sent to the
flag topic marks its sending moment anomalous, which means the timestamp of that
message indicates an anomalous moment.  Therefore, to extract anomalies, we need
to collect subsets of data topic messages based on the timestamps of flag topic
messages.

"""


from rosbag_handler import RosbagHandler
import os
import glob

class RosbagAnomalyExtractor(RosbagHandler):
    """To extract anomalies from rosbags.
    
    An instance of RosbagAnomalyExtractor helps you extract anomalies from one
    or more rosbag files. The rosbag files in question should contain a data topic
    and a flag topic. Messages of both topics should be timestamped. The messages of
    data topic contain data while the messages of flag topic indicate anomalous
    moments. The extracted anomalies are effectively subsets of the data topic
    collected according to the flag topic.

    Args:
        path_to_rosbag: A path to a rosbag file or 
            a folder of rosbag files. In the latter 
            case, all rosbag files in that folder
            will be processed. 
        use_cached_result (bool, optional): Default true. 
            If ture, cached result will be used instead 
            of reading and parsing the rosbag file again.
        
    Raises:
        InvalidRosbagPath

    Examples:
        To process a single rosbag file

        >>> o = RosbagAnomalyExtractor("/path_to_data_set/s01.bag")
        >>> o.get_anomaly_csv("/tag_multimodal", "/anomaly_detection_signal", 4, 10)
        [
            ("/path_to_data_set/s01.bag", [
                (anomaly_id, pandas.DataFrame),
                ...
                (anomaly_id, pandas.DataFrame),
            ])
        ]

        To process a folder of rosbag files

        >>> o = RosbagAnomalyExtractor("/path_to_data_set")
        >>> o.get_anomaly_csv("/tag_multimodal", "/anomaly_detection_signal", 4, 10)
        [
            ("/path_to_data_set/s01.bag", [
                (anomaly_id, pandas.DataFrame),
                ...
                (anomaly_id, pandas.DataFrame),
            ]),
            ...
            ("/path_to_data_set/s05.bag", [
                (anomaly_id, pandas.DataFrame),
                ...
                (anomaly_id, pandas.DataFrame),
            ])
        ]

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
        """Get anomaly data as CSV.
        
        In order to extract messages from data topic and flag topic,
        the user must provide their topic names. Besides, the length of an
        anomaly, e.g. the windows size of an anomaly, should be specified.
        Since the amount of timesteps of an anomaly may vary, we will resample
        each anomaly so that they're of the same size. Thus the rate of
        resampling should be specified too.

        Args:
            data_topic_name (str): The name of data topic, i.e. "/tag_multimodal".
            anomaly_topic_name: The name of flag topic, i.e. "anomaly_detection_signal".
            anomaly_window_size_in_sec: Time length of an anomaly.
            anomaly_resample_hz: Rate of resampling for anomaly data.

        Returns:
            A list of (bag path, x) tuples, where x is a list of 
            (anomaly id, pandas.Dataframe) tuples. pandas.Dataframe represents a CSV.
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

        return ret

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

