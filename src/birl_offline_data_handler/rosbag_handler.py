# -*- coding: utf-8 -*-
"""This is a module that handles rosbag.

This module assumes its users are only 
interested in extracting data from rosbag files 
one topic at a time. The core API that provides
this service is class RosbagHandler. Read the 
examples in its documentation for more details.

"""

import os

class InvalidRosbagPath(Exception): pass
class TopicNotFoundInRosbag(Exception): pass

class RosbagHandler(object):
    """To read data in rosbag as CSV.

    An instance of RosbagHandler helps you read data in one or
    multiple rosbag files. You can query content in rosbag by
    topic and the result is stored as pandas Dataframe which
    represents a CSV. To speed up repetitive processing, we will
    first store results alongside the corresponding rosbag files
    and reuse them in future query. Therefore you must have
    write permission on the folder where the rosbag files
    reside. 

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

        >>> o = RosbagHandler("/path_to_data_set/s01.bag")
        >>> o.get_csv_of_a_topic("/tag_multimodal")
        [("/path_to_data_set/s01.bag", pandas.DataFrame)]

        To process a folder of rosbag files

        >>> o = RosbagHandler("/path_to_data_set")
        >>> o.get_csv_of_a_topic("/tag_multimodal")
        [
            ("/path_to_data_set/s01.bag", pandas.DataFrame),
            ("/path_to_data_set/s02.bag", pandas.DataFrame),
            ...
            ("/path_to_data_set/s05.bag", pandas.DataFrame),
        ]

    """

    def __init__(self, path_to_rosbag, use_cached_result=True):
        import glob

        if os.path.isdir(path_to_rosbag):
            _list_of_bag_paths = glob.glob(
                os.path.join(
                    path_to_rosbag,
                    "*.bag",
                )
            )
        elif os.path.isfile(path_to_rosbag):
            _list_of_bag_paths = [path_to_rosbag]
        else:
            raise InvalidRosbagPath()
        self.path_to_rosbag = path_to_rosbag
        self._list_of_bag_paths = _list_of_bag_paths
        self._use_cache = use_cached_result

    def _get_csv_path(self, bag_path, topic_name):

        # Strip .bag extention
        fname = os.path.basename(bag_path)[:-4]

        return os.path.join(
            os.path.dirname(bag_path),
            fname,
            fname+topic_name.replace('/','-')+'.csv'
        )

    def get_csv_of_a_topic(
        self, 
        topic_name, 
    ):
        """Get data of a topic as CSV.

        Args:
            topic_name (str): The name of the to-be-extracted 
                topic. Don't forget the \"/\" if there is one.
 
        Returns:
            A list of (bag path, pandas.Dataframe) tuples,
            pandas.Dataframe represents a CSV.

        Raises:
            TopicNotFoundInRosbag
        """
        ret = []
        
        _list_of_bag_paths = self._list_of_bag_paths 

        for bag_path in _list_of_bag_paths:
            ret.append((
                bag_path, 
                self._get_csv_of_a_topic_of_one_bag(
                    bag_path,
                    topic_name,
                ),
            ))

        return ret


    def _get_csv_of_a_topic_of_one_bag(self, bag_path, topic_name):
        import rosbag
        import pandas as pd
        from _rosbag_handler_impl.tuned_rosbag_to_csv import bag_to_csv

        bag = rosbag.Bag(bag_path)
        available_topics = \
            bag.get_type_and_topic_info().topics.keys()
        if topic_name not in available_topics:
            raise TopicNotFoundInRosbag("topic name: %s"%topic_name)

        csv_path = self._get_csv_path(
            bag_path,
            topic_name,
        ) 

        if self._use_cache and os.path.isfile(csv_path):
            # Approved to use cache and cached csv 
            # is found.
            pass
        else:
            # Generate a csv for this topic and stored
            # it at csv_path.
            try:
                os.makedirs(os.path.dirname(csv_path))
            except OSError as exc: # Guard against race condition
                import errno
                if exc.errno != errno.EEXIST:
                    raise 
            bag_to_csv(bag, csv_path, topic_name)

        # Read the csv into pandas Dataframe and return it
        return pd.read_csv(csv_path, sep=',')
