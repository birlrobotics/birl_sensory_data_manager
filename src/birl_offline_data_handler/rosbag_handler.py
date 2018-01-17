import os

class InvalidRosbagPath(Exception): pass
class TopicNotFoundInRosbag(Exception): pass

class RosbagHandler(object):
    """Initialize a RosbagHandler.

    An instance of RosbagHandler provides access
    to one or multiple rosbag files. You can query
    content in rosbag by topic and the result is
    stored as pandas Dataframe which represents a 
    CSV. To speed up repetitive processing, 
    results will be automatically cached along 
    side the corresponding rosbag files. 

    Args:
        path_to_rosbag: A path to a rosbag file or 
            a folder of rosbag files. In the latter 
            case, all rosbag files in that folder
            will be processed. 

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

    def __init__(self, path_to_rosbag):
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
        use_cached_result=True,
    ):
        """Brief get_csv_of_a_topic.
        
        Detailed get_csv_of_a_topic.

        Args:
            topic_name (str): The name of the to-be-extracted 
                topic. Don't forget the \"/\" if there is one.
 
            use_cached_result (bool, optional): Default true. 
                If ture, cached result will be used instead 
                of reading and parsing the rosbag file again.

        Returns:
            A list of (bag path, csv dataframe) tuples.

        Raises:
            TopicNotFoundInRosbag
        """

        import rosbag
        import pandas as pd
        from _rosbag_handler_impl.tuned_rosbag_to_csv import bag_to_csv

        ret = []
        
        _list_of_bag_paths = self._list_of_bag_paths 

        for bag_path in _list_of_bag_paths:
            bag = rosbag.Bag(bag_path)
            available_topics = \
                bag.get_type_and_topic_info().topics.keys()
            if topic_name not in available_topics:
                raise TopicNotFoundInRosbag()

            csv_path = self._get_csv_path(
                bag_path,
                topic_name,
            ) 

            if use_cached_result and os.path.isfile(csv_path):
                # Approved to use cache and cached csv 
                # is found.
                pass
            else:
                # Generate a csv for this topic and stored
                # it at csv_path.
                try:
                    os.makedirs(os.path.dirname(csv_path))
                except OSError as exc: # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise 
                bag_to_csv(bag, csv_path, topic_name)


            # Read the csv into pandas Dataframe and 
            # return it
            ret.append((bag_path, pd.read_csv(csv_path, sep=',')))

        return ret
