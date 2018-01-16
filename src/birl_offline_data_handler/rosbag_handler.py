import glob, os
import rosbag
import pandas as pd
from _rosbag_handler_impl.tuned_rosbag_to_csv import bag_to_csv

class InvalidRosbagPath(Exception): pass
class TopicNotFoundInRosbag(Exception): pass

class RosbagHandler(object):
    """Brief RosbagHandler

    Detailed RosbagHandler

    Attributes:
    """


    def __init__(self, path_to_rosbag):
        """Brief init.

        Detailed init.

        Args:
            path_to_rosbag: A path to a rosbag file or 
                a folder of rosbag files. In the latter 
                case, all rosbag files in that folder
                will be processed. 

        Raises:
            InvalidRosbagPath
        """

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
            topic_name: topic_name. 
            use_cached_result: use_cached_result.

        Retunrs:
            A list.

        Raises:
            TopicNotFoundInRosbag
        """
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
            ret.append(pd.read_csv(csv_path, sep=','))

        return ret
