import glob, os
from _rosbag_handler_impl.tuned_rosbag_to_csv import bag_to_csv

class InvalidRosbagPath(Exception): pass

class RosbagHandler(object):
    def __init__(self, path_to_rosbag):
        if os.path.isdir(path_to_rosbag):
            list_of_bag = glob.glob(
                os.path.join(
                    path_to_rosbag,
                    "*.bag",
                )
            )
        elif os.path.isfile(path_to_rosbag):
            list_of_bag = [path_to_rosbag]
        else:
            raise InvalidRosbagPath()
        self.path_to_rosbag = path_to_rosbag
        self.list_of_bag = list_of_bag
        pass

    def convert_to_csv(self):
        """Convert ROS bag file(s) to csv file(s).

        If the ROS bag contains more than one ROS topics, 
        for each topic, one csv will be output. The 
        output csv(s) will be stored in a folder of the 
        same name and in the same folder as the original 
        ROS bag. 

        Args:
            path_to_rosbag: A path to a ROS bag file or 
                a folder of ROS bag files. In the latter 
                case, all ROS bag files in that folder
                will be processed. 

        Returns:
            None

        Raises:
            InvalidRosbagPath

        """

        list_of_bag = self.list_of_bag 

        for bag in list_of_bag:
            fname = os.path.basename(bag)[:-4]     # strip .bag
            bag_dir = os.path.join(
                os.path.dirname(bag),
                fname,
            )
            if os.path.isdir(bag_dir) is False:
                os.makedirs(bag_dir)
            output_file_format = os.path.join(bag_dir, "%t.csv")

            bag_to_csv(
                str(output_file_format),
                os.path.abspath(bag)
            )
        pass

    def load_csv(self):
        pass


