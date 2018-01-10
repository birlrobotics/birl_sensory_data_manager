import core
import glob, os


class InvalidRosbagPath(Exception): pass


def convert_rosbag_to_csv(path_to_rosbag):
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
    
