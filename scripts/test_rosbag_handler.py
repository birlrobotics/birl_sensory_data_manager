#!/usr/bin/env python
from birl_offline_data_handler.rosbag_handler import (
    RosbagHandler, 
    InvalidRosbagPath, 
    TopicNotFoundInRosbag,
)
from birl_offline_data_handler_test.util import (
    create_a_test_dir,
    get_score_dir,
    delete_test_dir,
    same_dir_test,
)    
import traceback
import pandas
import glob, os
import logging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

if __name__ == '__main__':

    score_dir = get_score_dir("good_rosbag_to_csv")

    logger.info("Test invalid rosbag input.")
    try:
        o = RosbagHandler("thispathdoesnotexist")
    except InvalidRosbagPath as e:
        logger.info("passed.")
    else:
        logger.error("failed.")
    

    test_dir = create_a_test_dir()
    clean_test_flag = True
    o = RosbagHandler(test_dir)

    logger.info("Test invalid topic input.")
    try:
        o.get_csv_of_a_topic("thistopicdoesnotexist")
    except TopicNotFoundInRosbag as e:
        logger.info("passed.")
    else:
        logger.error("failed.")

    ret = o.get_csv_of_a_topic("/tag_multimodal")
    ret = o.get_csv_of_a_topic("/anomaly_detection_signal")
    try:
        logger.info("Test converting rosbag folder.")
        assert same_dir_test(test_dir, score_dir)
    except AssertionError as e:
        logger.error('failed. test_folder(\"%s\") and score_folder(\"%s\") are different.'%(test_dir, score_dir))
    else:
        logger.info("passed.")

    try:
        logger.info("Test return value types.")
        assert len(ret)==2
        assert type(ret[0][0])==str
        assert type(ret[0][1])==pandas.core.frame.DataFrame
    except AssertionError as e:
        logger.error('failed.')
        clean_test_flag = False
    else:
        logger.info("passed.")

    if clean_test_flag:
        delete_test_dir(test_dir)
        
        

    test_dir = create_a_test_dir()
    clean_test_flag = True
    list_of_bag_file = glob.glob(
        os.path.join(
            test_dir,
            "*.bag",
        )
    )
    for bag_file in list_of_bag_file:
        o = RosbagHandler(bag_file)
        ret = o.get_csv_of_a_topic("/tag_multimodal")
        ret = o.get_csv_of_a_topic("/anomaly_detection_signal")
        fname = os.path.basename(bag_file)[:-4]     # strip .bag

        test_bag_dir = os.path.join(test_dir, fname)
        score_bag_dir = os.path.join(score_dir, fname)

        logger.info("Test converting single rosbag.")
        try:
            assert same_dir_test(test_bag_dir, score_bag_dir)
        except AssertionError as e:
            logger.error('failed. test_folder(\"%s\") and score_folder(\"%s\") are different.'%(test_dir, score_dir))
            clean_test_flag = False
        else:
            logger.info("passed.")

        try:
            logger.info("Test return value types.")
            assert len(ret)==1
            assert ret[0][0]==bag_file
            assert type(ret[0][1])==pandas.core.frame.DataFrame
        except AssertionError as e:
            traceback.print_exc()
            logger.error('failed.')
            clean_test_flag = False
        else:
            logger.info("passed.")

    if clean_test_flag:
        delete_test_dir(test_dir)
