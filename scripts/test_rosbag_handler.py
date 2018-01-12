#!/usr/bin/env python

import glob, os, shutil
from shutil import rmtree
import tempfile
import filecmp
import logging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def setup_test_dir():
    test_dir = os.path.join(
        tempfile.gettempdir(), 
        'test_rosbag_handler_test_dir_{}'.format(hash(os.times()))
    )
    shutil.copytree(
        os.path.join(
            '..',
            'test_data_set',
            'raw_data_for_test',
        ),
        test_dir
    ) 
    return test_dir

def _same_dir_test(dcmp):
    if len(dcmp.left_only) != 0\
        or len(dcmp.right_only) != 0\
        or len(dcmp.diff_files) != 0:
        return False
    
    for sub_dcmp in dcmp.subdirs.values():
        if _same_dir_test(sub_dcmp) == False:
            return False

    return True 

def same_dir_test(dir1, dir2):
    dcmp = filecmp.dircmp(dir1, dir2)
    return _same_dir_test(dcmp)
    

if __name__ == '__main__':
    import birl_offline_data_handler.rosbag_handler.interface as ri
    score_dir = os.path.join(
        '..',
        'test_data_set',
        'good_processed_data',
    )

    logger.info("Test invalid rosbag input.")
    try:
        ri.convert_rosbag_to_csv("thispathdoesnotexist")
    except ri.InvalidRosbagPath as e:
        logger.info("passed.")
    else:
        logger.error("failed.")
    

    logger.info("Test converting rosbag folder.")
    test_dir = setup_test_dir()
    clean_test_flag = True
    ri.convert_rosbag_to_csv(test_dir)
    try:
        assert same_dir_test(test_dir, score_dir)
    except AssertionError as e:
        logger.error('failed. test_folder(\"%s\") and score_folder(\"%s\") are different.'%(test_dir, score_dir))
        clean_test_flag = False
    else:
        logger.info("passed.")
    if clean_test_flag:
        rmtree(test_dir, ignore_errors=True)
        
        

    test_dir = setup_test_dir()
    clean_test_flag = True
    list_of_bag_file = glob.glob(
        os.path.join(
            test_dir,
            "*.bag",
        )
    )
    for bag_file in list_of_bag_file:
        ri.convert_rosbag_to_csv(bag_file) 
        fname = os.path.basename(bag_file)[:-4]     # strip .bag

        test_bag_dir = os.path.join(test_dir, fname)
        score_bag_dir = os.path.join(score_dir, fname)

        logger.info("Test converting single rosbag.")
        try:
            assert same_dir_test(test_bag_dir, score_bag_dir)
        except AssertionError as e:
            logger.error('failed. test_bag_folder(\"%s\") and score_bag_folder(\"%s\") are different.'%(test_bag_dir, score_bag_dir))
            clean_test_flag = False
        except OSError as e:
            logger.error('failed. %s'%(e,))
            clean_test_flag = False
        else:
            logger.info("passed.")
    if clean_test_flag:
        rmtree(test_dir, ignore_errors=True)
