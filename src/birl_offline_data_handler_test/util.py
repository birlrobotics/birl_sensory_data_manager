from shutil import rmtree
import os, shutil
import tempfile
import filecmp
import logging 
logger = logging.getLogger("test_util.dir")

dir_of_this_script = os.path.dirname(os.path.realpath(__file__))

def create_a_test_dir():
    test_dir = os.path.join(
        tempfile.gettempdir(), 
        'test_rosbag_handler_test_dir_{}'.format(hash(os.times()))
    )
    shutil.copytree(
        os.path.join(
            dir_of_this_script,
            'test_data_set',
            'raw_data_for_test',
        ),
        test_dir
    ) 
    return test_dir

def get_score_dir(score_type):        
    return \
    os.path.join(
        dir_of_this_script,
        'test_data_set',
        'score_data',
        score_type,
    )

def delete_test_dir(test_dir):
    rmtree(test_dir, ignore_errors=True)
    

def _same_dir_test(dcmp):
    ret = True
    if len(dcmp.left_only) != 0\
        or len(dcmp.right_only) != 0\
        or len(dcmp.diff_files) != 0:

        if len(dcmp.left_only) != 0:
            logger.error("dcmp.left_only: %s"%dcmp.left_only)
        if len(dcmp.right_only) != 0:
            logger.error("dcmp.right_only: %s"%dcmp.right_only)
        if len(dcmp.diff_files) != 0:
            logger.error("dcmp.diff_files: %s"%dcmp.diff_files)
        ret = False
    
    for sub_dcmp in dcmp.subdirs.values():
        if _same_dir_test(sub_dcmp) == False:
            ret = False

    return ret

def same_dir_test(dir1, dir2):
    dcmp = filecmp.dircmp(dir1, dir2)
    return _same_dir_test(dcmp)
