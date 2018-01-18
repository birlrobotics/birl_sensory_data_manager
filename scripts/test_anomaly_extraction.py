from birl_offline_data_handler.rosbag_anomaly_extractor import (
    RosbagAnomalyExtractor
)
from birl_offline_data_handler_test.util import (
    create_a_test_dir,
    get_score_dir,
    delete_test_dir,
    same_dir_test,
)    

import logging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

if __name__ == '__main__':
    score_dir = get_score_dir("good_anomaly_extraction")

    logger.info("Test anomaly extract for a folder of rosbags")
    test_dir = create_a_test_dir()
    clean_test_flag = True
    rae = RosbagAnomalyExtractor(test_dir)
    rae.get_anomaly_csv(
        "/tag_multimodal",
        "/anomaly_detection_signal",
        4,
        10,
    )
    try:
        assert same_dir_test(test_dir, score_dir)
    except AssertionError as e:
        logger.error('failed. test_folder(\"%s\") and score_folder(\"%s\") are different.'%(test_dir, score_dir))
        clean_test_flag = False
    else:
        logger.info("passed.")
    if clean_test_flag:
        delete_test_dir(test_dir)
