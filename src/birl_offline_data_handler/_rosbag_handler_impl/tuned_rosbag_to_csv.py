#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import rosbag
import subprocess
from optparse import OptionParser
from datetime import datetime
import os

def message_to_csv(stream, msg, flatten=False):
    """
    stream: StringIO
    msg: message
    """
    try:
        for s in type(msg).__slots__:
            val = msg.__getattribute__(s)
            message_to_csv(stream, val, flatten)
    except:
        msg_str = str(msg)
        if msg_str.find(",") is not -1:
            if flatten:
                msg_str = msg_str.strip("(")
                msg_str = msg_str.strip(")")
                msg_str = msg_str.strip(" ")
            else:
                msg_str = "\"" + msg_str + "\""
        stream.write("," + msg_str)

def message_type_to_csv(stream, msg, parent_content_name=""):
    """
    stream: StringIO
    msg: message
    """
    try:
        for s in type(msg).__slots__:
            val = msg.__getattribute__(s)
            message_type_to_csv(stream, val, ".".join([parent_content_name,s]))
    except:
        stream.write("," + parent_content_name)
 
def bag_to_csv(bag, output_file_path, topic_name):
    streamdict= dict()

    for topic, msg, time in bag.read_messages(topics=topic_name,
                                              start_time=None,
                                              end_time=None):
        if streamdict.has_key(topic):
            stream = streamdict[topic]
        else:
            stream = open(output_file_path, 'w')
            streamdict[topic] = stream
            stream.write("time")
            message_type_to_csv(stream, msg)
            stream.write('\n')

        stream.write(datetime.fromtimestamp(time.to_time()).strftime('%Y/%m/%d/%H:%M:%S.%f'))
        message_to_csv(stream, msg, flatten=False)
        stream.write('\n')
    [s.close for s in streamdict.values()]

