#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import rosbag
import rospy
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

def format_csv_filename(form, topic_name):
    global seq
    if form==None:
        return "Convertedbag.csv"
    ret = form.replace('%t', topic_name)
    return ret
 
def bag_to_csv(output_file_format, fname):
    bag = rosbag.Bag(fname)
    streamdict= dict()
    stime = None
    etime = None

    for topic, msg, time in bag.read_messages(topics=None,
                                              start_time=stime,
                                              end_time=etime):
        if streamdict.has_key(topic):
            stream = streamdict[topic]
        else:
            stream = open(
                format_csv_filename(
                    output_file_format, 
                    os.path.basename(fname)[:-4]+topic.replace('/','-')
                ),
                'w')
            streamdict[topic] = stream
            stream.write("time")
            message_type_to_csv(stream, msg)
            stream.write('\n')

        stream.write(datetime.fromtimestamp(time.to_time()).strftime('%Y/%m/%d/%H:%M:%S.%f'))
        message_to_csv(stream, msg, flatten=False)
        stream.write('\n')
    [s.close for s in streamdict.values()]

