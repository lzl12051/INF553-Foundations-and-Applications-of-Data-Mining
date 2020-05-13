# This is a lib of inf553 assignments
import json
# import logging
import os
import random
import time
# from math import sqrt
# from random import sample
# import random
from collections import Counter

# import matplotlib.pyplot as plt
from typing import Optional, Any


def timer(f):
    def wrapper(*args, **kwargs):
        startTime = time.time()
        # print(*args)
        result = f(*args, **kwargs)
        endTime = time.time()
        runingTime = endTime - startTime
        if runingTime * 1000 < 1000:
            print(f, "Duration:%.3fms" % (runingTime * 1000))
        else:
            print(f, "Duration:%.3fs" % runingTime)
        return result

    return wrapper


@timer
def START_SPARK(name='Assignment', master='local[*]'):
    """
    
    :param name: Name of Task, this will show on the UI of spark
    :param master: Set the number of cores spark should use, leave blank will use all available cores 
    :return: Spark context
    """
    from pyspark import SparkContext
    from pyspark import SparkConf
    confSpark = SparkConf().setAppName(name).setMaster(
        master).set('spark.driver.memory', '4G').set(
        'spark.executor.memory', '4G')

    sc: SparkContext = SparkContext.getOrCreate(confSpark)
    # os.environ["PYSPARK_SUBMIT_ARGS"] = (
    # "--packages graphframes:graphframes:0.8.0-spark2.4-s_2.11")
    sc.setLogLevel(logLevel='ERROR')
    return sc


@timer
def START_SPARK_STREAMING(sc, second: int = 5):
    """

    :param name: Name of Task, this will show on the UI of spark
    :param second: The batch duration of Spark Streaming
    :param master: Set the number of cores spark should use, leave blank will use all available cores
    :return: Spark Streaming context
    """
    from pyspark.streaming import StreamingContext

    # confSpark = SparkConf().setAppName(name).setMaster(
    #     master).set('spark.driver.memory', '4G').set(
    #     'spark.executor.memory', '4G')
    ssc = StreamingContext(sc, second)
    return ssc


def getPartitionItemNum(rddIn):
    # get length of each partition
    partitionItemNum = rddIn.glom().map(len).collect()
    return partitionItemNum


# some map functions
def json_trans_to_dict(lineIn: str):
    return json.loads(lineIn)


def trans_CSV_to_tuple(line: str, headers: list = []):
    # headers = headers.split(",")
    line = line.split(",")
    outputList = []
    if headers:
        for i in range(len(line)):
            if not line[i] in headers:
                outputList.append(line[i])
    else:
        for i in range(len(line)):
            outputList.append(line[i])
    return outputList


def list_flatten(list_in: list):
    """
    Flatten a list to 1 dimension
    :param list_in: A nested target list
    :return: A flattened list
    """
    for k in list_in:
        if not isinstance(k, (list, tuple)):
            yield k
        else:
            yield from list_flatten(k)


def most_common(list_in: list, n):

    counter = 0
    change_flag = -1
    result_list = []

    for tag, number in list_in:
        if number != change_flag:
            change_flag = number
            counter += 1
        if counter <= n:
            result_list.append((tag, number))
        else:
            break
    return result_list


class TweetTagBoard:
    tweet_num = 0
    top_num = 3
    tweet_tags_list = []
    tags_board = []

    def __init__(self):
        pass

    def add_one(self, tag_list: list):
        self.tweet_num += 1
        self.tweet_tags_list.append(tuple(tag_list))
        self.update_board()

    def update_board(self):
        count_dict = dict(Counter(list_flatten(self.tweet_tags_list)))
        sorted_tags = sorted(count_dict.items(), key=lambda key: (-key[1], key[0]))
        self.tags_board = most_common(sorted_tags, self.top_num)

    def pop_one(self):
        random_item_index = random.choice(range(len(self.tweet_tags_list)))
        self.tweet_tags_list.pop(random_item_index)
        self.tweet_num -= 1




















