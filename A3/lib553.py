# This is a lib of inf553 assignments
import json
import time
import os
from sys import getsizeof


def timer(f):
    def wrapper(*args, **kwargs):
        startTime = time.time()
        print(*args)
        result = f(*args, **kwargs)
        endTime = time.time()
        runingTime = endTime-startTime
        if runingTime*1000 < 1000:
            print(f, "Duration:%.3fms" % (runingTime*1000))
        else:
            print(f, "Duration:%.3fs" % runingTime)
        return result
    return wrapper


@timer
def START_SPARK(name='Assignment'):
    from pyspark import SparkContext
    from pyspark import SparkConf
    confSpark = SparkConf().setAppName(name).setMaster(
        "local[*]").set('spark.driver.memory', '12G').set(
            'spark.worker.memory', '4G')

    sc = SparkContext.getOrCreate(confSpark)
    sc.setLogLevel(logLevel='ERROR')
    return sc


def getPartitionItemNum(rddIn):
    # get length of each partition
    partitionItemNum = rddIn.glom().map(len).collect()
    return partitionItemNum


def info(data):
    print(type(data))
    try:
        print("Lenth:", len(data))
    except TypeError:
        print("The input data has no lenth")

    size = getsizeof(data)
    if size < 1024:
        print("Size:", size, "b")
    elif size/1024 < 1024:
        print("Size:", size / 1024, "Kb")
    elif size/1024/1024 < 1024:
        print("Size:", size / 1024 / 1024, "Mb")
    else:
        print("Size:", size / 1024 / 1024 / 1024, "Gb")


def path():
    return os.getcwd()


# some map functions


def Map_jsonTransToDict(lineIn: str):
    return json.loads(lineIn)
