import json
# from pprint import pprint
from pyspark import SparkContext as sc
from pyspark import SparkConf
from sys import argv
# from multiprocessing import cpu_count

confSpark = SparkConf().setAppName(
    "Task3").setMaster(
    "local[*]").set(
    'spark.driver.memory', '4G')  # .set(
#   "spark.default.parallelism", "8")  # .set(
# "spark.files.maxPartitionBytes", "128M")
sc = sc.getOrCreate(confSpark)


reviewFile = argv[1]
outputFile = argv[2]
custom = str(argv[3])
partitions = int(argv[4])
reviewsThreshold = int(argv[5])
finalOutputDict = {}

if custom == "default":
    customFlag = 0
else:
    customFlag = 1


def transToDict(lineIn: str):
    x = json.loads(lineIn)
    output = (x["business_id"].strip(), 1)
    return output


rddReviews = sc.textFile(reviewFile)

# print("Partition Num:", rddReviewsDict.getNumPartitions())


def getPartitionItemNum(rddIn):
    # get length of each partition
    partitionItemNum = rddIn.glom().map(len).collect()
    return partitionItemNum


def Map_emitBusinessWithReviewsNum(line):
    return (line["business_id"], 1)


def customizedPartitioner(key):
    return hash(key)


rddBusinessWithReviewNum = rddReviews.map(transToDict)  # .persist()
if customFlag == 1:
    print("***********Using costomized partition!***********")
    rddBusinessWithReviewNum1 = rddBusinessWithReviewNum.partitionBy(
        partitions, customizedPartitioner)
    rddBusinessWithReviewNum2 = rddBusinessWithReviewNum1.reduceByKey(
        lambda pair1, pair2: pair1+pair2)
    businessWithNReviews = rddBusinessWithReviewNum2.filter(
        lambda pair: pair[1] > reviewsThreshold).collect()
    finalOutputDict["n_partitions"] = rddBusinessWithReviewNum1.getNumPartitions()
    finalOutputDict["n_items"] = getPartitionItemNum(rddBusinessWithReviewNum1)
    finalOutputDict["result"] = businessWithNReviews
else:
    print("***********Using default partition!**************")
    rddBusinessWithReviewNum1 = rddBusinessWithReviewNum.reduceByKey(
        lambda pair1, pair2: pair1+pair2)
    businessWithNReviews = rddBusinessWithReviewNum1.filter(
        lambda pair: pair[1] > reviewsThreshold).collect()
    finalOutputDict["n_partitions"] = rddBusinessWithReviewNum.getNumPartitions()
    finalOutputDict["n_items"] = getPartitionItemNum(rddBusinessWithReviewNum)
    finalOutputDict["result"] = businessWithNReviews

with open(outputFile, 'a') as outfile:
    json.dump(finalOutputDict, outfile, ensure_ascii=False)
    outfile.write('\n')
