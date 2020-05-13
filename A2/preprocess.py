import csv
import json
# Preprocess of files
import os


def transToDict(lineIn: str):
    return json.loads(lineIn)


def START_SPARK():
    from pyspark import SparkContext
    from pyspark import SparkConf
    confSpark = SparkConf().setAppName("Task1").setMaster(
        "local[*]")

    sc = SparkContext.getOrCreate(confSpark)
    sc.setLogLevel(logLevel='ERROR')
    return sc


def PREPROCESS(sc):
    nvBusinessList = sc.textFile(businessFile).map(transToDict).filter(
        lambda line: line['state'] == 'NV').map(lambda line: line['business_id']).collect()
    # print(rddBusiness.count(),rddBusiness.first())
    # print(len(rddBusiness))
    nvBusinessSet = set(nvBusinessList)
    del nvBusinessList
    CSV = sc.textFile(reviewFile).map(transToDict).filter(
        lambda line: line if line["business_id"] in nvBusinessSet else 0).map(
        lambda line: (line["user_id"], line["business_id"])).collect()

    headerCSV = ['user_id', 'business_id']
    outFile = 'user_business.csv'
    with open(outFile, 'w') as f:
        csvOut = csv.writer(f)
        csvOut.writerow(headerCSV)
        csvOut.writerows(CSV)
    print("Saving successful to path:", outFile)
    del CSV


PATH = os.getcwd()

businessFile = PATH + "/datasets/business.json"
reviewFile = PATH + "/datasets/review.json"

sc = START_SPARK()
PREPROCESS(sc)
