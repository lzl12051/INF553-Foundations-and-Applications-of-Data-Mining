import sys
import json
import string
from pyspark import SparkContext as sc
from pyspark import SparkConf


def transToDict(lineIn: str):
    return json.loads(lineIn)


def task1_B(givenYear: str):

    def Map_countGivenYear(givenYear: str, reviewIn: dict):
        if reviewIn["date"][0:4] == givenYear:
            return 1
        return 0

    if not isinstance(givenYear, str):
        givenYear = str(givenYear)
    yearsNumOfReview = rddReviewsDict.map(
        lambda line: Map_countGivenYear(givenYear, line)).collect()
    sumNum = 0
    for x in yearsNumOfReview:
        sumNum += x
    return sumNum


def task1_C():
    def Map_extractUserId(line):
        tupleOut = (line["user_id"], 1)
        return tupleOut
    rddUserId = rddReviewsDict.map(Map_extractUserId)
    distinctUserId = rddUserId.reduceByKey(lambda x, y: x+y).collect()
    return len(distinctUserId)


def task1_D(userNum: int):
    if not isinstance(userNum, int):
        userNum = int(userNum)

    def Map_extractUserIdAndReviewNum(line):
        tupleOut = (line["user_id"], 1)
        return tupleOut
    rddUserIdAndReviews = rddReviewsDict.map(Map_extractUserIdAndReviewNum)
    userIdAndReviewNum = rddUserIdAndReviews.reduceByKey(
        lambda x, y: x+y).collect()
    userIdAndReviewNum.sort(key=lambda t: (-t[1], t[0]))

    outputList = []
    for i in range(userNum):
        outputList.append(list(userIdAndReviewNum[i]))
    return outputList


def task1_E(stopWordsFile, nTopWords):

    def LOAD_STOPWORDS_IN_MEMORY(stopWordsFile):
        stopWords = set()
        with open(stopWordsFile) as stwf:
            for x in stwf.readlines():
                v = x[:-1]
                stopWords.add(v)
        return stopWords

    def Map_cleanText(line: str):
        result = []
        line = line.lower()
        line = line.translate(str.maketrans('', '', string.punctuation))
        line = line.translate(str.maketrans('', '', "1234567890"))
        for word in line.split():
            if word not in stopWords:
                result.append(word)
        return result

    def Map_generateKVPairs(word):
        outTuple = (word, 1)
        return outTuple

    stopWords = LOAD_STOPWORDS_IN_MEMORY(stopWordsFile)
    rddReviewWords = rddReviewsDict.flatMap(
        lambda line: Map_cleanText(line["text"]))
    rddReviewWords = rddReviewWords.map(Map_generateKVPairs)
    freqWords = rddReviewWords.reduceByKey(lambda x, y: x+y).collect()
    freqWords.sort(key=lambda t: (-t[1], t[0]))
    outputList = []
    for _words in freqWords[0:nTopWords]:
        outputList.append(_words[0])
    return outputList


# Get arguments from system
argv = sys.argv
reviewFile = argv[1]
outputFile = argv[2]
stopWordsFile = argv[3]
givenYear = str(argv[4])
mUsers = int(argv[5])
nFreqWords = int(argv[6])

finalOutPutDict = {}
# Start spark
confSpark = SparkConf().setAppName("Task1").setMaster(
    "local[*]").set('spark.driver.memory', '12G')
sc = sc.getOrCreate(confSpark)

# Loading files in RDD
rddReviews = sc.textFile(reviewFile)

# Trans RDD content to dict
rddReviewsDict = rddReviews.map(transToDict)
rddReviewsDict.persist()


finalOutPutDict["A"] = rddReviews.count()
finalOutPutDict["B"] = task1_B(givenYear)
finalOutPutDict["C"] = task1_C()
finalOutPutDict["D"] = task1_D(mUsers)
finalOutPutDict["E"] = task1_E(stopWordsFile, nFreqWords)

with open(outputFile, 'a') as outfile:
    json.dump(finalOutPutDict, outfile, ensure_ascii=False)
    outfile.write('\n')
