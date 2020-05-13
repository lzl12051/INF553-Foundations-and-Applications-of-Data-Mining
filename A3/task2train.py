# import os
import string
from math import log2
from sys import argv
import lib553


# set the path of datasets
# PATH = os.getcwd()
# trainReviewFile = PATH+"/datasets/train_review.json"
# testFile = PATH+'/datasets/test_review.json'
# stopwords = PATH + "/datasets/stopwords"
trainReviewFile = argv[1]
stopwords = argv[3]

# start spark
sc = lib553.START_SPARK('A3Task2 Train')

# read files in RDD
rddReview = sc.textFile(trainReviewFile).map(
    lib553.Map_jsonTransToDict)  # .cache()


def cleanText(line: str, stopWords):
    result = []
    line = line.lower()
    line = line.translate(str.maketrans('', '', string.punctuation))
    line = line.translate(str.maketrans('', '', "1234567890"))
    # print(line)
    for word in line.split():
        if word not in stopWords:
            result.append(word)
    return result


def Map_extractText(lineIn: dict, stopWords: list):
    def mapCleanText(line: str):
        result = []
        line = line.lower()
        line = line.translate(str.maketrans('', '', string.punctuation))
        line = line.translate(str.maketrans('', '', "1234567890"))
        print(line)
        for word in line.split():
            if word not in stopWords:
                result.append([word, 1])
        return result
    return mapCleanText(lineIn['text'])


# load stopwords
def LOAD_STOPWORDS_IN_MEMORY(stopWordsFile):
    stopWords = set()
    with open(stopWordsFile) as stwf:
        for x in stwf.readlines():
            v = x[:-1]
            stopWords.add(v)
    return stopWords


stopwordsList = LOAD_STOPWORDS_IN_MEMORY(stopwords)


def Map_extractBusAndReview(lineIn: dict, stopWords):
    result = [lineIn['business_id'], cleanText(lineIn['text'], stopWords)]
    return result


rddTFIDFbase = rddReview.map(lambda line: Map_extractBusAndReview(
    line, stopwordsList)).reduceByKey(lambda x, y: x+y)


def Map_docWords(lineIn):
    return set(lineIn[1])


rddAllWords = rddTFIDFbase.flatMap(Map_docWords).cache()
countThreshold = int(rddAllWords.count()*0.0000005)
# Don't filter the wordsInDocDict,
# when calculate the TFIDF can search the dict for data
wordsInDocDict = dict(rddAllWords.map(
    lambda line: (line, 1)).reduceByKey(lambda x, y: x+y).collect())
rddAllWords.unpersist()


# calculate TFIDF and make business profiles
def Map_calcTFIDF(lineIn, wordsInDoc: dict, numOfDocs: int):
    text = lineIn[1]
    countingDict = {}
    for x in text:
        countingDict[x] = countingDict.get(x, 0)+1
    # print(countingDict)
    countedWordsList = sorted(list(
        zip(countingDict.keys(), countingDict.values())),
        key=lambda key: -key[1])
    # print(countedWordsList)
    maxWordsNumber = countedWordsList[0][1]
    tfList = []
    for word in countedWordsList:
        tfList.append((word[0], (word[1]/maxWordsNumber)
                       * (log2(numOfDocs/wordsInDoc[x]))))
    tfList.sort(key=lambda line: -line[1])
    k, _discard = zip(*tfList[:200])
    return (lineIn[0], k)


numOfDocs = rddReview.count()
rddBusinessProfile = rddTFIDFbase.map(
    lambda line: Map_calcTFIDF(line, wordsInDocDict, numOfDocs))
businessProfile = dict(rddBusinessProfile.collect())
del wordsInDocDict

# make User profiles


def Map_extractUserAndBusiness(lineIn: dict):
    return (lineIn['user_id'], {lineIn['business_id']})


def Map_geneUserProfile(lineIn, busProfile: dict):
    busIdList = lineIn[1]
    userProfile = []
    for busId in busIdList:
        userProfile += busProfile[busId]

    # to eliminate the duplicates
    userProfile = set(userProfile)
    return (lineIn[0], userProfile)


modelFile = argv[2]
rddUserBusiness = rddReview.map(
    Map_extractUserAndBusiness).reduceByKey(lambda x, y: x.union(y))
userProfile = dict(rddUserBusiness.map(
    lambda line: Map_geneUserProfile(line, businessProfile)).collect())
rddReview.unpersist()


# save the model file
with open(modelFile, 'wb') as model:
    import pickle
    pickle.dump(businessProfile, model, pickle.HIGHEST_PROTOCOL)
    pickle.dump(userProfile, model, pickle.HIGHEST_PROTOCOL)
