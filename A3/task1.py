import json
from sys import argv
import random
from itertools import combinations


trainReviewFile = argv[1]
outputFile = argv[2]
#print(argv)

def START_SPARK(name):
    from pyspark import SparkContext
    from pyspark import SparkConf
    confSpark = SparkConf().setAppName(name).setMaster(
        "local[*]")
    sc = SparkContext.getOrCreate(confSpark)
    sc.setLogLevel(logLevel='ERROR')
    return sc


def getPartitionItemNum(rddIn):
    # get length of each partition
    partitionItemNum = rddIn.glom().map(len).collect()
    return partitionItemNum


sc = START_SPARK('Task1')


def Map_jsonTransToDict(lineIn: str):
    return json.loads(lineIn)


def Map_extractUserAndBusiness(lineIn: dict):
    return (lineIn['business_id'], {lineIn['user_id']})


def Map_sortUsers(lineIn: tuple):
    return (lineIn[0], set(sorted(set(lineIn[1]))))


def Map_getAllUsers(lineIn):
    return lineIn['user_id']
# return the minHash value for every input set
# in terms of the given seed


def minHash(lineIn, userToRows: dict, seedList: list):
    # the hash function that hashing every row number
    def rowNumHash(rowIn, seed):
        totalLines = totalRows
        newLine = (seed*rowIn+1205) % 999371 % totalLines
        return int(newLine)

    busId = lineIn[0]
    usrSet = lineIn[1]
    sigLenth = len(seedList)
    totalRows = len(userToRows)
    originRows = []
    for u in usrSet:
        originRows.append(userToRows[u])

    signatureVector = []
    for i in range(0, sigLenth):
        hashedRows = map(lambda line: rowNumHash(
            line, seedList[i]), originRows)
        signatureVector.append(min(hashedRows))
    return (busId, signatureVector)

# return the LSH result


def lsh(lineIn: tuple, r: int):

    bus_id = lineIn[0]
    sig = lineIn[1]
    if len(sig) % r:
        raise ValueError(
            'The list of signatures can not be divided by the Row number "r"')
    resultList = []
    for x in range(r, len(sig), r):
        # use list to store the bus_ids
        # if cause performance problems, change to set
        temp = (hash(tuple(sig[x-r:x])), [bus_id])
        resultList.append(temp)
    return resultList


# read files in RDD
rddReview = sc.textFile(trainReviewFile).map(Map_jsonTransToDict)

# [(business_id, [user_id1, user_id2, ...])]
rddBusiness = rddReview.map(Map_extractUserAndBusiness).reduceByKey(
    lambda x, y: set(x).union(set(y))).map(Map_sortUsers).cache()
totalBusinessNum = rddBusiness.count()

# get all user_id list
userList = sorted(rddReview.map(Map_getAllUsers).distinct().collect())

# assign row numbers to user_id
rowsToUsersDict = dict(zip(range(0, len(userList)), userList))
userToRowsDict = dict(zip(userList, range(0, len(userList))))


# print('Users:', len(userList))
# print("Business:", totalBusinessNum)
# set random seed to varify the result
seed = 1205
random.seed(seed)

# set how many signatures in a vector
signatureNum = 900

# generate a list of random offset
randOffsetList = random.sample(range(1, 5*signatureNum), signatureNum)

# generate 100 minhash signatures; store in minHash signature matrix
signatureMatrix = rddBusiness.map(
    lambda line: minHash(line, userToRowsDict, randOffsetList))

# run LSH to calculate the similarities between each business
rddLsh = signatureMatrix.flatMap(lambda line: lsh(line, 2)).reduceByKey(
    lambda x, y: x + y).filter(lambda line: 1 if len(line[1]) > 1 else 0).map(
        lambda line: line[1])


def generateCandidatePairs(lineIn):
    comb = list(combinations(lineIn, 2))
    for x in range(0, len(comb)):
        comb[x] = tuple(sorted(comb[x]))
    return comb


rddCandidates = rddLsh.flatMap(generateCandidatePairs).map(
    lambda line: (line[0], line[1]))
candidates = set(sorted(rddCandidates.collect()))


def jcSimiSet(x, y):
    # print(x)
    #     x = set(x)
    #     y = set(y)
    up = x & y
    down = x | y
    return len(up)/len(down)


finalResult = []
data = dict(rddBusiness.collect())
for pairs in candidates:
    b1 = pairs[0]
    b2 = pairs[1]
    if b1 != b2:
        jcs = jcSimiSet(data[b1], data[b2])
        if jcs >= 0.05:
            finalResult.append((b1, b2, jcs))

outfile = open(outputFile, 'w')
outputList = []
for x in finalResult:
    b1, b2, simi = x[0], x[1], x[2]
    if b1 != b2:
        tempDict = {"b1": b1, "b2": b2, "sim": simi}
        # outputList.append(tempDict)
        json.dump(tempDict, outfile, ensure_ascii=False)
        outfile.write("\n")

outfile.close()

