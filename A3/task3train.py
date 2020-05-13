import hashlib
from itertools import combinations
from math import sqrt
from random import sample, seed
from sys import argv
import lib553

seed(1205)
sc = lib553.START_SPARK('A3T3 Train')

PATH = lib553.path()
trainReviewFile = argv[1]

rddReview = sc.textFile(trainReviewFile).map(lib553.Map_jsonTransToDict)


def Map_pearson(pair, businessWithStar, businessNoStar):
    b1 = pair[0]
    b2 = pair[1]
    corater = businessNoStar[b1] & businessNoStar[b2]

    b1Avg = 0
    b2Avg = 0
    for user in corater:
        b1Avg += businessWithStar[b1][user]
        b2Avg += businessWithStar[b2][user]
    b1Avg /= len(corater)
    b2Avg /= len(corater)
    # print(b1Avg,b2Avg)
    number = 0
    for user in corater:
        number += (businessWithStar[b1][user]-b1Avg) * \
            (businessWithStar[b2][user]-b2Avg)
    # denominator = 0
    b1Dis = 0
    b2Dis = 0
    for user in corater:
        b1Dis += pow(businessWithStar[b1][user]-b1Avg, 2)
        b2Dis += pow(businessWithStar[b2][user]-b1Avg, 2)
    denominator = sqrt(b1Dis*b2Dis)
    if denominator == 0:
        return (b1, b2, -1)
    return (b1, b2, number/denominator)


def modelOutput(outputFile, data, modelType: str):
    import json
    # outputFile = argv[2]  # '/Users/l-zl/Desktop/task3item.model'  #
    outfile = open(outputFile, 'w')
    # outputList = []
    if modelType == 'item_based':
        headerList = ['b1', 'b2']
    else:
        headerList = ['u1', 'u2']

    for x in data:
        b1, b2, simi = x[0], x[1], x[2]
        tempDict = {headerList[0]: b1, headerList[1]: b2, "sim": simi}
        json.dump(tempDict, outfile, ensure_ascii=False)
        outfile.write("\n")
    outfile.close()


def itemBased():
    rddBusinessAndUser = rddReview.map(
        lambda line: (line['business_id'], line['user_id']))
    rddBusiness = rddBusinessAndUser.mapValues(lambda v: {v}).reduceByKey(
        lambda x, y: x.union(y)).filter(
        lambda line: len(line[1]) > 2)
    businessNoStarDict = rddBusiness.collectAsMap()

    def Map_findBusPair(data: iter, businessProfile: dict):
        def isCorated(line1: set, line2: set, threshold: int):
            if len(line1 & line2) >= threshold:
                return 1
            else:
                return 0

        resultList = []
        for line in data:
            # print(line)
            for bus in businessProfile.keys():
                if line[0] != bus:
                    if isCorated(line[1], businessProfile[bus], 3):
                        resultList.append(tuple(sorted([line[0], bus])))
        return resultList

    rddPairs = rddBusiness.mapPartitions(
        lambda data: Map_findBusPair(data, businessNoStarDict)).distinct()
    rddBusinessAndUserStar = rddReview.map(
        lambda line: (line['business_id'], {(line['user_id'], line['stars'])}))

    rddBusinessStar = rddBusinessAndUserStar.reduceByKey(
        lambda x, y: x.union(y)).mapValues(lambda v: dict(v))
    businessWithStarDict = rddBusinessStar.collectAsMap()

    pearson = rddPairs.map(lambda pair: Map_pearson(
        pair, businessWithStarDict, businessNoStarDict)).filter(
            lambda line: line[2] > 0)
    itemBasedModel = pearson.collect()

    modelOutput(argv[2], itemBasedModel, argv[3])


def userBased():

    rddUserAndBusiness = rddReview.map(
        lambda line: (line['user_id'], line['business_id']))
    rddUser = rddUserAndBusiness.mapValues(lambda v: {v}).reduceByKey(
        lambda x, y: x.union(y)).filter(
        lambda line: len(line[1]) > 2)
    userDict = rddUser.collectAsMap()
    # userIndex = dict(zip(userDict.keys(), range(len(userDict))))

    allBusiness = rddReview.map(
        lambda line: line['business_id']).distinct().collect()
    businessIndex = dict(zip(allBusiness, range(len(allBusiness))))
    totalRows = len(businessIndex)

    def minHash(lineIn, totalRows, userToRows: dict, seedList: list):
        # the hash function that hashing every row number
        def rowNumHash(rowIn, seed):
            # totalLines = totalRows  # totalRows+1
            newLine = (seed*rowIn+17) % 277  # % (totalRows*3)
            return int(newLine)

        busId = lineIn[0]
        usrSet = lineIn[1]
        sigLenth = len(seedList)
        # totalRows = len(userList)
        # get every row number of user which contained in the business_id
        originRows = []
        for u in usrSet:
            originRows.append(userToRows[u])
        # print("Ori:",originRows,'\n')

        signatureVector = []
        for i in range(0, sigLenth):
            hashedRows = list(map(lambda line: rowNumHash(
                line, seedList[i]), originRows))
            # print("Hsh:",list(hashedRows),'\n')

            signatureVector.append(min(hashedRows))
        return (busId, signatureVector)

    # set how many signatures in a vector
    signatureNum = 30

    # generate a list of random offset
    randOffsetList = sample(range(1, 2*signatureNum), signatureNum)

    # generate N minhash signatures; store in minHash signature matrix
    signatureMatrix = rddUser.map(lambda line: minHash(
        line, totalRows, businessIndex, randOffsetList))
    # return the LSH result

    def lsh(lineIn: tuple, r: int):

        bus_id = lineIn[0]
        sig = lineIn[1]
        if len(sig) % r:
            raise ValueError(
                'The list of signatures can not be divided by the Row number "r"')
        resultList = []

        for x in range(r, len(sig), r):
            h = hashlib.md5()
            h.update((str(sig[x-r:x])).encode('utf-8'))
            temp = (h.hexdigest(), [bus_id])
            resultList.append(temp)
        return resultList

    # run LSH to calculate the similarities between each business
    rddLsh = signatureMatrix.flatMap(lambda line: lsh(line, 2)).reduceByKey(
        lambda x, y: x + y).filter(lambda line: 1 if len(line[1]) > 1 else 0).map(
            lambda line: line[1])
    # print(rddLsh.count())

    def generateCandidatePairs(lineIn):
        comb = list(combinations(lineIn, 2))
        for x in range(0, len(comb)):
            comb[x] = tuple(sorted(comb[x]))
        return comb

    rddCandidates = rddLsh.flatMap(generateCandidatePairs).map(
        lambda line: (line[0], line[1]))
    # print(rddCandidates.count())  # 1174105 after distinct

    def Filter_calcJCSim(pair, data, threshold):
        def jcSimiSet(x: set, y: set):
            up = x & y
            down = x | y
            return len(up)/len(down)
        if pair[0] != pair[1]:
            if jcSimiSet(data[pair[0]], data[pair[1]]) >= 0.01:
                return 1
            else:
                return 0
        else:
            return 0

    def Filter_coratedNum(pair, data, threshold):
        if len(data[pair[0]] & data[pair[1]]) >= threshold:
            return 1
        else:
            return 0

    jcThreshold = 0.01
    coratedNum = 3
    rddCandidates = rddCandidates.filter(lambda line: Filter_calcJCSim(
        line, userDict, jcThreshold)).distinct().filter(
        lambda line: Filter_coratedNum(line, userDict, coratedNum))
    # userCandidates = rddCandidates.collect()
    rddUserAndBusinessStar = rddReview.map(
        lambda line: (line['user_id'], {(line['business_id'], line['stars'])}))

    rddUserStar = rddUserAndBusinessStar.reduceByKey(
        lambda x, y: x.union(y)).mapValues(lambda v: dict(v))
    userWithStarDict = rddUserStar.collectAsMap()

    userPearson = rddCandidates.map(lambda pair: Map_pearson(
        pair, userWithStarDict, userDict)).filter(lambda line: line[2] > 0)
    userBasedModel = userPearson.collect()
    userPearson = rddCandidates.map(lambda pair: Map_pearson(
        pair, userWithStarDict, userDict)).filter(lambda line: line[2] > 0)
    userBasedModel = userPearson.collect()
    modelOutput(argv[2], userBasedModel, argv[3])


if argv[3] == 'item_based':
    itemBased()
else:
    userBased()
