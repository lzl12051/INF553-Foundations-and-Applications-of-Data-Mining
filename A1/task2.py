
from sys import argv
import json


finalOutput = {"result": ""}
outputFile = argv[3]
if_spark = argv[4]
reviewFile = argv[1]
businessFile = argv[2]
nCates = int(argv[5])


def no_spark():

    def LOAD_FILE(filePath):
        output = []
        with open(filePath) as file:
            for line in file:
                output.append(json.loads(line))
        return output

    # nCates = 5

    reviews = LOAD_FILE(reviewFile)
    business = LOAD_FILE(businessFile)

    def howManyKeys(dictIn):
        counter = 0
        for x in dictIn:
            counter += 1
        print(counter)

    def transReviewToDict(listIn: list):
        outputDict = {}
        for line in listIn:
            if line['business_id'] in outputDict:
                outputDict[line['business_id']][line['review_id']] = line
            else:
                outputDict[line['business_id']] = {line['review_id']: line}
        return outputDict

    def transBusinessToDict(listIn: list):
        outputDict = {}
        for line in listIn:
            outputDict[line['business_id']] = line
        return outputDict

    # Load the datasets
    businessDict = transBusinessToDict(business)
    reviewsDict = transReviewToDict(reviews)

    def joinDatasets(reviews, business):
        outputDict = {}
        for line in reviews:
            if line in business:
                outputDict[line] = business[line]
                outputDict[line]['reviews'] = reviews[line]
                # business[line]['reviews'] = line
        return outputDict

    allDict = joinDatasets(reviewsDict, businessDict)

    def countStars(dictIn: dict):
        outputDict = {}
        for line in dictIn:
            # print(line)
            cates = str(dictIn[line]['categories']).split(",")
            stars = dictIn[line]['stars']
            for w in cates:
                w = w.strip()
                if w in outputDict:
                    outputDict[w][0] += stars
                    outputDict[w][1] += 1
                else:
                    outputDict[w] = [stars, 1]
        return outputDict

    def avgCatesStars(countedStars: dict):
        # outputDict = {}
        for cate in countedStars:
            countedStars[cate] = countedStars[cate][0]/countedStars[cate][1]
        return countedStars

    def updateStarsByReview(allDict: dict):
        for line in allDict:
            reviewsOneBusiness = allDict[line]['reviews']
            star = 0
            for review in reviewsOneBusiness:
                # pprint(review)
                star += reviewsOneBusiness[review]['stars']
            allDict[line]['stars'] = star/len(reviewsOneBusiness)
        return allDict

    allDictStarUpdated = updateStarsByReview(allDict)
    finalCatesStar = avgCatesStars(countStars(allDictStarUpdated))
    catesStarList = sorted(finalCatesStar.items(),
                           key=lambda t: (-t[1], t[0]), reverse=0)
    outList = []
    c = 0
    for i in range(len(catesStarList)):
        if c >= nCates:
            break
        c += 1
        # , finalCatesStar[catesStarList[i][0]]])
        outList.append(catesStarList[i])

    return outList


def spark():
    from pyspark import SparkContext as sc
    from pyspark import SparkConf
    from multiprocessing import cpu_count
    cpuNum = cpu_count()
    confSpark = SparkConf().setAppName(
        "Task2_with_spark").setMaster(
        "local[*]").set(
        'spark.driver.memory', '12G')
    sc = sc.getOrCreate(confSpark)

    def transToDict(lineIn: str):
        return json.loads(lineIn)

    rddReviews = sc.textFile(reviewFile)
    rddBusiness = sc.textFile(businessFile)
    rddReviewsDict = rddReviews.map(transToDict)
    rddBusinessDict = rddBusiness.map(transToDict)

    def Map_extractIdAndCategories(line: dict):
        cateList = []
        cateStr = str(line['categories']).split(",")
        for cate in cateStr:
            cateList.append(cate.strip())
        # return {"business_id":line['business_id'],"categories":cateList}
        return (line['business_id'], cateList)

    rddExtarctedBusinessDict = rddBusinessDict.map(Map_extractIdAndCategories)

    def Map_emitBusinessIdAndStar(line: dict):
        outputTuple = (line["business_id"], (float(line["stars"]), 1))
        return outputTuple

    def Reduce_countAvgStars(value1: tuple, value2: tuple):
        return (value1[0]+value2[0], value1[1]+value2[1])

    def Map_calcAvgStars(line: tuple):
        outputTuple = (line[0], line[1][0]/line[1][1])
        return outputTuple

    def Reduce_joinStarsAndCategories(line1: tuple, line2: tuple):
        outputTuple = (line1, line2)
        return outputTuple

    def Map_emitCategoriesWithStars(line: tuple):
        if isinstance(line, float) or isinstance(line[1], float):
            return []
        elif len(line[1]) < 2:
            return []
        else:
            outputList = []
            stars = line[1][1]
            cateList = line[1][0]
            for x in cateList:
                tupleO = (x, (stars, 1))
                outputList.append(tupleO)
            return outputList

    def test(line):
        if not isinstance(line[1][0], str):
            return line

    rddBusinessStarFromReviews = rddReviewsDict.map(
        Map_emitBusinessIdAndStar).partitionBy(cpuNum, lambda line: hash(line[0]) % cpuNum)
    rddBusinessStarFromReviews = rddBusinessStarFromReviews.reduceByKey(
        Reduce_countAvgStars)
    rddBusinessStarFromReviews = rddBusinessStarFromReviews.map(
        Map_calcAvgStars)
    rddFull = rddExtarctedBusinessDict.union(rddBusinessStarFromReviews)

    rddFull = rddFull.reduceByKey(Reduce_joinStarsAndCategories)  # .collect()
    rddFull = rddFull.flatMap(Map_emitCategoriesWithStars)  # .collect()
    rddCategoires = rddFull.reduceByKey(Reduce_countAvgStars)  # .collect()
    rddCategoires = rddCategoires.filter(test)  # .collect()
    rddCategoires = rddCategoires.map(Map_calcAvgStars).collect()
    rddCategoires.sort(key=lambda x: [-x[1], x[0]])

    outputList = rddCategoires[0:nCates]
    return outputList


def sparkOrNot(if_spark):
    if if_spark == 'spark':
        return spark()
    elif if_spark == "no_spark":
        return no_spark()


finalOutput["result"] = sparkOrNot(if_spark)

with open(outputFile, 'a') as outfile:
    json.dump(finalOutput, outfile, ensure_ascii=False)
    outfile.write('\n')
