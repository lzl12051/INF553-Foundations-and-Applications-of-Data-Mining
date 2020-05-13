import lib553
from sys import argv
# from math import sqrt
sc = lib553.START_SPARK('A3T3 predict')
# PATH = lib553.path()
trainReviewFile = argv[1]  # PATH+"/datasets/train_review.json"  # argv[1]
testReviewFile = argv[2]  # PATH+"/datasets/test_review.json"  # argv[2]
# itemModelFile = argv[3]  # PATH+"/task3item.model"  # argv[3]
# userModelFile = PATH+'/task3user.model'  # argv[3]

# testFile = PATH+'/datasets/test_review.json'


def modelOutput(outputFile, data):
    import json
    # outputFile = argv[2]  # '/Users/l-zl/Desktop/task3item.model'  #
    outfile = open(outputFile, 'w')
    # outputList = []

    headerList = ['user_id', 'business_id']
    for x in data:
        b1, b2, simi = x[0], x[1], x[2]
        tempDict = {headerList[0]: b1, headerList[1]: b2, "stars": simi}
        json.dump(tempDict, outfile, ensure_ascii=False)
        outfile.write("\n")
    outfile.close()


rddReview = sc.textFile(trainReviewFile).map(lib553.Map_jsonTransToDict)
rddTest = sc.textFile(testReviewFile).map(lib553.Map_jsonTransToDict)
# print(rddTest.take(1), '\n', rddReview.take(1))


def item_based():
    itemModelFile = argv[3]  # PATH+"/task3item.model"  # argv[3]

    def loadModel(modelPath: str):
        import json
        with open(modelPath, 'r') as model:
            result = {}
            for line in model.readlines():
                data = json.loads(line)
                if data['b1'] in result:
                    result[data['b1']].append((data['b2'], data['sim']))
                else:
                    result[data['b1']] = [(data['b2'], data['sim'])]

                if data['b2'] in result:
                    result[data['b2']].append((data['b1'], data['sim']))
                else:
                    result[data['b2']] = [(data['b1'], data['sim'])]
                # result[data['b2']]=result.get(data['b2'],[(data['b1'], data['sim'])]).append((data['b1'], data['sim']))
        for line in result.keys():
            result[line] = dict(sorted(result[line], key=lambda x: -x[1]))
        return result

    itemModel = loadModel(itemModelFile)
    # lib553.info(itemModel)
    rddUserAndBusinessStar = rddReview.map(
        lambda line: (line['user_id'], [(line['business_id'], line['stars'])]))

    rddBusinessStar = rddUserAndBusinessStar.reduceByKey(
        lambda x, y: x+y).mapValues(lambda v: dict(v))
    businessWithStarDict = rddBusinessStar.collectAsMap()

    rddBusinessStar = rddReview.map(
        lambda line: (line['business_id'], (line['stars'], 1)))

    # rddBusinessAvgStar = rddBusinessStar.reduceByKey(
    #     lambda x, y: (x[0]+y[0], x[1]+y[1])).mapValues(lambda v: v[0]/v[1])
    # businessAvgStarDict = rddBusinessAvgStar.collectAsMap()

    def Map_predict(pair, model, data):
        user = pair['user_id']
        item = pair['business_id']
        try:
            userHist = set(data[user].keys())
            simItem = set(model[item].keys())
        except KeyError:
            return (user, item, -1)
        overlap = userHist & simItem
        upNum = []
        downNum = []
        for i in overlap:
            try:
                upNum.append(model[item][i]*data[user][i])
                downNum.append(model[item][i])
            except KeyError:
                return (user, item, -2)
        if sum(downNum) == 0:
            return (user, item, -3)
        predictStar = sum(upNum)/sum(downNum)
        return (user, item, predictStar)

    predict = rddTest.map(lambda pair: Map_predict(
        pair, itemModel, businessWithStarDict)).filter(lambda line: line[2] >= 0).collect()
    return predict


def user_based():
    userModelFile = argv[3]  # PATH + '/task3user.model'  # argv[3]

    def loadModel(modelPath: str):
        import json
        with open(modelPath, 'r') as model:
            result = {}
            for line in model.readlines():
                data = json.loads(line)
                if data['u1'] in result:
                    result[data['u1']].append((data['u2'], data['sim']))
                else:
                    result[data['u1']] = [(data['u2'], data['sim'])]

                if data['u2'] in result:
                    result[data['u2']].append((data['u1'], data['sim']))
                else:
                    result[data['u2']] = [(data['u1'], data['sim'])]
                # result[data['b2']]=result.get(data['b2'],[(data['b1'], data['sim'])]).append((data['b1'], data['sim']))
        for line in result.keys():
            result[line] = dict(sorted(result[line], key=lambda x: -x[1]))
        return result

    userModel = loadModel(userModelFile)
    # lib553.info(userModel)
    rddUserAndBusinessStar = rddReview.map(
        lambda line: (line['user_id'], [(line['business_id'], line['stars'])]))

    rddBusinessStar = rddUserAndBusinessStar.reduceByKey(
        lambda x, y: x+y).mapValues(lambda v: dict(v))
    businessWithStarDict = rddBusinessStar.collectAsMap()

    def Map_predict(pair, model, data):
        user = pair['user_id']  # 'mEzc6LeTNiQgIVsq3poMbg'
        item = pair['business_id']  # '9UVkAmyodpaSBm49DBKFNw'
        n = 5
        try:
            simUsers = model[user]
        except KeyError:
            return (user, item, -1)
        _ = []
        for u in simUsers.keys():
            if item in businessWithStarDict[u]:
                _.append((u, data[u][item], simUsers[u]))
        if len(_) < n:
            return (user, item, -1)
        _ = sorted(_, key=lambda line: -line[2])[:n]
        # print(_)

        def calcWeightedStar(line: tuple):
            return (line[0], line[1]*line[2], line[2])
        _ = list(map(calcWeightedStar, _))
        _discard, up, down = zip(*_)
        return (user, item, sum(up)/sum(down))

    predict = rddTest.map(lambda pair: Map_predict(
        pair, userModel, businessWithStarDict)).filter(lambda line: line[2] >= 0).collect()
    return predict


if argv[5] == 'item_based':
    predict = item_based()
else:
    predict = user_based()

modelOutput(argv[4], predict)
