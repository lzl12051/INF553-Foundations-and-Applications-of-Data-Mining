import lib553
import json
import pickle
# import os
from sys import argv
from math import sqrt

# PATH = os.getcwd()
# testFile = PATH + '/datasets/test_review.json'
# modelFile = PATH + '/task2.model'

testFile = argv[1]
modelFile = argv[2]
sc = lib553.START_SPARK('A3Task2 Predict')

with open(modelFile, 'rb') as f:
    businessProfile = pickle.load(f)
    userProfile = pickle.load(f)


def Map_calcCosDistance_Prediction(data, busProfile: dict, userProfile: dict):
    # dataIter = list(dataIter)
    # print(dataIter)
    userId = data['user_id']
    busId = data['business_id']
    try:
        userProf = userProfile[userId]
        businessProf = busProfile[busId]
    except KeyError:
        data['sim'] = 0
        return data

    def cosDis(line1, line2):
        inter = len(line1.intersection(line2))
        down = sqrt(len(line1)*len(line2))
        result = inter/down
        return result
    cosDistance = cosDis(userProf, businessProf)
    data['sim'] = cosDistance
    return data


rddTest = sc.textFile(testFile).map(lib553.Map_jsonTransToDict).map(
    lambda line: Map_calcCosDistance_Prediction(
        line, businessProfile, userProfile)).filter(
    lambda line: 1 if line['sim'] >= 0.01 else 0).collect()

outfile = argv[3]
with open(outfile, 'w') as f:
    for x in rddTest:
        json.dump(x, f)
        f.write('\n')
