import os

from graphframes import *
from pyspark.sql import SparkSession
from sys import argv
import lib553

# os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11"


sc = lib553.START_SPARK('A4 Task1', 'local[3]')

spark = SparkSession.builder.master("local[*]").appName("A4Task1").config('spark.driver.memory', '4G').config(
    'spark.executor.memory', '4G').getOrCreate()

# dataFile = 'datasets/ub_sample_data.csv'
# outputFile = 'A4Task1output.txt'
filterThreshold = int(argv[1])
dataFile = argv[2]
outputFile = argv[3]

rddData = sc.textFile(dataFile).map(lambda line: lib553.Map_transCSVToTuple(
    line, ['user_id', 'business_id'])).filter(lambda line: line if line is not None else 0)

rddUserAndBus = rddData.mapValues(lambda v: {v}).reduceByKey(lambda x, y: x.union(y))

userProfile = rddUserAndBus.collectAsMap()


def Map_findRelationship(line, data):
    user_id = line[0]
    busSet = line[1]
    resultList = []
    for user in data:
        if user != user_id:
            if len(busSet & data[user]) >= filterThreshold:
                resultList.append((user_id, user))
    if resultList == []:
        return (None,)
    return resultList


rddRelationship = rddUserAndBus.map(
    lambda line: Map_findRelationship(line, userProfile))
# print(rddRelationship.take(10))
rddNodes = rddRelationship.filter(lambda line: 0 if line[0] is None else 1).map(lambda line: (line[0][0],))
rddRelationship = rddRelationship.flatMap(lambda line: line).filter(lambda line: line is not None)

users = spark.createDataFrame(rddNodes, schema=['id'])
relationship = spark.createDataFrame(rddRelationship, schema=['src','dst'])

g = GraphFrame(users, relationship)

# g.vertices.show(1, 50)
#
# g.inDegrees.show(5, 22)
# g.outDegrees.show(5, 22)
print('Start LPA')
result = g.labelPropagation(maxIter=5)
print('LPA end')
rddLabeled = result.rdd.map(lambda line: (line[1], {line[0]})).reduceByKey(lambda x, y: x.union(y)).map(
    lambda line: (len(line[1]), sorted(line[1]))).sortByKey()

singleNodeCommunity = rddLabeled.filter(lambda line: line[0] == 1).sortBy(lambda line: line[1]).collect()
re = rddLabeled.filter(lambda line: line[0] != 1).sortBy(lambda line: (line[0], line[1])).collect()

with open(outputFile, 'w') as f:
    for com in singleNodeCommunity:
        f.write('\'' + str(com[1][0]) + '\'')
        f.write('\n')
    for com in re:
        f.write(', '.join(['\'' + i + '\'' for i in com[1]]))
        f.write('\n')
