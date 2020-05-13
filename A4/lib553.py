# This is a lib of inf553 assignments
import json
import time
import os


def timer(f):
    def wrapper(*args, **kwargs):
        startTime = time.time()
        print(*args)
        result = f(*args, **kwargs)
        endTime = time.time()
        runingTime = endTime - startTime
        if runingTime * 1000 < 1000:
            print(f, "Duration:%.3fms" % (runingTime * 1000))
        else:
            print(f, "Duration:%.3fs" % runingTime)
        return result

    return wrapper


@timer
def START_SPARK(name='Assignment', master='local[*]'):
    from pyspark import SparkContext
    from pyspark import SparkConf
    import os
    confSpark = SparkConf().setAppName(name).setMaster(
        master).set('spark.driver.memory', '4G').set(
        'spark.executor.memory', '4G')

    sc = SparkContext.getOrCreate(confSpark)
    # os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.8.0-spark2.4-s_2.11")
    sc.setLogLevel(logLevel='ERROR')
    return sc


def getPartitionItemNum(rddIn):
    # get length of each partition
    partitionItemNum = rddIn.glom().map(len).collect()
    return partitionItemNum


def info(data):
    from sys import getsizeof
    print(type(data))
    try:
        print("Lenth:", len(data))
    except TypeError:
        print("The input data has no lenth")

    size = getsizeof(data)
    if size < 1024:
        print("Size:", size, "b")
    elif size / 1024 < 1024:
        print("Size:", size / 1024, "Kb")
    elif size / 1024 / 1024 < 1024:
        print("Size:", size / 1024 / 1024, "Mb")
    else:
        print("Size:", size / 1024 / 1024 / 1024, "Gb")


def path():
    return os.getcwd()


def plotGraph(cutted_edge, filename='', dpi=300, nodeSize=5, nodeColor='r', nodeShape='o',
              withLabels=0, edgeWidth=0.1, fontSize=12, fontColor='black', figSize=10):
    import matplotlib.pyplot as plt
    import networkx as nx

    plt.figure(figsize=(figSize, figSize))
    Gplot = nx.Graph
    for edge in cutted_edge:
        Gplot.add_edge(edge[0], edge[1])
    pos = nx.spring_layout(Gplot)
    nx.draw(Gplot, pos, node_size=nodeSize, node_color=nodeColor,
            width=edgeWidth, with_labels=withLabels, node_shape=nodeShape,
            font_size=fontSize, font_color=fontColor)
    if filename != '':
        plt.savefig(filename, dpi=dpi)
    plt.show()
    plt.close()


# some map functions


def Map_jsonTransToDict(lineIn: str):
    return json.loads(lineIn)


def Map_transCSVToTuple(line: str, headers: list):
    # headers = headers.split(",")
    line = line.split(",")
    outputList = []
    for i in range(len(line)):
        if not line[i] in headers:
            outputList.append(line[i])
    return outputList


# define the node class
class Node:

    def __init__(self, nodeId):
        self.nodeId = nodeId
        self.level = 0
        self.parent = set()
        self.firstValue = 0
        self.secondValue = 1.0
        self.community = -1

    def setLeaf(self):
        self.leaf = True

    def addParent(self, parentNodeName):
        self.parent.add(parentNodeName)
        # self.parent.append(parentNodeName)

    def getParent(self):
        return self.parent

    def setLevel(self, newLevel):
        self.level = newLevel

    def increaseFirstValue(self, newValue):
        self.firstValue += newValue

    def increaseSecondValue(self, newValue):
        self.secondValue += newValue

    def getEdges(self):
        resultList = []
        for p in self.parent:
            resultList.append((self.nodeId, p))
        return resultList

    def getEdgesWithValue(self, sort=0):
        resultList = []
        if sort:
            for p in self.parent:
                resultList.append(
                    tuple(sorted(self.nodeId, p)), self.secondValue)
        else:
            for p in self.parent:
                resultList.append((self.nodeId, p), self.secondValue)
        return resultList

    def collectAsMap(self):
        resultDict = {self.nodeId: {
            'level': self.level, 'parent': self.parent}}



def findAllCommunities(network):
    comList = list()
    while network:
        nodeList = list(network.keys())
        ff = 0
        tempCom = [nodeList[0]]
        visited = {nodeList[0]}

        while ff < len(tempCom):
            for subNode in network[tempCom[ff]]:
                if subNode not in visited:
                    visited.add(subNode)
                    tempCom.append(subNode)
            network.pop(tempCom[ff])
            ff = ff + 1
        comList.append((len(tempCom), sorted(tempCom)))
    comList.sort(key=lambda line: [line[0], line[1]])
    return comList

def Q(community, A,  degree, m):
    result = 0
    mm = 2*m
    for comNum in community.keys():
        tempCom = community[comNum]
        for j in tempCom:
            for i in tempCom:
                if j != i:
                    if tuple(sorted((j, i))) in A:
                        AijValue = 1
                    else:
                        AijValue = 0
                    result += AijValue - degree[j]*degree[i]/mm
    return result/mm