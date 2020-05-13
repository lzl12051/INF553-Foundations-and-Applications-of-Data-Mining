from sys import argv
from queue import Queue as queue
from copy import deepcopy
import lib553
from lib553 import Node
from lib553 import Q

sc = lib553.START_SPARK('A4 Task2')
edgeThreshold = int(argv[1])
dataFile = argv[2]  # 'datasets/ub_sample_data.csv'
btwoutputFile = argv[3]  # 'task2_1_ans'
communityOutputFile = argv[4]

rddData = sc.textFile(dataFile).map(lambda line: lib553.Map_transCSVToTuple(
    line, ['user_id', 'business_id'])).filter(lambda line: line if line != None else 0)

rddUserAndBus = rddData.mapValues(
    lambda v: {v}).reduceByKey(lambda x, y: x.union(y))

userProfile = rddUserAndBus.collectAsMap()


def Map_findRelationship(line, data):
    user_id = line[0]
    busSet = line[1]
    resultList = []
    for user in data:
        if user != user_id:
            if len(busSet & data[user]) >= edgeThreshold:
                resultList.append((user_id, user))
    if resultList == []:
        return (None,)
    return resultList


rddRelationship = rddUserAndBus.map(
    lambda line: Map_findRelationship(line, userProfile))
# print(rddRelationship.take(10))
rddNodes = rddRelationship.filter(lambda line: 0 if line[0] is None else 1).map(
    lambda line: (line[0][0],))
allNodes = rddNodes.map(lambda line: line[0]).sortBy(
    lambda line: line).collect()
nodesNumDict = dict(zip(sorted(allNodes), range(len(allNodes))))
numNodesDict = dict(zip(range(len(allNodes)), sorted(allNodes)))
del allNodes
# edges are undirected, so it has 2 edges bewteen every two nodes
rddNodes = rddNodes.map(lambda line: (nodesNumDict[line[0]]))
rddRelationship = rddRelationship.flatMap(
    lambda line: line).filter(lambda line: line is not None).map(
    lambda line: (nodesNumDict[line[0]], nodesNumDict[line[1]]))
rddNetwork = rddRelationship.mapValues(lambda v: {v}).reduceByKey(
    lambda x, y: x.union(y))
networkDict = rddNetwork.collectAsMap()


def GN(root, graph):
    # some functions
    def calcWeight(x, y):
        # print(x,y)
        return x / y

    # some initial settings
    visitedSet = set()

    # build the rootNode
    nextNodesQueue = queue()
    rootNode = Node(root)

    # set the initial condition
    visitedSet.add(rootNode.nodeId)
    nextNodesQueue.put(rootNode)
    rootNode.firstValue = 1

    # all nodes
    nodeDict = {}
    nodeDict[rootNode.nodeId] = rootNode

    while not nextNodesQueue.empty():
        nextNode = nextNodesQueue.get()
        for node in graph[nextNode.nodeId]:
            # build a subNode
            subNode = Node(node)
            if node not in visitedSet:

                # add the parent node to the subNode
                subNode.addParent(nextNode.nodeId)
                subNode.setLevel(nextNode.level + 1)
                subNode.increaseFirstValue(nextNode.firstValue)

                # mark as visited
                visitedSet.add(subNode.nodeId)

                # add the subNode to queue
                nextNodesQueue.put(subNode)
                nodeDict[subNode.nodeId] = subNode

            else:
                # to deal with the node which has more than 1 parent node
                subNode = nodeDict[node]
                if subNode.level > nextNode.level:
                    subNode.addParent(nextNode.nodeId)
                    subNode.increaseFirstValue(nextNode.firstValue)

    # Build a dict by level
    levelDict = {}
    for node in nodeDict.values():
        try:
            levelDict[node.level].add(node.nodeId)
        except KeyError:
            levelDict[node.level] = {node.nodeId}

    # get the deepest layer number
    deepestLevel = max(levelDict.keys())
    # from deepest to 0
    betweenessList = []
    for l in range(deepestLevel, 0, -1):
        currentLayerNodes = levelDict[l]
        for n in currentLayerNodes:

            node = nodeDict[n]
            parentNodeList = list(node.parent)
            parentNodeFVList = []
            for p in parentNodeList:
                parentNodeFVList.append(nodeDict[p].firstValue)
            parentSum = sum(parentNodeFVList)
            # print(parentNodeFVList)
            parentNodeWeightList = list(
                map(lambda x: calcWeight(x, parentSum), parentNodeFVList))
            # print(parentNodeWeightList)
            for w in range(len(parentNodeWeightList)):
                nodeId = parentNodeList[w]
                newValue = float(node.secondValue * parentNodeWeightList[w])
                # print(node.secondValue,parentNodeWeightList[w],newValue)
                nodeDict[nodeId].increaseSecondValue(newValue)
                betweenessList.append((tuple(sorted((node.nodeId, nodeId))), newValue))
                # print(nodeDict[nodeId].nodeId, nodeDict[nodeId].secondValue)
    # print(list(parentNodeWeightList))
    # return nodeDict
    return betweenessList


rddBetweenness = rddNetwork.flatMap(lambda line: GN(line[0], networkDict)).reduceByKey(
    lambda x, y: x + y).mapValues(lambda v: v / 2).sortBy(lambda line: (-line[1], line[0]))
finalResult = rddBetweenness.collect()
with open(btwoutputFile, 'w') as f:
    for line in finalResult:
        outstr = '(' + numNodesDict[line[0][0]] + ', ' + numNodesDict[line[0][1]] + '), ' + str(line[1])
        f.write(outstr)
        f.write('\n')


# T22 start

def findAllClusters(graph):
    def geneCluster(root, graph):
        visitedSet = set()

        # build the rootNode
        nextNodesQueue = queue()
        rootNode = Node(root)

        # set the initial condition
        visitedSet.add(rootNode.nodeId)
        nextNodesQueue.put(rootNode)

        # all nodes
        nodeDict = {}
        nodeDict[rootNode.nodeId] = rootNode

        while not nextNodesQueue.empty():
            nextNode = nextNodesQueue.get()
            for node in graph[nextNode.nodeId]:

                # build a subNode
                subNode = Node(node)
                if node not in visitedSet:
                    # mark as visited
                    visitedSet.add(subNode.nodeId)

                    # add the subNode to queue
                    nextNodesQueue.put(subNode)
                    nodeDict[subNode.nodeId] = subNode
        return visitedSet

    globalVisited = set()
    communityDict = {}
    communityCounter = 0
    for node in networkDict.keys():
        if not node in globalVisited:
            # print(node)
            tempCom = geneCluster(node, networkDict)
            communityDict[communityCounter] = tempCom
            communityCounter += 1
            globalVisited = globalVisited | tempCom
    return communityDict


m = rddBetweenness.count()

A = set(rddBetweenness.keys().collect())

D = rddBetweenness.keys().flatMap(lambda line: [(line[0], 1), (line[1], 1)]).reduceByKey(
    lambda x, y: x + y).sortByKey().collectAsMap()

cuttedEdge = rddBetweenness.sortBy(
    lambda line: line[1], ascending=False).first()
# print(cuttedEdge)

# networkDictCopy = deepcopy(networkDict)
# print(networkDictCopy[cuttedEdge[0][0]])


try:
    networkDict[cuttedEdge[0][0]].remove(cuttedEdge[0][1])
except KeyError:
    print(1)
try:
    networkDict[cuttedEdge[0][1]].remove(cuttedEdge[0][0])
except KeyError:
    print(2)

# print(networkDictCopy[cuttedEdge[0][0]])
tempQ = Q(findAllClusters(networkDict), A, D, m)
QValue = tempQ

while tempQ >= QValue:
    lastNetwork = deepcopy(networkDict)
    rddNewNetwork = sc.parallelize(networkDict.items())
    # print(rddNewNetwork.take(1))
    rddBetweenness = rddNewNetwork.flatMap(lambda line: GN(line[0], networkDict)).reduceByKey(
        lambda x, y: x + y).mapValues(lambda v: v / 2).sortBy(lambda line: -line[1])

    cuttedEdge = rddBetweenness.first()
    # print(rddBetweenness.take(5))

    # print('CE:', cuttedEdge)

    networkDict[cuttedEdge[0][0]].remove(cuttedEdge[0][1])

    networkDict[cuttedEdge[0][1]].remove(cuttedEdge[0][0])
    tempQ = Q(findAllClusters(networkDict), A, D, m)
    if tempQ >= QValue:
        QValue = tempQ
    # print(tempQ, QValue)

outputList = lib553.findAllCommunities(lastNetwork.copy())

with open(communityOutputFile, 'w') as f:
    for line in outputList:
        outstr = ', '.join(['\'' + numNodesDict[i] + '\'' for i in line[1]])
        f.write(outstr)
        f.write('\n')
