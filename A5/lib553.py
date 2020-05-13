# This is a lib of inf553 assignments
import json
import logging
import os
import time
from math import sqrt
from random import sample
import random

import matplotlib.pyplot as plt


def timer(f):
    def wrapper(*args, **kwargs):
        startTime = time.time()
        # print(*args)
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
    confSpark = SparkConf().setAppName(name).setMaster(
        master).set('spark.driver.memory', '4G').set(
        'spark.executor.memory', '4G')

    sc = SparkContext.getOrCreate(confSpark)
    # os.environ["PYSPARK_SUBMIT_ARGS"] = (
    # "--packages graphframes:graphframes:0.8.0-spark2.4-s_2.11")
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


def Q(community, A, degree, m):
    result = 0
    mm = 2 * m
    for comNum in community.keys():
        tempCom = community[comNum]
        for j in tempCom:
            for i in tempCom:
                if j != i:
                    if tuple(sorted((j, i))) in A:
                        AijValue = 1
                    else:
                        AijValue = 0
                    result += AijValue - degree[j] * degree[i] / mm
    return result / mm


def findAllCommunities(network):
    comList = list()
    while bool(network):
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

# 图数据绘图
def plotGraph(cutted_edge, filename='', dpi=300, nodeSize=5, nodeColor='r',
              nodeShape='o', withLabels=0, edgeWidth=0.1, fontSize=12,
              fontColor='black', figSize=10):
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


def Map_transCSVToTuple(line: str, headers: list = []):
    # headers = headers.split(",")
    line = line.split(",")
    outputList = []
    if headers:
        for i in range(len(line)):
            if not line[i] in headers:
                outputList.append(line[i])
    else:
        for i in range(len(line)):
            outputList.append(line[i])
    return outputList


def Map_transToDataPoint(line):
    x = Datapoint(line)
    return x


# 聚类数据绘图
def drawClusters(km, testDataDict):
    # Configure the logging system
    import logging
    logging.basicConfig(
        filename='ClusterDrawer.log',
        level=logging.DEBUG
    )

    def getClusterContent(clusterData):
        result = []
        for cId, c in clusterData.items():
            result.append([cId, list(c.allPoints.keys())])
        return result

    def getClusterCentDict(clusterDict):
        resultDict = {}
        for cId, c in clusterDict.items():
            resultDict[cId] = c.centroid

        return resultDict

    kmCent = getClusterCentDict(km)
    kmContent = getClusterContent(km)

    plt.figure(figsize=(15, 15))
    ax = plt.subplot(projection='3d')
    for x in kmContent:
        colorList = ['r', 'b', 'g', 'y', 'c', 'm', 'orange',
                     'lime', 'deepskyblue', 'hotpink', 'black']
        cId = x[0]
        c = x[1]
        for p in c:
            ax.scatter(testDataDict[p][0],
                       testDataDict[p][1],
                       testDataDict[p][2],
                       s=5,
                       c=colorList[cId])
    for x in kmCent.keys():
        if x != -1:
            ax.scatter(kmCent[x][0], kmCent[x][1], kmCent[x]
            [2], c='black', marker='x', s=30)
    # ax.axis('equal')
    plt.show()


# 计算欧氏距离
def eucDis(point1, point2):
    sumOfSqr = 0
    for i in range(len(point1)):
        sumOfSqr += pow((point1[i] - point2[i]), 2)
    return sqrt(sumOfSqr)
    # return sqrt(sum((x-y)**2 for (x,y) in zip(point1, point2)))

def getClusterCentDict(clusterDict):
    resultDict = {}
    for cId, c in clusterDict.items():
        resultDict[cId] = c.centroid
    return resultDict

def showClusterContent(clusterDict):
    for cluster in clusterDict.values():
        print(cluster.id, sorted(cluster.allPoints.keys()))

def KMeans(data, k, offset=0,  outlier = 1, coefR = 1.5):
    """

    :rtype: dict
    :param data: 原生数据点[id, d1, d2, d3, ... , dn]
    :param k: 聚类数量
    :param offset: 聚类输出编号的偏移量
    :param coefR: 这个是用来检测outlier的半径系数，默认为1.5
    :param outlier: 如果设置为1，就会检测outlier并且返回id为-1的类
    :return: 返回k个Cluster类和一个id为-1的包含outlier的类
    """
    # 转化数据到datapoint对象！！！
    # 如果在这里转的话就不会出问题
    sampleDataList = []
    counter = 0
    for dp in data:
        sampleDataList.append((counter, Datapoint(dp)))
        counter += 1

    # 这里是选择最远点作为聚类中心的算法
    kk = k - 1
    clusterCounter = 1 + offset
    # 随便取一个点
    randomPoint = sample(sampleDataList, 1)
    clusterDict = {0 + offset: Cluster(0 + offset, randomPoint[0][1])}
    # 把这个点的坐标加入已选择的列表
    selectedPoints = [randomPoint[0][1].value]

    while kk:
        nextSelect = None
        maxDis = float('-inf')
        for p in sampleDataList:
            tempDis = p[1].farthestPointCalc(selectedPoints)
            if tempDis > maxDis:
                maxDis = tempDis
                nextSelect = p[1]

        selectedPoints.append(nextSelect.value)
        # pprint(selectedPoints)
        clusterDict[clusterCounter] = Cluster(
            clusterCounter, nextSelect)
        clusterCounter += 1
        kk -= 1
    # 最远点算法结束了！
    ########################

    # 计算所有初始聚类的中心
    clusterCentDict = getClusterCentDict(clusterDict)

    # Kmeans的主要循环
    changeFlag = 1
    iterNum = 0
    while changeFlag:
        changeFlag = 0

        for x in sampleDataList:
            point = x[1]
            logging.debug('\nupdating point: %d', point.id)
            # 更新每个数据点的所属集合（计算离自己最近的clusterCent并且修改自己的cluster标记
            point.updateCluster(clusterCentDict)
            # 判断这个点是否发生变动
            if point.isChanged:
                logging.debug('Point %d change cluster from %d to %d \n',
                              point.id, point.lastCluster, point.cluster)
                # 如果本次有点发生了移动，那么就把这个flag加1，表示聚类算法还没有达到稳定
                # 同时也能知道本次有多少点发生了移动
                changeFlag += 1
                # 将这个点加入新的集合
                clusterDict[point.cluster].addPoint(point)
                # 将这个点从旧的集合移除
                if point.lastCluster != -1:
                    # 删除的时候只需要传递点的Id不需要把整个点传进去，只传id就好
                    clusterDict[point.lastCluster].removePoint(point.id)

        # 这个计数器用来算到底迭代了几次
        iterNum += 1
        # print('Iter:', iterNum, "Change:", changeFlag)

        # 下边2行代码是为了防止一些极端情况，比如会有一些卡在两个cluster中间的点反复横跳，
        # 造成算法无法结束
        if changeFlag <= int(len(data) * 0.005):
            changeFlag = 0

        # 如果迭代了20次还没有结果那就强制退出，值随便设的
        if iterNum > 25:
            changeFlag = 0

        # 把新的ClusterCent更新到字典里
        clusterCentDict = getClusterCentDict(clusterDict)
    if outlier:
        # 检测outliers并且移除，构成RS
        tempOutlierDict = {}
        for cId, c in clusterDict.items():
            tempOutlierDict.update(c.detectOutlier(coefR))
            # 把clusterDict更新成新的去掉outlier之后的cluster对象
            clusterDict[cId] = c

        # 这里是想把RS也作为一个正常的cluster对象，但是ID是-1，然后先从outliers里随机选了一个点，生成一个新的cluster
        if tempOutlierDict:
            outlierCluster = Cluster(-1, tempOutlierDict.pop(random.choice(list(tempOutlierDict.keys()))))

            # 用一个for把所有的outlier点都加入这个RS Cluster
            for point in tempOutlierDict.values():
                outlierCluster.addPoint(point)

            # 写入这个id是-1的outlier cluster到最终的clusterDict
            clusterDict[-1] = outlierCluster

    print('KMeans IterNum:', iterNum)
    return clusterDict

###############################
#         Classes             #
###############################

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
        return resultDict


# define the dataPoint class
class Datapoint:
    name = 'dataPoint'

    def __init__(self, data):
        self.id = int(data[0])
        self.value = data[1:]
        self.dim = len(self.value)
        self.lastCluster = -1
        self.cluster = -1
        self.isChanged = 0

    def __len__(self):
        return self.dim

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, data):
        _ = [data['id']] + data['value']
        return self.__init__(_)

    def __str__(self):
        return 'id:' + str(self.id) + ' value:' + str(self.value)

    def updateCluster(self, clusterCentDict):

        distance = float('inf')
        tempCluster = 0
        for clusterId, clusterCent in clusterCentDict.items():
            tempDistance = eucDis(self.value, clusterCent)
            if tempDistance < distance:
                distance = tempDistance
                tempCluster = clusterId

        if self.cluster == -1:
            self.cluster = tempCluster
            self.isChanged = 1
            # logging.debug('Point First Time assign to cluster %d',
            # self.cluster)
        elif tempCluster != self.cluster:
            self.lastCluster = self.cluster
            self.cluster = tempCluster
            self.isChanged = 1
        else:
            self.isChanged = 0
        # logging.debug('Point %d cluster:%d, lastCluster:%d',
        #               self.id, self.cluster, self.lastCluster)

    def farthestPointCalc(self, pointList: list):
        # 计算传入的点，自身到其中最近的一个点的距离
        # 这里的东西都给你讲过了！
        # 我就不写注释了
        minD = float('inf')
        for p in pointList:
            if p == self.value:
                return -1
            tempDis = eucDis(p, self.value)
            if tempDis <= minD:
                minD = tempDis
        return minD

    # 计算马氏距离的#￥%……&
    def __mahDis(self, centroidIn: list, STDDIn: list):
        dis = 0
        for i in range(self.dim):
            upperNum = self.value[i] - centroidIn[i]
            if STDDIn[i] != 0:
                dis += pow(upperNum / STDDIn[i], 2)
            else:
                dis += pow(upperNum, 2)
        return sqrt(dis)

    # 这个是用来判断这个点是否应该归属于一个DS
    def assignToDS(self, DSDictIn: dict, alpha: float):
        mahThreshold = alpha * sqrt(self.dim)
        tempMinDis = float('inf')
        for DSId in DSDictIn.keys():
            ds = DSDictIn[DSId]
            mahDis = self.__mahDis(ds.centroid, ds.STDD)
            if mahDis < tempMinDis:
                # print(mahDis, mahThreshold)
                if mahDis <= mahThreshold:
                    self.cluster = DSId
        return (self.id, self.cluster, self)


class Cluster:
    """
    This is cluster class
    """
    name = 'cluster'

    def __init__(self, id, iniCent):
        self.id = id
        self.allPoints = {iniCent.id: iniCent}
        self.numOfPoints = 1
        self.dim = iniCent.dim
        self.updateCentroid()

    def addPoint(self, point, updateCent = 1):
        self.allPoints[point.id] = point
        self.numOfPoints = len(self.allPoints)
        if updateCent:
            self.updateCentroid()

    def removePoint(self, pointId, updateCent=1):
        """

        :param pointId: Input point.id(int) not the point object.
        :return: No return value, this will update the centroid of the cluster
        """
        self.allPoints.pop(pointId, None)
        self.numOfPoints = len(self.allPoints)
        if updateCent:
            self.updateCentroid()

    # 根据自身的所有点来更新自己的质心数据
    def updateCentroid(self):

        def calcMean(x):
            return x / self.numOfPoints

        # 列表生成器，其实就是一个for语句，但是第一个0，就是要输出到这个列表里的结果
        sumList = [0 for i in range(self.dim)]
        # 把每个点的每个维度加起来
        for point in self.allPoints.values():
            sumList = map(lambda x, y: x + y, sumList, point.value)

        # 调用calcMean函数把每个列表的元素都除以点的数量，算出质心来
        self.centroid = list(map(calcMean, sumList))

    # 检测outlier
    def detectOutlier(self, scaleCoe: float):
        """
        输入一个半径系数，大于这个系数*半径的点会被标记为离群点并且输出
        """
        outlierDict = {}
        distenceSum = 0
        # 把所有点到质心的距离和算出来
        for point in self.allPoints.values():
            distenceSum += eucDis(point.value, self.centroid)
        # 距离和除以点数，就是平均距离了
        meanRadius = distenceSum / self.numOfPoints
        # 看哪个点的距离大于这个平均距离乘以某个系数，系数是我自己试出来的。。。
        for pId in list(self.allPoints.keys()):
            point = self.allPoints[pId]
            if eucDis(point.value, self.centroid) > meanRadius * scaleCoe:
                # 记录到outlier的字典里
                outlierDict[pId] = point
                # 如果确定这个点是outlier就要把他从集群里删掉！
                self.removePoint(pId)
        # 送出一个outlier的字典，供外部处理
        return outlierDict

    def originalData(self):
        outList = []
        for pId, point in self.allPoints.items():
            outList.append([pId] + point.value)
        return outList


class DiscardSet:
    def __init__(self, clusterIn: Cluster):
        """

        :param clusterIn:就是传入一个cluster对项，把他转成DS Cluster对象
        """
        self.id = int(clusterIn.id)
        self.N = clusterIn.numOfPoints
        self.dim = clusterIn.dim
        # 以下就是每个DS的那个概要信息，好像叫：statistics
        self.SUM = [0 for i in range(self.dim)]
        self.SUMSQ = [0 for i in range(self.dim)]
        # 这是每个维度的标准差，提前算好存着比较方便
        self.STDD = [0 for i in range(self.dim)]
        # 生成初始化的上述信息
        self.__iniSUMAndSUMSQ(clusterIn)
        self.centroid = [0 for i in range(self.dim)]
        # 生成这个DS的质心！
        self.__updateCent()
        # 把所有的点从集群那里继承过来
        self.allPointsId = set(clusterIn.allPoints.keys())

    # 初始化那些信息
    def __iniSUMAndSUMSQ(self, clusterIn: Cluster):
        for point in clusterIn.allPoints.values():
            for i in range(self.dim):
                self.SUM[i] = point.value[i] + self.SUM[i]
                self.SUMSQ[i] = pow(point.value[i], 2) + self.SUM[i]
                self.STDD[i] = sqrt(
                    abs(self.SUMSQ[i] / self.N - pow(self.SUM[i] / self.N, 2)))

    def __updateCent(self):
        self.centroid = [i / self.N for i in self.SUM]

    # 这个函数可以批量更新DS的统计学概要信息，还没有完工
    def updateSUMAndSUMSQ(self, newPoints: set,
                          newSUM: list, newSUMSQ: list):
        self.N += len(newPoints)
        for i in range(self.dim):
            self.SUM[i] += newSUM[i]
            self.SUMSQ[i] += newSUMSQ[i]
            self.STDD[i] = sqrt(
                abs(self.SUMSQ[i] / self.N - pow(self.SUM[i] / self.N, 2)))
        self.allPointsId = self.allPointsId.union(newPoints)
        self.__updateCent()
        return self

    def merge(self, cs):
        return self.updateSUMAndSUMSQ(cs.allPointsId, cs.SUM, cs.SUMSQ)

    def __mahDis(self, centroidIn: list, STDDIn: list):
        dis = 0
        for i in range(self.dim):
            upperNum = self.centroid[i] - centroidIn[i]
            if STDDIn[i] != 0:
                dis += pow(upperNum / STDDIn[i], 2)
            else:
                dis += pow(upperNum, 2)
        return sqrt(dis)

    def calcMahDisBetwDS(self, CSDict, alpha):
        mahThreshold = alpha * sqrt(self.dim)
        minDis = float('inf')
        tempId = -1
        for csId, cs in CSDict.items():
            if csId != self.id:
                tempDis = self.__mahDis(cs.centroid, cs.STDD)
                if tempDis <= mahThreshold and tempDis < minDis:
                    minDis = tempDis
                    tempId = csId

        return (self.id, tempId, minDis)
