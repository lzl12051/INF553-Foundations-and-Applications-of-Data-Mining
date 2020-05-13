import csv
import json
import os
import random
from sys import argv

import lib553

# os.environ['PYSPARK_PYTHON']='/usr/local/bin/python3.6'
# os.environ['PYSPARK_DRIVER_PYTHON']='/usr/local/bin/python3.6'
random.seed(1205)

inputPath = argv[1]
fileList = os.listdir(inputPath)

n_clusters = int(argv[2])
clusterOutput = argv[3]
interOutput = argv[4]

sc = lib553.START_SPARK("A5")

rddData = sc.textFile(inputPath + fileList[0]).map(
    lambda line: lib553.Map_transCSVToTuple(line)).map(lambda line: [float(i) for i in line])

allPointsSoFar = rddData.count()
print(allPointsSoFar)
# 这里完成取样第一个file chunk的1%，大概1300个点
rddSampleData = rddData.sample(False, 0.01, 1205)
sampleData = rddSampleData.collect()
sampleDataNum = len(sampleData)
print(f'Take {sampleDataNum} sample points')

# 这里先把取样的数据转成dataPoint对象
rddSampleData = rddSampleData.map(lambda line: (
    int(line[0]), lib553.Map_transToDataPoint(line)))
# 把所有数据都转成dataPoint对象
rddData = rddData.map(lambda line: (
    int(line[0]), lib553.Map_transToDataPoint(line)))
# 去掉那些已经被取样过的数据点
rddData = rddData.subtractByKey(rddSampleData)
print('First chunk remains:', rddData.count())

nof_cluster_discard = n_clusters

# 第一次运行KMeans
initialKMeansResult = lib553.KMeans(sampleData, n_clusters, 0, 1, 1.6)

DSDict = {}
CSDict = {}
RSDict = {}
# 如果第一次KMeans有产生RS点，就放进RSDict
try:
    for pointData in initialKMeansResult[-1].originalData():
        RSDict[pointData[0]] = pointData
except KeyError:
    print('No outlier detected in initial KMeans')

cluster: lib553.Cluster
# 把第一次KMeans的聚类结果生成DS放进DSDict
for clusterID, cluster in initialKMeansResult.items():
    if clusterID != -1:
        DSDict[clusterID] = lib553.DiscardSet(cluster)

# 如果RS的点比较多，就生成CS
if len(RSDict) > 6 * n_clusters:
    rsKMeansResult = lib553.KMeans(RSDict.values(), 3 * n_clusters, n_clusters)
    # 既然重新运行了KMeans，旧的RS也就没有什么用处了
    # 直接重新声明一个新的空RSDict覆盖掉
    RSDict = {}
    for clusterID, cluster in rsKMeansResult.items():
        # 如果有outlier，就放进RSDict
        if clusterID == -1:
            for pointData in cluster.originalData():
                RSDict[pointData[0]] = pointData
        if cluster.numOfPoints == 1:
            RSDict[cluster.originalData()[0][0]] = cluster.originalData()[0]
        if cluster.numOfPoints != 1:
            # 如果不是outlier cluster，就放进CS
            CSDict[clusterID] = lib553.DiscardSet(cluster)


##################
#      BFR       #
##################

def Map_computeSUMAndSUMSQ(line):
    # line[1][1][0]应该是个Datapoint对象
    resultSUM = [0 for i in range(line[1][1][0].dim)]
    resultSUMSQ = [0 for i in range(line[1][1][0].dim)]
    resultPointsSet = set()
    for point in line[1][1]:
        resultSUM = list(map(lambda x, y: x + y, resultSUM, point.value))
        resultSUMSQ = list(map(lambda x, y: x + pow(y, 2), resultSUMSQ, point.value))
        resultPointsSet.add(point.id)
    return (line[0], resultPointsSet, resultSUM, resultSUMSQ)


headerList = ['round_id', 'nof_cluster_discard', 'nof_point_discard',
              'nof_cluster_compression', 'nof_point_compression', 'nof_point_retained']
# 写入中间过程的CSV文件头
with open(interOutput, 'w') as inter:
    out = csv.writer(inter)
    out.writerow(headerList)

round_id = 0  # 计算这是第几个file chunk
alpha = 2  # 这个是用来做马氏距离的系数
# 开始BFR过程
while round_id < len(fileList):
    print('Round:', round_id)
    # 如果不是第一轮，那就重新载入数据。并且把数据点转换成dataPoint对象
    if round_id != 0:
        rddData = sc.textFile(inputPath + fileList[round_id]).map(
            lambda line: lib553.Map_transCSVToTuple(line)).cache()
        allPointsSoFar += rddData.count()
        rddData = rddData.map(
            lambda line: [float(i) for i in line]).map(
            lambda line: (int(line[0]), lib553.Map_transToDataPoint(line)))

    ##################
    #       DS       #
    ##################

    # 每个数据点检查是否与DS接近
    rddCheckDS = rddData.map(lambda line: line[1].assignToDS(DSDict, alpha)).cache()  # 这个cache十分重要，可以防止SPARK去重算结果导致结果出错
    # 筛选出能够分配到DS的点，存入变量DS
    DS = rddCheckDS.filter(lambda line: line[1] != -1)
    # print('Spark DS point', DS.count())
    DS = DS.map(lambda line: (line[1], ([line[0]], [line[2]]))).reduceByKey(
        lambda x, y: (x[0] + y[0], x[1] + y[1])).map(Map_computeSUMAndSUMSQ).collect()

    # 把归类到DS的点融合进DS，并且更新DS的信息
    # print('Merging points to DS...')
    for dsPoint in DS:
        DSDict[dsPoint[0]] = DSDict[dsPoint[0]].updateSUMAndSUMSQ(dsPoint[1], dsPoint[2], dsPoint[3])
        # nof_point_discard += len(dsPoint[1])

    ##################
    #       CS       #
    ##################

    # 经过DS筛选后的数据再检查是否与CS接近
    rddCheckCS = rddCheckDS.filter(lambda line: line[1] == -1)
    # print(rddCheckCS.collect())
    # print('rddCheckCS :', rddCheckCS.count())
    rddCheckCS = rddCheckCS.map(
        lambda line: line[2].assignToDS(CSDict, alpha)).cache()  # 同样用来防止重算结果导致出错

    # 与CS接近的点存到变量CS
    CS = rddCheckCS.filter(lambda line: line[1] != -1)
    # print('Spark CS point', CS.count())
    CS = CS.map(lambda line: (line[1], ([line[0]], [line[2]]))).reduceByKey(
        lambda x, y: (x[0] + y[0], x[1] + y[1])).map(Map_computeSUMAndSUMSQ).collect()

    # 把归类到CS的点融合进CS，并且更新CS的信息
    print('Merging points to CS...')
    for csPoint in CS:
        CSDict[csPoint[0]] = CSDict[csPoint[0]].updateSUMAndSUMSQ(csPoint[1], csPoint[2], csPoint[3])


    ##################
    #       RS       #
    ##################

    # 经过CS筛选的点，转变成原数据点后存到变量tempRSList，放进RSDict
    tempRSList = rddCheckCS.filter(lambda line: line[1] == -1)
    print('Spark RS point', tempRSList.count())
    tempRSList = tempRSList.map(
        lambda line: [line[2].id] + line[2].value).collect()
    for pointData in tempRSList:
        RSDict[pointData[0]] = pointData

    ##################
    #    RS -> CS    #
    ##################

    # 因为每次KMeans给出的结果，cluster编号都是从0开始计数
    # 为了避免冲突，这里设定一个offset，让KMeans从offset+1开始计数
    if CSDict:
        csIdOffset = max(list(CSDict.keys())) + 1
    else:
        csIdOffset = 0

    if len(RSDict) > 6 * n_clusters:
        print(f'running KMeans on {len(RSDict)} RS points.')
        rsKMeansResult = lib553.KMeans(RSDict.values(), 3 * n_clusters, csIdOffset)
        # 既然重新运行了KMeans，旧的RS也就没有什么用处了，直接重新声明一个新的空RSDict覆盖掉
        RSDict = {}
        for clusterID, cluster in rsKMeansResult.items():
            # 如果有outlier，就放进RSDict
            if clusterID == -1:
                for pointData in cluster.originalData():
                    RSDict[pointData[0]] = pointData
            if cluster.numOfPoints == 1:
                RSDict[cluster.originalData()[0][0]] = cluster.originalData()[0]
            if cluster.numOfPoints != 1:
                # 如果不是outlier cluster，就放进CS
                CSDict[clusterID] = lib553.DiscardSet(cluster)

    ##################
    #   CS Merging   #
    ##################

    # 融合相近的CS，先判断是否存在CS
    if CSDict:
        mergingFlag = 1
        while mergingFlag:
            csDisList = []
            for cId, cs in CSDict.items():
                csDisList.append(cs.calcMahDisBetwDS(CSDict, alpha))
            minDis = min(csDisList, key=lambda line: line[2])
            if minDis[2] == float('inf'):
                mergingFlag = 0
            else:
                firstCSId = minDis[0]
                secondCSId = minDis[1]
                CSDict[firstCSId] = CSDict[firstCSId].merge(CSDict[secondCSId])
                CSDict.pop(secondCSId)

    ########################
    #   CS Merge into DS   #
    ########################

    removedCS = []
    for csId, CS in CSDict.items():
        tempResult = CS.calcMahDisBetwDS(DSDict, alpha)
        if tempResult[2] != float('inf'):
            DSDict[tempResult[1]] = DSDict[tempResult[1]].merge(CS)
            removedCS.append(csId)

    for csId in removedCS:
        del CSDict[csId]

    nof_point_discard = 0
    ds: lib553.DiscardSet
    for ds in DSDict.values():
        nof_point_discard += ds.N

    nof_point_compression = 0
    cs: lib553.DiscardSet
    for cs in CSDict.values():
        nof_point_compression += cs.N

    nof_point_retained = len(RSDict)

    nof_cluster_compression = len(CSDict)
    pointsSum = nof_point_retained + nof_point_discard + nof_point_compression
    nof_point_discard += (allPointsSoFar - pointsSum)
    print(f'round:{round_id}, sum:{nof_point_retained + nof_point_discard + nof_point_compression}')
    with open(interOutput, 'a') as inter:
        out = csv.writer(inter)
        out.writerow([round_id + 1, nof_cluster_discard, nof_point_discard,
                      nof_cluster_compression, nof_point_compression, nof_point_retained])
    round_id += 1

# 输出最后结果
clusterResult = {}
for dsId, ds in DSDict.items():
    for point in ds.allPointsId:
        clusterResult[str(point)] = dsId

for csId, cs in CSDict.items():
    for point in cs.allPointsId:
        clusterResult[str(point)] = -1

for p in RSDict.values():
    clusterResult[str(p[0])] = -1

with open(clusterOutput, 'w') as out:
    json.dump(clusterResult, out)

print("Num of data point:", len(clusterResult),
      'Should be:', 689617, "lost:", 689617 - len(clusterResult))
