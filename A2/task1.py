import time
from pprint import pprint
from sys import argv

# PATH = os.getcwd()
# csvFile = PATH + "/datasets/small2.csv"

caseNum = int(argv[1])
sup = int(argv[2])
csvFile = argv[3]

startTime = time.time()


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


# @timer
def START_SPARK():
    from pyspark import SparkContext
    from pyspark import SparkConf

    confSpark = SparkConf().setAppName("Task1").setMaster(
        "local[*]")
    sc = SparkContext.getOrCreate(confSpark)
    sc.setLogLevel('ERROR')

    return sc


def getPartitionItemNum(rddIn):
    partitionItemNum = rddIn.glom().map(
        len).collect()  # get length of each partition
    return partitionItemNum


# Start spark
sc = START_SPARK()


def Map_transCSVToTuple(line: str, headers=""):
    headers = headers.split(",")
    line = line.split(",")
    outputList = []
    for i in range(len(line)):
        if not line[i] in headers:
            outputList.append(line[i])
    return outputList


def Map_emitKVPairFromCSV(line: list):
    return (line[0], line[1])


def Map_businessAsBasket(line):
    outputList = []
    for business in line[1]:
        outputList.append((business, [line[0]]))
    return outputList


def Map_emitKVPair(line):
    return (line, 1)


def Map_countFreqSets(lineIn, candidates: list):
    outputList = []
    for candidate in candidates:
        if set(candidate[0]).issubset(set(lineIn[1])):
            outputList.append((candidate[0], 1))
    return outputList


def partitioner(line):
    return hash(line)


def Reduce_joinByUser(line1, line2):
    # If find the same value then ignore it
    if not line2[0] in line1:
        [line1].append(line2[0])
    return line1


def Reduce_joinByBusiness(line1: list, line2: list):
    if not line2[0] in line1:
        line1.append(line2[0])
    return line1


def Map_aPriori_boosted(partition: dict, sup: int, phase: int,
                        lastResultOfFreqSets=[]):
    partition = tuple(partition)

    def combinations(iterable, r):
        # This function is COPYED from the stdlib: itertools
        # And I have done some modifying to make it works well with my code
        # combinations('ABCD', 2) --> AB AC AD BC BD CD
        # combinations(range(4), 3) --> 012 013 023 123
        pool = tuple(iterable)
        n = len(pool)
        if r > n:
            return
        indices = list(range(r))
        yield tuple(sorted(tuple(pool[i] for i in indices)))
        while True:
            for i in reversed(range(r)):
                if indices[i] != i + n - r:
                    break
            else:
                return
            indices[i] += 1
            for j in range(i + 1, r):
                indices[j] = indices[j - 1] + 1
            yield tuple(sorted(tuple(pool[i] for i in indices)))

    def getFreqItemsOfPartition():
        # counting every items in the partition
        # print("counting items in the partition...")
        itemsCountingDict = {}

        for key in partition:
            for item in key[1]:
                # print(item)
                if item in itemsCountingDict:
                    itemsCountingDict[item] += 1
                else:
                    itemsCountingDict[item] = 1
        # print("FreqItems:",itemsCountingDict)
        return itemsCountingDict

    def getCombinationsFromFreqsets(freqSets: list, size: int):
        if size != 2:
            newFreqItems = set()
            for freqSet in freqSets:
                newFreqItems.add(freqSet)
            result = list(combinations(list(newFreqItems), size))
            result.sort()
        else:
            result = combinations(freqSets, size)
        # print("output combination:",list(result),"size:",size)
        return result

    def countEverySet(freqSetsCandidates: list):
        # print("freqSetsCandidates:",freqSetsCandidates)
        freqSetsCountingDict = {}
        for freqSet in freqSetsCandidates:
            # print(freqSet)
            _freqSet = set(freqSet)
            # print(partition)
            for key in partition:
                # print("Keys:",key)
                if _freqSet.issubset(key[1]):
                    # freqSetsCountingDict[freqSet] = freqSetsCountingDict.get(freqSet, 0) + 1
                    if freqSet in freqSetsCountingDict:
                        freqSetsCountingDict[freqSet] += 1
                    else:
                        freqSetsCountingDict[freqSet] = 1
        # print("Counted sets:",freqSetsCountingDict)
        return freqSetsCountingDict

    def filterTheDict(freqSetsCountingDict: dict):
        freqSets = set()
        for itemSet in freqSetsCountingDict.keys():
            if freqSetsCountingDict[itemSet] >= sup:
                freqSets.add(itemSet)
        return freqSets

    def pairsToTriples(pairs):
        countingTable = {}
        for s1 in pairs:
            s1 = set(s1)
            for s2 in pairs:
                s2 = set(s2)
                if s1 != s2:
                    tempUnion = tuple(s1 | s2)
                    if len(tempUnion) == 3:
                        if tempUnion in countingTable:
                            countingTable[tempUnion] += 1
                        else:
                            countingTable[tempUnion] = 1
        dictKeys = list(countingTable.keys())
        for key in dictKeys:
            if countingTable[key] < 2:
                countingTable.pop(key)
        triples = list(countingTable.keys())
        for i in range(len(triples)):
            triples[i] = tuple(sorted(triples[i]))
        return sorted(triples)

    # decide if should start from first phase or just use the result of last phase
    if phase == 1:
        out = set(filterTheDict(getFreqItemsOfPartition()))
    else:
        out = lastResultOfFreqSets

    if phase != 1:
        newFreq = set()
        for x in out:
            for i in x:
                newFreq.add(i)
    else:
        newFreq = set(out)

    _combinations = getCombinationsFromFreqsets(newFreq, phase)

    if phase > 3:
        newComb = set()
        for itemset in _combinations:
            subComb = set(combinations(itemset, len(itemset) - 1))
            # print("subComb",subComb)
            flag = 1
            for subset in subComb:
                if subset not in out:
                    flag = 0
                    break
            if flag:
                newComb.add(itemset)
        _countedSets = countEverySet(newComb)
    elif phase == 3:
        _countedSets = countEverySet(pairsToTriples(out))
    else:
        _countedSets = countEverySet(_combinations)

    out = set(filterTheDict(_countedSets))

    return out


# @timer
def task1(caseNum: int, sup: int, sc):
    rddCSV = sc.textFile(csvFile).map(lambda line: Map_transCSVToTuple(
        line, "business_id,user_id")).filter(
        lambda line: line if line != None else 0).map(
        Map_emitKVPairFromCSV).distinct()

    # Generating the data which user is the basket
    if caseNum == 1:
        rddData = rddCSV.map(lambda line:(line[0],[line[1]])).reduceByKey(lambda x,y:x+y).cache()
    elif caseNum == 2:
        # Generating the data which business is the basket
        rddData = rddCSV.map(lambda line:(line[1],[line[0]])).reduceByKey(lambda x,y:x+y).cache()

    sonSup = int(sup / rddData.getNumPartitions())

    phase = 1
    candidates = []
    result = []
    singlePhaseResult = [1]
    while singlePhaseResult != []:
        # print(f"Phase:{phase}")
        freqSetsCandidates = rddData.mapPartitions(
            lambda partition: Map_aPriori_boosted(partition, sonSup, phase, singlePhaseResult)).map(
            Map_emitKVPair).distinct().collect()
        freqSetsCandidates.sort()
        candidates.append(freqSetsCandidates)
        # print(freqSetsCandidates.collect())
        singlePhaseResult = rddData.flatMap(
            lambda line: Map_countFreqSets(line, freqSetsCandidates)).reduceByKey(lambda x, y: x + y).filter(
            lambda line: line if line[1] >= sup else 0).map(lambda line: line[0]).collect()
        phase += 1
        singlePhaseResult.sort()
        result.append(singlePhaseResult)
    return result[:-1], candidates[:-1]


frequentSets, candidateSets = task1(caseNum, sup, sc)

# size = 1
# for x in range(len(frequentSets)):
#     print(f"size{size}", len(frequentSets[x]), len(candidateSets[x]))
#     size += 1

for i in range(len(candidateSets)):
    for j in range(len(candidateSets[i])):
        candidateSets[i][j] = candidateSets[i][j][0]

outStr = 'Candidates:\n'
for i in range(len(candidateSets)):
    if i == 0:
        outStr += str(candidateSets[i]).replace(",)", ")").replace('), (', '),(').lstrip('[').rstrip(']')
    else:
        outStr += str(candidateSets[i]).lstrip('[').rstrip(']').replace('), (', '),(')
    outStr += '\n\n'
outStr += 'Frequent Itemsets:\n'
for i in range(len(frequentSets)):
    if i == 0:
        outStr += str(frequentSets[i]).replace(",)", ")").replace('), (', '),(').lstrip('[').rstrip(']')
    else:
        outStr += str(frequentSets[i]).lstrip('[').rstrip(']').replace('), (', '),(')
    outStr += '\n\n'

with open(argv[4], 'w') as f:
    f.write(outStr)

endTime = time.time()
runingTime = endTime - startTime
print("Duration: %.2fs" % runingTime)
# print(outStr)
