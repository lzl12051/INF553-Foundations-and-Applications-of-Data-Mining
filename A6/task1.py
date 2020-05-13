import binascii
import math
import random
from sys import argv
import lib553

random.seed(1205)

train_data = argv[1]#'dataset/business_first.json'
predict_data = argv[2]#'dataset/business_second.json'
output_file = argv[3]#'task1_out.csv'

sc = lib553.START_SPARK('A6T1')

rdd_train_data = sc.textFile(train_data).map(lib553.json_trans_to_dict).map(
    lambda line: line['city']).filter(lambda line: line != '').distinct().map(
    lambda line: int(binascii.hexlify(line.encode('utf8')), 16))
total_cities = rdd_train_data.count()
# test_5 = rdd_train_data.take(5)

k = 8
m = total_cities
n = 10000

fp_rate = (1 - math.e ** (-k * m / n)) ** k
print('The false positive rate could be ', fp_rate)

cof_seed_list = random.sample(list(range(1, m * k)), k)
offset_seed_list = random.sample(list(range(1, 500 * k)), k)


def hashfunc(lineIn: int, cof_seed_list: list, offset_seed_list: list, m):
    if not lineIn is None:
        result = set()
        for i in range(len(cof_seed_list)):
            hash_result = (cof_seed_list[i] * lineIn + offset_seed_list[i]) % m
            result.add(hash_result)
        return result
    else:
        return None


bloom_filter = rdd_train_data.map(lambda line: hashfunc(line, cof_seed_list, offset_seed_list, n)).reduce(
    lambda x, y: x.union(y))


def trans_str_to_int(lineIn: str):
    if not lineIn is None:
        return int(binascii.hexlify(lineIn.encode('utf8')), 16)
    else:
        return None


def predict(lineIn, bloom_filter_in):
    if not lineIn is None:
        for position in lineIn:
            if not position in bloom_filter_in:
                return 0
        return 1
    else:
        return 0


rdd_predict_data = sc.textFile(predict_data)
# print(rdd_predict_data.count())
rdd_predict_data = rdd_predict_data.map(lib553.json_trans_to_dict).map(
    lambda line: line['city']).map(lambda line: None if line == '' else line).map(trans_str_to_int).map(
    lambda line: hashfunc(line, cof_seed_list, offset_seed_list, n)).map(
    lambda line: predict(line, bloom_filter)).collect()  # reduce(lambda x,y: x.union(y))

output_result = ' '.join(map(str,rdd_predict_data))
with open(output_file, 'w') as f:
    f.write(output_result)