import binascii
from sys import argv
import random
from typing import List

import lib553
import datetime
import csv
from statistics import median_high, mean, mode

# random.seed(1205)

port = int(argv[1])
output_file = argv[2]
ssc = lib553.START_SPARK_STREAMING(lib553.START_SPARK('A6T2'), 5)

csv_header = ['Time', 'Ground Truth', 'Estimation']
with open(output_file, 'w') as f:
    out = csv.writer(f)
    out.writerow(csv_header)


def trans_str_to_int(lineIn: str):
    return int(binascii.hexlify(lineIn.encode('utf8')), 16)


ds_text_stream = ssc.socketTextStream('localhost', port).map(lambda line: lib553.json_trans_to_dict(line)).map(
    lambda line: line['city']).filter(lambda line: line != '').map(trans_str_to_int)

#######################
# Building Hash Funcs #
#######################
k = 24
cof_seed_list = random.sample(list(range(1, k * 3)), k)
offset_seed_list = random.sample(list(range(1, 500 * k)), k)

# print(cof_seed_list, offset_seed_list)

prime_number = 337


def testfunc(rdd):
    def how_many_0s(binIn: bin):
        counter = 0
        inverse = str(binIn)[:1:-1]

        for c in inverse:
            if c == '0':
                counter += 1
            else:
                return counter
        else:
            return 1

    def hashs(lineIn: int, cof_seed_list_in: list, offset_seed_list_in: list, prime):
        result: List[List[int]] = []
        for i in range(len(cof_seed_list_in)):
            hash_result = (cof_seed_list_in[i] * lineIn + offset_seed_list_in[i]) % prime
            result.append([how_many_0s(bin(hash_result))])
        return result

    def reduce_hash_result(x: list, y: list):
        return list(map(lambda x_in, y_in: x_in + y_in, x, y))


    timestamp = str(datetime.datetime.now())[:-7]
    estimate_list = []
    i = 0
    slice_length = int(len(cof_seed_list)/4)
    # print(slice_length)
    all_element = rdd.count()
    ground_truth = rdd.distinct().count()
    while i < k:
        num_0 = rdd.map(lambda line: hashs(line, cof_seed_list[i:(i+slice_length)], offset_seed_list[i:(i+slice_length)], prime_number))
        reduced = num_0.reduce(lambda x, y: reduce_hash_result(x, y))
        # print(reduced)
        magic_number = 1
        # mean_list = map(max, reduced)
        # est_list = list(map(lambda x_in: int(x_in**2), mean_list))
        # print(median_high(est_list))
        estimate_t = int(2 ** median_high((map(max, reduced))) * magic_number)

        estimate_list.append(estimate_t)
        i += slice_length
    estimate = mean(estimate_list)

    with open(output_file, 'a') as file:
        output = csv.writer(file)
        output.writerow([timestamp, ground_truth, estimate])

    print(
        f'{timestamp}, ALL:{all_element}, GT: {ground_truth}, EST: {estimate}, '
        f'ERR: {round((estimate - ground_truth) / ground_truth * 100, 2)}%')


ds_window = ds_text_stream.window(30, 10).foreachRDD(testfunc)

ssc.start()
ssc.awaitTermination()
