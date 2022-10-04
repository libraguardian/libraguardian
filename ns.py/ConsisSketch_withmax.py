# -*- coding: utf-8 -*-
import hashlib
import array
import copy
# borrowed from https://github.com/rafacarrascosa/countminsketch
import math

# -*- coding: utf-8 -*-
import hashlib
import array
# from mrac import *
from collections import defaultdict
from utils import merge_dict_with_max, max_dict_update
# borrowed from https://github.com/rafacarrascosa/countminsketch
import EMFSD


class HalfCUsketch_withmax(object):
    def __init__(self, m, d):
        if not m or not d:
            raise ValueError("Table size (m) and amount of hash functions (d)"
                             " must be non-zero")
        self.m = m
        self.d = d
        self.n = 0
        self.sum_tables = []
        self.MAX = 65535
        self.ts_tables = []  # array ->timestamp _ table
        self.int_tables = []  # array ->max interval table
        self.MIN = 1e-5
        for _ in range(d): #sum table    unsigned int 4Bytes
            table = array.array("l", (0 for _ in range(m)))
            self.sum_tables.append(table)
        for _ in range(d):
            table = array.array("f", (0 for _ in range(int(m))))  #1/2 memory
            self.ts_tables.append(table)
        for _ in range(d):
            table = array.array("f", (0 for _ in range(int(m))))  #1/2 memory
            self.int_tables.append(table)

    def _hash(self, x):
        md5 = hashlib.md5(str(hash(x)).encode('utf-8'))
        hash_d = []
        for i in range(self.d):
            md5.update(str(i).encode('utf-8'))
            hash_d.append(int(md5.hexdigest(), 16) % self.m)
        return hash_d

    def add(self, x, value=1, y=None):
        self.n += value

        hash_d = self._hash(x)
        #sum
        while(value):
            minus = value
            minp, secp = self.MAX, self.MAX
            for table, i in zip(self.sum_tables, hash_d):
                # print(table)
                if minp >= table[i]:
                    minp = table[i]
                else:
                    secp = min(secp, table[i])
                    minus = min(minus, secp-minp)

            minp = self.MAX
            for i, table in zip(range(self.d), self.sum_tables):
                minp = min(minp, table[hash_d[i]])
                if table[hash_d[i]] == minp:
                    table[hash_d[i]] += minus

            value -= minus

        #max
        for num, (ts_table, int_table, i) in enumerate(zip(self.ts_tables, self.int_tables, hash_d)):
            last_ts = ts_table[i]
            last_int = int_table[i]
            if abs(last_ts) == 0.:
                self.ts_tables[num][i] = y
            else:
                interval = y - last_ts
                self.ts_tables[num][i] = y
                self.int_tables[num][i] = max(last_int, interval)


    def query(self, x, only_sum=False):
        if only_sum:
            sum_ans = self.MAX
            for table, i in zip(self.sum_tables, self._hash(x)):
                sum_ans = min(sum_ans, table[i])
            return sum_ans
        else:
            sum_ans = self.MAX
            for table, i in zip(self.sum_tables, self._hash(x)):
                sum_ans = min(sum_ans, table[i])

            ts_ans, int_ans = self.MIN, self.MAX
            for ts_tables, int_tables, i in zip(self.ts_tables, self.int_tables, self._hash(x)):
                ts_ans = max(ts_ans, ts_tables[i])
                int_ans = min(int_ans, int_tables[i])
            return sum_ans, ts_ans, int_ans

    def __getitem__(self, x):
        return self.query(x)

    def __len__(self):
        return self.n

    def get_zero_count(self):
        zero = 0.
        for counter in self.sum_tables[0]:
            if counter == 0.:
                zero += 1
        return int(zero)



class SuMax_withmax(object):

    def __init__(self, memory, flowtable_size, hashtable_size, threshold_ratio):
        self.d = 3
        self.m = int(memory / self.d / 4 / 2)
        self.Sketch = HalfCUsketch_withmax(self.m, self.d)

        self.flowtable_size = flowtable_size
        self.hashtable_size = hashtable_size
        self.flow_table = defaultdict(list)  #key:[freq, last_timestamp, max_interval]
        self.hash_table = defaultdict(list)

        self.flowtable_sum = defaultdict(list)
        self.flow_table_segment = []
        self.sketch_segment = []
        self.hash_table_keys_segment = []

        self.first_segement = True
        self.threshold = None
        self.threshold_ratio = threshold_ratio
        self.flowtable_min = None

        self.entropy_num_segment = []
        self.entorpy_num = {}

    def insert(self, x, freq, cur_ts):
        if x in self.flow_table:
            self.flow_table[x][0] += freq
            if self.flow_table[x][0] > 1:
                self.flow_table[x][2] = max(self.flow_table[x][2], cur_ts-self.flow_table[x][1])
            self.flow_table[x][1] = cur_ts
        else:
            former_sum_result = self.Sketch.query(x, only_sum=True)

            self.Sketch.add(x, freq, cur_ts)

            sum_result, ts_result, int_result = self.Sketch.query(x)
            if self.first_segement:
                self.hash_table[x] = [sum_result, ts_result, int_result]

                if len(self.hash_table) > self.hashtable_size:
                    sort_hash_table = sorted(self.hash_table.items(), key=lambda x: x[1][0], reverse=True)
                    self.hash_table = dict(sort_hash_table[:self.hashtable_size])
            else:
                #sum
                if sum_result >= self.threshold[0] and sum_result <= self.threshold[-1]:
                    if sum_result in self.threshold:
                        if x in self.hash_table:
                            self.hash_table = max_dict_update(self.hash_table, x, sum_result, ts_result, int_result)
                        else:
                            self.hash_table[x] = [sum_result, ts_result, int_result]

                    if len(self.hash_table) > self.hashtable_size:
                        sort_hash_table = sorted(self.hash_table.items(), key=lambda x: x[1][0], reverse=True)
                        self.hash_table = dict(sort_hash_table[:self.hashtable_size])
            #entropy
            if former_sum_result in self.entorpy_num:
                if self.entorpy_num[former_sum_result] > 0:
                    self.entorpy_num[former_sum_result] -= 1
            if sum_result in self.entorpy_num:
                self.entorpy_num[sum_result] += 1
            else:
                self.entorpy_num[sum_result] = 1


    def change_segment(self):
        # print("changesegment")
        for k, v in self.hash_table.items():
            self.flow_table[k] = [v[0], v[1], v[2]]
        if self.first_segement:
            pass
        else:

            for k, v in self.flow_table.items():
                if v[0] in self.entorpy_num:
                    self.entorpy_num[v[0]] += 1
                else:
                    self.entorpy_num[v[0]] = 1


        if len(self.flow_table) > self.flowtable_size:
            sort_flow_table = sorted(self.flow_table.items(), key=lambda x: x[1][0], reverse=True)
            self.flowtable_min = sort_flow_table[self.flowtable_size][1][0]
            self.flow_table = dict(sort_flow_table[:self.flowtable_size])
        else:
            if len(self.flow_table.items())==0:
                pass
            else:
                sort_flow_table = sorted(self.flow_table.items(), key=lambda x: x[1][0], reverse=True)
                self.flowtable_min = sort_flow_table[-1][1][0]

                self.threshold = [math.ceil(i*self.flowtable_min) for i in self.threshold_ratio]
                # print(self.threshold)

                self.flowtable_sum = merge_dict_with_max(self.flowtable_sum, self.flow_table)

                self.first_segement = False

        self.flow_table_segment.append(copy.deepcopy(self.flow_table))
        self.hash_table_keys_segment.append(self.hash_table.keys())
        self.sketch_segment.append(copy.deepcopy(self.Sketch))

        self.entropy_num_segment.append(copy.deepcopy(self.entorpy_num))


    def reset_ske(self, mem):
        self.d = 3
        self.m = int(mem / self.d / 4 / 2)
        self.Sketch = HalfCUsketch_withmax(self.m, self.d)

        for k,v in self.flow_table.items():
            self.flow_table[k] = [0, v[1], 0]

        self.hash_table.clear()
        self.entorpy_num.clear()
