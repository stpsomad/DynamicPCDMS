# -*- coding: utf-8 -*-
"""
Created on Fri May 20 10:47:57 2016

@author: Stella Psomadaki
"""

from pointcloud.test.Validator import Validate
from pointcloud.test.ValidateQuery import Querier
from tabulate import tabulate
import os

dataset = 'zandmotor'
path = os.getcwd()

approach = ['mini', 'medium', 'large']
loading, queries = [], []

for num in range(1, 4):
    configuration = path + '/ini/' + dataset + '/validation_part{0}.ini'.format(num)
    validate = Validate(configuration)
    connection = validate.getConnection()
    load, rtree, btree = validate.loadSpatialSqlldr(connection)
    loading.append([approach[num-1], load, rtree, btree])
    
    #querying    
    querier = Querier(configuration)
    for query in querier.ids:
        for i in range(6):
            t = querier.query(query)
            queries.append([query, t])
    
# print stats    
hloading = ['approach', 'load', 'rtree', 'btree']
print tabulate(loading, hloading, tablefmt="plain")
hquery = ['id', 'fetch.time']
print tabulate(queries, hquery, tablefmt="plain")