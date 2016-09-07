# -*- coding: utf-8 -*-
"""
Created on Fri May 06 14:11:41 2016

@author: Stella Psomadaki
"""
import os
import time
from tabulate import tabulate
from pointcloud.cppQuerierOnlySFC import Querier
import pointcloud.oracleTools as ora


###########################
###   Setup Variables   ###
###########################
dataset = 'zandmotor'
case = 'dxyzt_10000_0_False_part1'
repeat = 1
queryID = [1,3,4,5,6,7,8,9,10,11,12]
###########################

hquery =  ["id", "preparation", "filter", 'decode+store', 'refinement', 'ranges', "Appr.pts", "Fin.pts", 'extra%', 'total', 'query']
queries = []

path = os.getcwd()
configuration = path + '/ini/' + dataset + '/' + case + '.ini'

querier = Querier(configuration)
connection = querier.getConnection()
cursor = connection.cursor()

cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[querier.queriesTable.upper(),])
length = len(cursor.fetchall())
if not length:
    os.system('python -m pointcloud.queryTab {0}'.format(configuration))

for num in queryID:
    num = str(num)
    for j in range(repeat):
        start = time.time()
        lst = querier.query(num)
        lst.append(round(time.time() - start, 2))
        lst.append(round(lst[0]+lst[1],2))
        lst.insert(0, num)
        queries.append(lst)
        ora.dropTable(cursor, querier.queryTable + '_' +  str(num))
        ora.dropTable(cursor, querier.filterTable + str(num))
        ora.dropTable(cursor, querier.rangeTable + str(num))
        print tabulate([lst], hquery, tablefmt="plain")
print
print tabulate(queries, hquery, tablefmt="plain")
