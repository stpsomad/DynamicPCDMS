# -*- coding: utf-8 -*-
"""
Created on Fri May 06 14:11:41 2016

@author: Stella Psomadaki
"""
import os
import time
from tabulate import tabulate
from pointcloud.AbstractQuerier import Querier
import pointcloud.oracleTools as ora


###########################
###   Setup Variables   ###
###########################
dataset = 'zandmotor'
case = 'lxyt_1_part1'
repeat = 1
###########################

hquery =  ["id", "prep.", 'insert', 'ranges', 'fetching', "decoding", 'storing', "Appr.pts", "Fin.pts", "FinFilt", "time", 'extra%', 'total']
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

for num in querier.ids:
    for j in range(repeat):
        start = time.time()
        lst = querier.query(num)
        lst.append(round(time.time() - start, 2))
        lst.append(round((lst[6] - lst[7])/float(lst[7])*100,2))
        lst.append(round(lst[1]+lst[3]+lst[4]+lst[5]+lst[8],2))
        lst.insert(0, num)
        queries.append(lst)
        ora.dropTable(cursor, querier.queryTable + '_' +  str(num))
        print tabulate([lst], hquery, tablefmt="plain")
#    if querier.integration == 'deep':
#        ora.dropTable(cursor, querier.rangeTable + str(num))
print
print tabulate(queries, hquery, tablefmt="plain")
