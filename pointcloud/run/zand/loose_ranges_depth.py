# -*- coding: utf-8 -*-
"""
Created on Tue May 31 15:50:12 2016

@author: Stella Psomadaki
"""

#run for mini
import os
import time
from tabulate import tabulate
from pointcloud.AbstractQuerier import Querier
import pointcloud.oracleTools as ora


###########################
###   Setup Variables   ###
###########################
dataset = 'zandmotor'
integrations = ['lxyt', 'lxyzt']
scaling = '1'
repeat = 3
queryIds = [1, 2, 3, 4, 6, 7, 8, 9, 11, 12]
###########################

fh = open('ranges_non-int_200_{0}.txt'.format(time.strftime("%d%m%Y")), 'a')
fh.write('Test executed on \n')
fh.write(time.strftime("%d/%m/%Y"))
fh.write('\n')
fh.write(
"""CASE: Non-integrated approach (loose) with scale of 1

Test to identify if the number of extra points is caused from the 200 ranges 
used as maximum or from the fact that the ranges come from more rough n-tree 
blocks.

Different approaches:
    * z as an attribute and as part of morton code

The queries are repeated 3 times

--START--\n\n\n""")

hquery =  ["id", 'depth', "prep.", 'insert', 'ranges', 'fetching', "decoding", 'storing', "Appr.pts", "Fin.pts", "FinFilt", "time", 'extra%', 'total']
path = os.getcwd()

for integr in integrations:
    queries = []
    configuration = path + '/ini/' + dataset + '/' + integr + '_' + scaling + '_part1.ini'
    querier = Querier(configuration)
    querier.numProcesses = 0
    connection = querier.getConnection()
    cursor = connection.cursor()
    
    cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[querier.queriesTable.upper(),])
    length = len(cursor.fetchall())
    if not length:
        os.system('python -m pointcloud.queryTab {0}'.format(configuration))
    
    # till 15 not enough
    for num in queryIds:
        for i in range(9, 20):
            querier.numLevels = i
            for j in range(repeat):
                start = time.time()
                lst = querier.query(str(num))
                lst.append(round(time.time() - start, 2))
                lst.append(round((lst[6] - lst[7])/float(lst[7])*100,2))
                lst.append(round(lst[1]+lst[3]+lst[4]+lst[5]+lst[8],2))
                lst.insert(0, i)
                lst.insert(0, num)
                queries.append(lst)
                ora.dropTable(cursor, querier.queryTable + '_' +  str(num))
                print tabulate([lst], hquery, tablefmt="plain")

    print integr + '\n\n'
    print tabulate(queries, hquery, tablefmt="plain")
    fh.write(integr + '\n\n')
    fh.write(tabulate(queries, hquery, tablefmt="plain"))
    fh.write('\n\n\n\n')