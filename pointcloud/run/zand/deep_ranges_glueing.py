# -*- coding: utf-8 -*-
"""
Created on Thu Jul 07 08:44:32 2016

@author: Stella
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
integrations = ['dxyt', 'dxyzt']
scaling = '10000'
repeat = 3
merges = [10, 100, 1000, 10000, 100000]
###########################


fh = open('ranges_int_glueing_{0}.txt'.format(time.strftime("%d%m%Y")), 'a')
fh.write('Test executed on \n')
fh.write(time.strftime("%d/%m/%Y"))
fh.write('\n')
fh.write(
"""CASE: Integrated approach (deep) with scale of 10,000

Test to identufy what is the effect of having different degree of merging.

Different approaches:
    * z as an attribute and as part of morton code

The queries are repeated 3 times

--START--\n\n\n""")

hquery =  ["id", 'maxRanges', "prep.", 'insert', 'ranges', 'Levels', 'fetching', "decoding", 'storing', "Appr.pts", "Fin.pts", "FinFilt", "time", 'extra%', 'total']
path = os.getcwd()

for integr in integrations:
    queries = []
    configuration = path + '/ini/' + dataset + '/' + integr + '_' + scaling + '_0_False_part1.ini'
    querier = Querier(configuration)
    querier.numProcesses = 0
    connection = querier.getConnection()
    cursor = connection.cursor()
    
    cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[querier.queriesTable.upper(),])
    length = len(cursor.fetchall())
    if not length:
        os.system('python -m pointcloud.queryTab {0}'.format(configuration))

    if integr == 'dxyt':
        levels = [16, 16, 15, 17, 13, 16, 16, 16, 17, 13, 15, 17]
    elif integr == 'dxyzt':
        levels = [15, 15, 14, 15, 12, 15, 15, 14, 15, 13, 14, 15]
    
    for num in querier.ids:
        fh.write('\n\n')
        querier.numLevels = levels[int[num]-1]
        for merge in merges:
            querier.maxRanges = merge
            for j in range(repeat):
                start = time.time()
                lst = querier.query(num)
                lst.append(round(time.time() - start, 2))
                lst.append(round((float(lst[7]) - float(lst[8]))/float(lst[8])*100,2))
                lst.append(round(lst[1]+lst[4]+lst[5]+lst[6]+lst[9],2))
                lst.insert(0, merge)
                lst.insert(0, num)
                queries.append(lst)
                ora.dropTable(cursor, querier.queryTable + '_' +  str(num))
                print tabulate([lst], hquery, tablefmt="plain")
                fh.write(tabulate([lst], tablefmt="plain"))
                fh.write('\n')
            ora.dropTable(cursor, querier.rangeTable + str(num))
        fh.write('\n\n')

    print integr + '\n\n'
    print tabulate(queries, hquery, tablefmt="plain")
    fh.write(integr + '\n\n')
    fh.write(tabulate(queries, hquery, tablefmt="plain"))
    fh.write('\n\n\n\n')