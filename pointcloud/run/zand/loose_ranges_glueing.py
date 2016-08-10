# -*- coding: utf-8 -*-
"""
Created on Tue May 31 19:59:03 2016

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
merges = [10, 100, 1000, 10000]
scaling = '1'
repeat = 3
queriyIds = [1, 2, 3, 4, 6, 7, 8, 9, 11, 12]
###########################

fh = open('non-int_glueing_{0}.txt'.format(time.strftime("%d%m%Y")), 'a')
fh.write('Test executed on \n')
fh.write(time.strftime("%d/%m/%Y"))
fh.write('\n')
fh.write(
"""CASE: Non-integrated approach (loose) with scale of 1

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
    
    if integr == 'lxyt':
        # run two first with -2
        levels = [19, 19, 20, 19, 20, 22, 22, 19, 20, 19]
    elif integr == 'lxyzt':
        levels = [14, 14, 16, 15, 16, 17, 17, 15, 16, 15]
    
    for num in range(len(queriyIds)):
        fh.write('\n\n')
        querier.numLevels = levels[num]
        for merge in merges:
            querier.maxRanges = merge
            
            for j in range(repeat):
                start = time.time()
                lst = querier.query(str(queriyIds[num]))
                lst.append(round(time.time() - start, 2))
                lst.append(round((float(lst[7]) - float(lst[8]))/float(lst[8])*100,2))
                lst.append(round(lst[1] + lst[4] + lst[5] + lst[6] + lst[9],2))
                lst.insert(0, merge)
                lst.insert(0, queriyIds[num])
                queries.append(lst)
                ora.dropTable(cursor, querier.queryTable + '_' +  str(queriyIds[num]))
                print tabulate([lst], hquery, tablefmt="plain")
                fh.write(tabulate([lst], tablefmt="plain"))
                fh.write('\n')
        fh.write('\n\n')

    print integr + '\n\n'
    print tabulate(queries, hquery, tablefmt="plain")
    fh.write(integr + '\n\n')
    fh.write(tabulate(queries, hquery, tablefmt="plain"))
    fh.write('\n\n\n\n')