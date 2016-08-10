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


if __name__ == "__main__":
    dataset = 'zandmotor'
    case = 'lxyt_1_part1'
    
    #glueing different amount of ranges
    maxRanges = [10, 100, 1000, 10000]

    hquery =  ["id", "prep.", 'insert', 'ranges', 'fetching', "decoding", 'storing', "Appr.pts", "Fin.pts", "FinFilt", "time", 'extra%', 'total']
    queries = []    
    
    path = os.getcwd()
    configuration = path + '/ini/' + dataset + '/' + case + '.ini'
    querier = Querier(configuration)
    connection = querier.getConnection()
    cursor = connection.cursor()
    
    #going deeper to test merging more ranges
    querier.params[0] = 6   
    querier.params[1] = 8
    
    cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[querier.queriesTable.upper(),])
    length = len(cursor.fetchall())
    if not length:
        os.system('python -m pointcloud.queryTab {0}'.format(configuration))

    f = open('/home/stella/results/ranges_test.txt', 'a')
    
    for num in querier.ids:
        for maxRange in maxRanges:
            querier.maxRanges = maxRange
            for j in range(3):
                start = time.time()
                lst = querier.query(num)
                lst.append(round(time.time() - start, 2))
                lst.append(round((lst[6] - lst[7])/float(lst[7])*100,2))
                lst.append(round(lst[1]+lst[3]+lst[4]+lst[5]+lst[8],2))
                lst.insert(0, num)
                queries.append(lst)
                ora.dropTable(cursor, querier.queryTable + '_' +  str(num))

    if querier.integration == 'deep':
        for num in querier.ids:
            ora.dropTable(cursor, querier.rangeTable + str(num))
    print
    print tabulate(queries, hquery, tablefmt="plain")
    
    f.write(tabulate(queries, hquery, tablefmt="plain"))
    f.write('\n')
