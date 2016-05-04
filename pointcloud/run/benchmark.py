# -*- coding: utf-8 -*-
"""
Created on Mon Apr 04 14:38:34 2016

@author: Stella Psomadaki
"""

from pointcloud.AbstractBulkLoader import BulkLoader
from pointcloud.AbstractQuerier import Querier
import time
from tabulate import tabulate
import pointcloud.oracleTools as ora
import os

###########################
dataset = 'zandmotor'
integrations = ['dxyt']
scalings = ['10000']
repeatQueries = 2
###########################

path = os.getcwd()
benchmark = ['mini', 'medium', 'full']  
hloading = ['approach', 'preparation', 'loading', 'closing', 'size[MB]', 'points']


for integr in integrations:
    for scaling in scalings:
        loadings = []
        queries = []
        for i in range(1,2):
            configuration = path + '/ini/' + dataset + '/' + integr + '_' + scaling + '_part' + str(i) + '.ini'

            bulk = BulkLoader(configuration)
            loading = []
            loading.append(benchmark[i - 1])
                      
            start = time.time()
            bulk.preparation()
            loading.append(round(time.time() - start, 2))
            
            start = time.time()
            bulk.loading()
            loading.append(round(time.time() - start, 2))
            
            start = time.time()
            bulk.closing()
            loading.append(round(time.time() - start, 2))
           
            size, points = bulk.statistics()
            loading.append(round(size,2))
            loading.append(int(points))
            
            loadings.append(loading)
            print tabulate(loadings, hloading, tablefmt="plain")
            
            if i == 1:
                os.system('python -m pointcloud.queryTab {0}'.format(configuration))

            hquery =  ["id", "prep.", 'insert', 'ranges', 'fetching', "decoding", 'storing', "Appr.pts", "Fin.pts", "FinFilt", "time", 'extra%', 'total']
        
            querier = Querier(configuration)
            connection = bulk.getConnection()
            cursor = connection.cursor()

            for num in querier.ids:
                for j in range(repeatQueries):
                    start = time.time()
                    lst = querier.query(num)
                    lst.append(round(time.time() - start, 2))
                    lst.append(round((lst[6] - lst[7])/float(lst[7])*100,2))
                    lst.append(round(lst[1]+lst[3]+lst[4]+lst[5]+lst[8],2))
                    lst.insert(0, num)
                    queries.append(lst)
                    ora.dropTable(cursor, querier.queryTable + '_' +  str(num))               
                if querier.integration == 'deep':
                    ora.dropTable(cursor, querier.rangeTable + str(num))
            print
            print tabulate(queries, hquery, tablefmt="plain")

    
##    print 'writing to file statistics for case: {0} {1} {2}'.format(bulk.integration, bulk.parse, bulk.scale)
##    f = open('test.txt', 'a')
##    f.write('Statistics for case: {0} {1} {2}\n'.format(bulk.integration, bulk.parse, bulk.scale))
##    f.write(tabulate(loadings, hloading, tablefmt="plain"))
##    f.write('\n')