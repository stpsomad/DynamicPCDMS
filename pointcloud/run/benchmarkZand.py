# -*- coding: utf-8 -*-
"""
Created on Sun Mar 13 11:26:53 2016

@author: Stella Psomadaki
"""
from pointcloud.AbstractBulkLoader import BulkLoader
from pointcloud.AbstractQuerier import Querier
import time
from tabulate import tabulate
import pointcloud.oracleTools as ora
#import os

benchmark = ['mini', 'medium', 'full']  
hloading = ['approach', 'preparation', 'loading', 'closing', 'size[MB]', 'points']

integrations = ['dxyt']
scalings = ['10000']


for integr in integrations:
    for scaling in scalings:
        loadings = []
        queries = []
        for i in range(1,4):
            configuration = 'D:/Dropbox/Thesis/Thesis/Code/ini/zandmotor/{0}_{1}_part{2}.ini'.format(integr, scaling, i)
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
            
#            if i == 1:
#                os.system('python -m pointcloud.createQueryTable {0}'.format(configuration)) 
                
            hquery =  ["id", "prep.", 'insert', 'ranges', 'fetching', "decoding", 'storing', "Appr.pts", "Fin.pts", "FinFilt", "time", 'extra%', 'total']
        
            querier = Querier(configuration)
            connection = bulk.getConnection()
            cursor = connection.cursor()
            print 'querying...'
            for num in querier.ids:
                for j in range(5):
                    start = time.time()
                    lst = querier.query(num)
                    lst.append(round(time.time() - start, 2))
                    lst.append(round((lst[6] - lst[7])/float(lst[7])*100,2))
                    lst.append(round(lst[1]+lst[3]+lst[4]+lst[5]+lst[8],2))
                    lst.insert(0, num)
                    queries.append(lst)
                    ora.dropTable(cursor, querier.queryTable + '_' +  str(num))               
                ora.dropTable(cursor, querier.rangeTable + str(num))
            
            print tabulate(queries, hquery, tablefmt="plain")
        
        print 'writing to file statistics for case: {0}_{1}_{2}'.format(bulk.integration, bulk.parse, bulk.scale)
        f = open('loading_stats_31_03_load_2.txt', 'a')
        f.write('Statistics for case: {0} {1} {2}\n'.format(bulk.integration, bulk.parse, bulk.scale))
        f.write(tabulate(loadings, hloading, tablefmt="plain"))
        f.write('\n')
        f.write(tabulate(queries, hquery, tablefmt="plain"))
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.close()