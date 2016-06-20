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
integrations = ['dxyt', 'dxyzt']
scaling = '10000'
repeatQueries = 6
parallels = [0, 8]
fresh_reloads = [True, False]
maxRanges = [200, 1000000]
###########################

if dataset == 'zandmotor':
    bench = 3
elif dataset == 'coastline':
    bench = 4
    
path = os.getcwd()
benchmark = ['mini', 'medium', 'full']  
hloading = ['approach', 'preparation', 'loading', 'closing', 'size[MB]', 'points']
hquery =  ["id", "prep.", 'insert', 'ranges', 'fetching', "decoding", 'storing', "Appr.pts", "Fin.pts", "FinFilt", "time", 'extra%', 'total']

fh = open('integrated_{0}.txt'.format(time.strftime("%d%m%Y")), 'a')
fh.write('Benchmark executed on \n')
fh.write(time.strftime("%d/%m/%Y"))
fh.write('\n')
fh.write(
"""CASE: Integrated approach (deep) with scale of 10,000

Different approaches examined are:
    * z as an aatribute and as part of the morton key
    * parallel execution of 8 and no parallel
    * fresh reload of the datasets and not
    * querying with max_Ranges 200 (for comparison) and 1,000,000

The queries are repeated 6 times

--START--\n\n\n""")


for fresh_reload in fresh_reloads:
    for parallel in parallels:
        for integr in integrations:
            loadings = []
            queries = []
            for i in range(1,bench + 1):
            
                #================================================================
                #                 Loading Phase
                #================================================================
                configuration = path + '/ini/' + dataset + '/' + integr + '_' + scaling + "_{0}_{1}".format(parallel, fresh_reload) + '_part' + str(i) + '.ini'
    
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
                
                #================================================================
                #                 Querying Phase
                #================================================================
            
                querier = Querier(configuration)
                connection = querier.getConnection()
                cursor = connection.cursor()
                
                cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[querier.queriesTable.upper(),])
                length = len(cursor.fetchall())
                if not length:
                    os.system('python -m pointcloud.queryTab {0}'.format(configuration))
                
                for maxRange in maxRanges:
                    querier.maxRanges = maxRange
                    sublist = []
                    for num in querier.ids:
                        for j in range(repeatQueries):
                            start = time.time()
                            lst = querier.query(num)
                            lst.append(round(time.time() - start, 2))
                            lst.append(round((lst[6] - lst[7])/float(lst[7])*100,2))
                            lst.append(round(lst[1]+lst[3]+lst[4]+lst[5]+lst[8],2))
                            lst.insert(0, num)
                            sublist.append(lst)
                            ora.dropTable(cursor, querier.queryTable + '_' +  str(num))               
                        ora.dropTable(cursor, querier.rangeTable + str(num))
                    queries.append(sublist)
                
            print
            print tabulate(loadings, hloading, tablefmt="plain")
            for i in range(len(maxRanges)):
                print
                print 'maximum ranges: {0}'.format(maxRanges[i])
                print tabulate(queries[i], hquery, tablefmt="plain")

            fh.write('integration: {0}\nreload:{1}\nparallel:{2}\n\n'.format(integr, fresh_reload, parallel))
            fh.write('\n---LOADING---\n')
            fh.write(tabulate(loadings, hloading, tablefmt="plain"))
            fh.write('\n---QUERYING---\n')
            for i in range(len(maxRanges)):
                fh.write('maximum ranges: {0}'.format(maxRanges[i]))
                fh.write(tabulate(queries[i], hquery, tablefmt="plain"))
                fh.write('\n')
                fh.write('\n')
fh.close()