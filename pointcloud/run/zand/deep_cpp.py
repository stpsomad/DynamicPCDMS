# -*- coding: utf-8 -*-
"""
Created on Tue May 31 13:38:19 2016

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
integrations = ['dxyzt']
scaling = '10000'
repeatQueries = 6
queryID = [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
###########################

if dataset == 'zandmotor':
    bench = 3
elif dataset == 'coastline':
    bench = 1
    
path = os.getcwd()
benchmark = ['mini', 'medium', 'full']  
hloading = ['approach', 'preparation', 'loading', 'closing', 'size[MB]', 'points']
hquery = ["id", "preparation", "filter", 'decode+store', 'refinement', 'ranges', "Appr.pts", "Fin.pts", 'extra%', 'total', 'query']

fh = open('cpp_{0}.txt'.format(time.strftime("%d%m%Y")), 'a')
fh.write('Benchmark executed on ')
fh.write(time.strftime("%d/%m/%Y"))
fh.write('\n')
fh.write(
"""CASE: Integrated approach (deep) with scale of 1000

Different approaches examined are:
    * z as an aatribute and as part of the morton key

Remarks:
    * no parallel both for loading and querying
    * Only the mini benchmark is used for the proof
    * The queries are repeated 6 times.

--START--\n\n\n""")


for integr in integrations:
    loadings = []
    queries = []
    for i in range(1,bench + 1):
        #========================================================================
        #                               Loading Phase
        #========================================================================
        configuration = path + '/ini/' + dataset + '/' + integr + '_' + scaling + '_0_False_part' + str(i) + '.ini'

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

        #========================================================================
        #                              Querying Phase
        #========================================================================
        querier = Querier(configuration)
        connection = querier.getConnection()
        cursor = connection.cursor()
        
        cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[querier.queriesTable.upper(),])
        length = len(cursor.fetchall())
        if not length:
            os.system('python -m pointcloud.queryTab {0}'.format(configuration))

        for num in queryID:
            num = str(num)
            for j in range(repeatQueries):
                start = time.time()
                lst = querier.query(num)
                lst.append(round(time.time() - start, 2))
                lst.append(round(lst[0]+lst[1],2))
                lst.insert(0, num)
                queries.append(lst)
                ora.dropTable(cursor, querier.queryTable + '_' +  str(num))
                ora.dropTable(cursor, querier.filterTable + str(num))
                ora.dropTable(cursor, querier.rangeTable + str(num))
                print tabulate([lst], tablefmt="plain")

    print
    print 'integration: {0}\n\n'.format(integr)
    print
    print tabulate(loadings, hloading, tablefmt="plain")
    print
    print tabulate(queries, hquery, tablefmt="plain")
                    
    fh.write('integration: {0}\n\n'.format(integr))
    fh.write('\n---LOADING---\n\n')
    fh.write(tabulate(loadings, hloading, tablefmt="plain"))
    fh.write('\n---QUERYING---\n\n')
    fh.write(tabulate(queries, hquery, tablefmt="plain"))
    fh.write('\n')
    fh.write('\n')

fh.close()