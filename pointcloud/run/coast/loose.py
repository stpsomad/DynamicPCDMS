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
dataset = 'coastline'
integrations = ['lxyt', 'lxyzt']
scaling = '1'
repeatQueries = 6
###########################

bench = 3

fh = open('coast_non-int_{0}.txt'.format(time.strftime("%d%m%Y")), 'a')
fh.write('Benchmark executed on ')
fh.write(time.strftime("%d/%m/%Y"))
fh.write('\n')
fh.write(
"""CASE: Non - integrated approach (loose) with scale of 1 for the coastline dataset

Different approaches examined are:
    * z as an attribute and as part of the morton key

The queries are repeated 6 times

--START--\n\n\n
""")   

path = os.getcwd()
benchmark = ['mini', 'medium', 'full']  
hloading = ['approach', 'preparation', 'loading', 'closing', 'size[MB]', 'points']
hquery =  ["id", "prep.", 'insert', 'ranges', 'Levels', 'fetching', "decoding", 'storing', "Appr.pts", "Fin.pts", "FinFilt", "time", 'extra%', 'total']


for integr in integrations:
    loadings = []
    queries = []
    for i in range(1,bench + 1):
        configuration = path + '/ini/' + dataset + '/' + integr + '_' + scaling + '_part' + str(i) + '.ini'

        bulk = BulkLoader(configuration)
        connection = bulk.getConnection()
        cursor = connection.cursor()
        if i == 1:
            cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[bulk.iotTableName.upper(),])
            length = len(cursor.fetchall())
            if length:
                cursor.execute("DROP TABLE " + bulk.iotTableName + " PURGE")
            
            cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[bulk.metaTable.upper(),])
            length = len(cursor.fetchall())
            if length:
                cursor.execute("DROP TABLE " + bulk.metaTable + " PURGE")
        
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
  
        querier = Querier(configuration)
        connection = querier.getConnection()
        cursor = connection.cursor()
        
        cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[querier.queriesTable.upper(),])
        length = len(cursor.fetchall())
        if not length:
            os.system('python -m pointcloud.queryTab {0}'.format(configuration))

        for num in querier.ids:
            for j in range(repeatQueries):
                start = time.time()
                lst = querier.query(num)
                lst.append(round(time.time() - start, 2))
                lst.append(round((lst[7] - lst[8])/float(lst[8])*100,2))
                lst.append(round(lst[1]+lst[4]+lst[5]+lst[6]+lst[9],2))
                lst.insert(0, num)
                queries.append(lst)
                ora.dropTable(cursor, querier.queryTable + '_' +  str(num))               
    print
    print 'integration: {0}\n\n'.format(integr)
    print tabulate(loadings, hloading, tablefmt="plain")
    print
    print tabulate(queries, hquery, tablefmt="plain")

    fh.write('\n---LOADING---\n')
    fh.write(tabulate(loadings, hloading, tablefmt="plain"))
    fh.write('\n---QUERYING---\n')
    fh.write(tabulate(queries, hquery, tablefmt="plain"))
    fh.write('\n')
    fh.write('\n')

fh.close()