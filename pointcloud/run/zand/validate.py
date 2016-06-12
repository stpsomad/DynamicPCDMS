# -*- coding: utf-8 -*-
"""
Created on Fri May 20 10:47:57 2016

@author: Stella Psomadaki
"""

from pointcloud.test.Validator import Validate
from pointcloud.test.ValidateQuery import Querier
from tabulate import tabulate
import os
import time

##############################
#  Set benchmark parameters  #
dataset = 'zandmotor'
repeat_queries = 6
parallels = [0, 8] # parallel processing
dims = [2, 3] # dimensions for indexing
fresh_reloads = [True, False]
##############################

path = os.getcwd()
approach = ['mini', 'medium', 'large']

if dataset == 'zandmotor':
    bench = 3
elif dataset == 'coastline':
    bench = 4
    
hloading = ['approach','load[s]', 'rtree[s]', 'btree[s]', 'table[MB]', 'Btree[MB]', 'Rtree[MB]', 'total[MB]', 'points']
hquery = ['id', 'fetch_time[s]', 'points', 'filtering_pts']

fh = open('validation_{0}.txt'.format(time.strftime("%d%m%Y")), 'a')
fh.write('Benchmark executed on \n')
fh.write(time.strftime("%d/%m/%Y"))
fh.write('\n')
fh.write('case: validation with Oracle SDO_GEOMETRY\n')
fh.write("""Different approaches examined are:
parallel and not parallel,
indexing in 2 dimensions and 3 dimensions,
fresh reload of the data and not.

--START--
""")

for dim in dims:
    for parallel in parallels:
        for fresh_reload in fresh_reloads:
            loading, queries = [], []
            for benchmark in range(1, bench + 1):
                configuration = path + '/ini/' + dataset + '/validation_part{0}.ini'.format(benchmark)
                validate = Validate(configuration)
                
                #configure the benchmark
                validate.reload = fresh_reload
                validate.dim = dim
                validate.numProcesses = parallel
                
                connection = validate.connect()
                cursor = connection.cursor()
                
                # check if the table exists, if not it is created
                cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[(validate.spatialTable).upper(),])
                length = len(cursor.fetchall())
                if length and validate.reload:
                    cursor.execute('DROP TABLE {0} PURGE'.format((validate.spatialTable).upper()))
                               
                load, rtree, btree, sizet, sizeb, sizer, points = validate.loadSpatialSqlldr()
                loading.append([approach[benchmark-1], load, rtree, btree, sizet, sizeb, sizer, sizet + sizeb + sizer, points])
            
                #querying    
                querier = Querier(configuration)
                querier.numProcesses = parallel
                
                connection = querier.connect()
                cursor = connection.cursor()
                
                # check if the query table exists, if not it is created
                cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[querier.queriesTable.upper(),])
                length = len(cursor.fetchall())
                if not length:
                    os.system('python -m pointcloud.queryTab {0}'.format(configuration))
                
                for query in querier.ids:
                    for i in range(repeat_queries):
                        t, points, filtering_pts = querier.query(query)
                        queries.append([query, t, points, filtering_pts])
                        
                        querier.dropQueryTable(cursor, query) #drop the table
                
            #cursor.execute('DROP TABLE {0} PURGE'.format((validate.spatialTable).upper()))
            
            # print stats
            print """\n\n
---Case----
parallel: {0}
dimensions indexed: {1}
fresh reload: {2}\n\n""". format(parallel, dim, fresh_reload)
            fh.write('\n---LOADING---\n')
            print tabulate(loading, hloading, tablefmt="plain")
            fh.write('\n---QUERYING---\n')
            print tabulate(queries, hquery, tablefmt="plain")
            
            #write in file
            fh.write("""\n\n
---Case----
parallel: {0}
dimensions indexed: {1}
fresh reload: {2}\n\n""". format(parallel, dim, fresh_reload))
            
            fh.write(tabulate(loading, hloading, tablefmt="plain"))
            fh.write('\n')
            fh.write(tabulate(queries, hquery, tablefmt="plain"))
            fh.write('\n')
            fh.write('\n')
            
fh.close()