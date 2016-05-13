# -*- coding: utf-8 -*-
"""
Created on Fri May 13 16:24:59 2016

@author: psomadak
"""

from pointcloud.AbstractBulkLoader import BulkLoader
import time
from tabulate import tabulate
import os

###########################
dataset = 'zandmotor'
integrations = ['lxyt']
scalings = ['1']
###########################

if dataset == 'zandmotor':
    bench = 3
elif dataset == 'coastline':
    bench = 4
    

path = os.getcwd()
benchmark = ['mini', 'medium', 'full']  
hloading = ['approach', 'preparation', 'loading', 'closing', 'size[MB]', 'points']


for integr in integrations:
    for scaling in scalings:
        loadings = []
        queries = []
        for i in range(1,bench + 1):
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
            loading.append(str(0))
           
            size, points = bulk.statistics()
            loading.append(round(size,2))
            loading.append(int(points))
            
            loadings.append(loading)
            print tabulate(loadings, hloading, tablefmt="plain")
            