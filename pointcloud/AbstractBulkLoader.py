# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 10:25:30 2016

@author: Stella Psomadaki
"""
from pointcloud.AbstractLoader import Loader
import time
import pointcloud.oracleTools as Ora
from tabulate import tabulate # external

class BulkLoader(Loader):
    def __init__(self, configuration):
        Loader.__init__(self, configuration)
        
    def initialization(self):
        """Initialises a bulk loading procedure. It creates the required users, directories"""
        connection = Ora.connectSYSDBA() #connect as SYSDBA
        cursor = connection.cursor()
        Ora.createUser(cursor, self.user, self.password, self.tableSpace, self.tempTableSpace)  
        connection.close()

    def preparation(self):
        connection = self.getConnection()
        if self.loader == 'external':
            self.extLoaderPrep(connection, self.configFile)
        elif self.loader == 'sqlldr':
            self.sqlldrPrep(connection, self.configFile)
            #dropping IOT and resorting available only for sqlldr
        connection.close()
        
    def loading(self):
        connection = self.getConnection()
        if self.loader == 'external':
            self.extLoaderLoading(connection)
        elif self.loader == 'sqlldr':
            self.sqlldrLoading(connection)
            
    def closing(self):
        connection = self.getConnection()
        self.computeStatistics(connection.cursor())
        connection.close()
    
    def statistics(self):
        return self.sizeIOT()
        
if __name__ == "__main__":
    
    times = []
    benchmark = ['mini', 'medium', 'full']  
    headers = ['benchmark', 'initialisation', 'preparation', 'loading',
               'closing', 'size [MB]', 'points']
    
    for i in range(1,4):
        configuration = 'D:/Dropbox/Thesis/Thesis/Code/ini/dxyt_10000_part{0}.ini'.format(i)
        bulk = BulkLoader(configuration)
        temp = []
        temp.append(benchmark[i - 1])
        
        temp.append(0) #no initialisation
        
        start = time.clock()
        bulk.preparation()
        temp.append(round(time.clock() - start, 2))
        
        start = time.clock()
        bulk.loading()
        temp.append(round(time.clock() - start, 2))
        
        start = time.clock()
        bulk.closing()
        temp.append(round(time.clock() - start, 2))
       
        size, points = bulk.statistics()
        temp.append(round(size,2))
        temp.append(int(points))
        
        times.append(temp)

    print tabulate(times, headers, tablefmt="plain")

    f = open('loading.txt', 'w')
    f.write(tabulate(times, headers, tablefmt="plain"))        
    f.close()