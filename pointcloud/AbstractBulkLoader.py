# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 10:25:30 2016

@author: Stella Psomadaki
"""
from pointcloud.AbstractLoader import Loader
import pointcloud.oracleTools as Ora

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
            self.sqlldrPrep(connection, self.configFile) #dropping IOT and resorting available only for sqlldr
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