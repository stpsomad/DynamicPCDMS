# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 10:25:30 2016

@author: Stella Psomadaki
"""
from pointcloud.AbstractLoader import Loader
import pointcloud.oracleTools as ora

class BulkLoader(Loader):
    def __init__(self, configuration):
        Loader.__init__(self, configuration)
        
    def initialization(self):
        """
        Initialises a bulk loading procedure. 
        It creates the required users, directories.
        """
        connection = ora.connectSYSDBA() #connect as SYSDBA
        cursor = connection.cursor()
        ora.createUser(cursor, self.user, self.password, self.tableSpace, self.tempTableSpace)  
        connection.close()

    def preparation(self):
        """
        1. The data are read from the LAZ files and converted to the Morton codes.
        2. The data are dumped into a heap table.
        """
        if self.reload:
            self.reloadPrep()
        connection = self.getConnection()
        if self.loader == 'external':
            self.extLoaderPrep(connection, self.configFile)
        elif self.loader == 'sqlldr':
            self.sqlldrPrep(connection, self.configFile)
        connection.close()
        
    def loading(self):
        """
        Creates the Index Organized Table.
        """
        connection = self.getConnection()
        if self.loader == 'external':
            self.extLoaderLoading(connection)
        elif self.loader == 'sqlldr':
            self.sqlldrLoading(connection)
            
    def closing(self):
        """
        The required optimizer statistics are gathered.        
        """
        connection = self.getConnection()
        self.computeStatistics(connection.cursor())
        connection.close()
    
    def statistics(self):
        """
        Returns the size of the table and the number of points.
        """
        return self.sizeIOT()