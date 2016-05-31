# -*- coding: utf-8 -*-
"""
Created on Sat Feb 13 19:13:05 2016

@author: Stella Psomadaki

Adapted from: https://github.com/NLeSC/pointcloud-benchmark/blob/master/python/pointcloud/oracle/CommonOracle.py
Author: Oscar Martinez Rubi
Apache License
Version 2.0, January 2004
"""

from ConfigParser import ConfigParser
from cx_Oracle import connect
import pointcloud.general as general

class Oracle:
    """This class contains common variables that are used both for the Loading
    anf Querying part."""
    
    def __init__(self, configurationFile):
        """
        Set the database parameters.
        """        
        config = ConfigParser()
        config.read(configurationFile)
        self.configFile = configurationFile
        
        # Setting up the option
        self.format = config.get('benchmark-options', 'format')
        self.clustering = config.get('benchmark-options', 'clustering') #Morton
        self.dataset = config.get('benchmark-options', 'dataset') #zandmotor, coastline
        self.db = config.get('benchmark-options', 'db')
        self.parse = config.get('benchmark-options', 'parse') #xyt, xyzt
        self.loader = config.get('benchmark-options', 'loader') #sqlldr, external, incremental
        self.integration = config.get('benchmark-options', 'integration') #loose, deep
        self.ORCLdirectory = config.get('data-dir', 'ORCLdirectory')
        self.directory = general.DIRS[self.ORCLdirectory]
        self.init = config.getboolean('benchmark-options', 'init') #true, false
        self.update = config.get('benchmark-options', 'update') #dump, union, resort
        self.scale = config.getint('benchmark-options', 'scale')
        self.granularity = config.get('benchmark-options', 'granularity') #day, year
        self.reload = config.getboolean('benchmark-options', 'reload') #true, false
        
        if self.integration not in ['deep', 'loose']:
            raise Exception('ERROR: Not supported data structure')
        if self.clustering.lower() not in ['morton']:
            raise Exception('ERROR: Currently only supporting morton')
        if self.format not in general.PC_FILE_FORMATS:
            raise Exception('ERROR: Not supported format. Use either las or laz')
        if self.parse.lower() not in ['xyt', 'xyzt']:
            raise Exception('ERROR: Cannot parse specified object. Use either xyt or xyzt')
        
        # Database connection        
        self.user = config.get(self.db, 'User')
        self.password = config.get(self.db, 'Pass')
        self.host = config.get(self.db, 'Host')
        self.port = config.get(self.db, 'Port')
        self.database = config.get(self.db, 'Name')
        self.superUserName = config.get(self.db,'SuperUser') 
        self.superPassword = config.get(self.db,'SuperPass')
        
        # Setting the tablespaces
        self.tableSpaceHeap = config.get('database','tableSpaceHeap')
        self.tableSpaceIOT = config.get('database','tableSpaceIOT')
        self.tempTableSpace = config.get('database','tempTableSpace')
        # Number of processes for loading
        self.numProcesses = config.getint('database','numProcesses')
        
        # table columns
        if self.integration.lower() == "loose":
            if self.parse.lower() == 'xyt':
                self.columns = config.items('columns-loose-xyt')
            else:
                self.columns = config.items('columns-loose-xyzt')
            self.index = 'time, morton'
        elif self.integration.lower() == "deep":
            if self.parse.lower() == 'xyt':
                self.columns = config.items('columns-deep-xyt')
            else:
                self.columns = config.items('columns-deep-xyzt')
            self.index = 'morton'
        self.cols = self.eqColumns() #e.g. ['m', 't', 'z']
        self.columnNames = [i[0] for i in self.columns]
        
        # name depending on the type of loader
        if self.loader == 'external':
            self.tableName = self.dataset[0] + self.integration[0] + self.parse + str(self.scale) +"_temp_ext"
            self.iotTableName = self.dataset[0] + self.integration[0] + self.parse + str(self.scale) + '_ext' 
            self.metaTable = 'meta' + self.iotTableName
        elif self.loader == 'sqlldr':
            self.tableName = self.dataset[0] + self.integration[0] + self.parse + str(self.scale) +"_temp"
            self.iotTableName = self.dataset[0] + self.integration[0] + self.parse + str(self.scale)
            self.metaTable = 'meta_' + self.iotTableName
        elif self.loader == 'incremental':
            self.iotTableName = self.dataset[0] + self.integration[0] + self.parse + str(self.scale) + "_incr"
            self.metaTable = 'meta' + self.iotTableName
        
            
    def eqColumns(self):
        """
        Performs the following operation MORTON, TIME, Z -> ['m', 't', 'z']
        """
        cols = []
        for i in self.columns:
            cols.append(general.EQ_DIM[i[0]])
        return cols
    
    def getDBColumn(self, index, includeType = False):
        """
        Get the column name used by the heap table or external table."""
        column = self.cols[index]
        if column not in general.DM_FLAT:
            raise Exception('Wrong column!' + column)
        columnName = general.REVERSE_DIM[column]
        if includeType:
            return (columnName, general.DM_FLAT[column])
        else:
            return (columnName,)
    
    def getConnectString(self, superUser = False):
        """
        Gets a connection string to establish a database connection.
        """
        if not superUser:
            return self.user + "/" + self.password + "@//" + self.host + ":" + self.port + "/" + self.database
        else:
            return self.superUserName + "/" + self.superPassword + "@//" + self.host + ":" + self.port + "/" + self.database
        
    def getConnection(self, superUser = False):
        """
        Establishes connection to an Oracle database.
        """
        try:
            return connect(self.getConnectString(superUser))
        except:
            print "Connection could not be established"