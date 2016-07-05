# -*- coding: utf-8 -*-
"""
Created on Thu May 19 14:30:32 2016

@author: Stella Psomadaki
"""
from ConfigParser import ConfigParser
from pointcloud.general import SRID, DIRS
import pointcloud.oracleTools as ora
from tabulate import tabulate
import time
import os

class Validate:
    """This class contains common variables that are used both for the Loading
    anf Querying part."""
    
    def __init__(self, configurationFile):
        """
        Set the database parameters.
        """        
        config = ConfigParser()
        config.read(configurationFile)
        self.configFile = configurationFile
        
        self.dataset = config.get('parameters', 'dataset') #zandmotor, coastline
        self.db = config.get('parameters', 'db')
        self.index = config.getboolean('parameters', 'index') #true, false
        self.init = config.getboolean('parameters', 'init') #true, false
        self.ORCLdirectory = config.get('data-dir', 'ORCLdirectory')
        self.directory = DIRS[self.ORCLdirectory]
        self.reload = config.getboolean('parameters', 'reload') #true, false

        # Database connection        
        self.user = config.get(self.db, 'User')
        self.password = config.get(self.db, 'Pass')
        self.host = config.get(self.db, 'Host')
        self.port = config.get(self.db, 'Port')
        self.database = config.get(self.db, 'Name')
        self.superUserName = config.get(self.db,'SuperUser') 
        self.superPassword = config.get(self.db,'SuperPass')
        
         # Setting the tablespaces
        self.tableSpaceSpatial = config.get('database','tableSpaceSpatial')
        self.tableSpaceIndex = config.get('database','tableSpaceIndex')
        self.tempTableSpace = config.get('database','tempTableSpace')
        self.tableSpaceBtree = config.get('database','tableSpaceBtree')
        
        # Number of processes for loading
        self.numProcesses = config.getint('database','numProcesses')
        
        self.spatialTable = ('valid_' + self.dataset[:3]).upper()
        self.spatialTableRTree = (self.spatialTable + "_idx").upper()
        self.spatialTableBTree = (self.spatialTable + "_btree_idx").upper()
        
        self.cols = ['X', 'Y', 'Z']
        self.lows = [69000, 449000, -100]
        self.uppers = [80000, 460000, 100]
        self.tols = [0.001, 0.001, 0.001]
        self.dim = config.get('parameters', 'dim')
        self.srid = SRID

        
    def sqlldrSpatial(self, tableName):
        """
        Generates the control file for loading spatial data with sqlldr and 
        composes the sqlloader command.
        """
        controlFile = tableName + '.ctl'
        badFile = tableName + '.bad'
        logFile = tableName + '.log'

        ctfile = open(controlFile,'w')
        ctfile.write("""load data
append into table """ + tableName + """
fields terminated by ','
TRAILING NULLCOLS
(
TIME DATE 'YYYY_MM_DD',
geom COLUMN OBJECT
(
SDO_GTYPE INTEGER EXTERNAL,
SDO_SRID INTEGER EXTERNAL,
SDO_POINT COLUMN OBJECT
(
X FLOAT EXTERNAL,
Y FLOAT EXTERNAL,
Z FLOAT EXTERNAL
)
)
)""")
        
        ctfile.close()
        sqlLoaderCommand = "sqlldr " + ora.getConnectString(self.user, self.password, \
        self.host, self.port, self.database) + " direct=true control=" + controlFile + \
        ' data=\\"-\\" bad=' + badFile + " log=" + logFile
        return sqlLoaderCommand
        
    def createSpatialTable(self, cursor, tableName, tableSpaceTable):

        ora.mogrifyExecute(cursor, """
CREATE TABLE """ + tableName + """
(
ID NUMBER GENERATED ALWAYS AS IDENTITY START WITH 1 INCREMENT BY 1,
GEOM SDO_GEOMETRY,
TIME DATE
)
TABLESPACE """ + tableSpaceTable)

        ora.mogrifyExecute(cursor, """
ALTER TABLE """ + tableName + """
ADD CONSTRAINT ID_PK PRIMARY KEY (ID)
""")

    def createRTree(self, cursor, tableName, indexName, dim, tablespace, numProcesses):
        indx_parameters, dims = "", ""
        if dims > 1:
            dims = "sdo_indx_dims=" + str(dim)
        indx_parameters = "PARAMETERS('" + dims + " tablespace=" + tablespace + " layer_gtype=POINT')"
        
        ora.mogrifyExecute(cursor, """
CREATE INDEX """ + indexName + " ON " + tableName + """(GEOM)
INDEXTYPE IS MDSYS.SPATIAL_INDEX
""" + indx_parameters + """
""" + ora.getParallelString(numProcesses))
        
    def dropSpatialIndex(self, cursor, tableName):

        ora.mogrifyExecute(cursor, "DROP INDEX " + tableName + "_idx")
    
    def createIndex(self, cursor, tableName, indexName, tableSpace, numProcesses):
        ora.mogrifyExecute(cursor, """
CREATE INDEX """ + indexName + ' ON ' + tableName  + """(TIME)
""" + ora.getTableSpaceString(tableSpace) + """ 
""" + ora.getParallelString(numProcesses))
    
    def connect(self):
        connection = ora.getConnection(self.user, self.password, self.host, self.port, self.database)
        return connection

    def loadSpatialSqlldr(self):
        connection = self.connect()
        cursor = connection.cursor()
        
        if self.reload:
            self.init = True

        #=======================================================================#
        #                                Loading
        #=======================================================================#
        start = time.time()
        if self.init:
            self.createSpatialTable(cursor, self.spatialTable, self.tableSpaceSpatial)
            
            cursor.execute("""
SELECT TABLE_NAME
FROM user_sdo_geom_metadata
WHERE TABLE_NAME = '""" + self.spatialTable + "'")
            
            if cursor.fetchall()[0]:
                cursor.execute("DELETE FROM user_sdo_geom_metadata t WHERE t.TABLE_NAME = '" + self.spatialTable + "'")
                connection.commit()            
            
            ora.updateSpatialMeta(connection, self.spatialTable, 'GEOM', self.cols, self.lows, self.uppers, self.tols, self.srid)
        else:
            self.dropSpatialIndex(cursor, self.spatialTable)

        sqlldr = self.sqlldrSpatial(self.spatialTable)
        command = """python -m pointcloud.test.las2txyz {0} | """.format(self.configFile) + sqlldr
        
        os.system(command)
        time1 = round(time.time() - start, 4)

        #=======================================================================#
        #                                Indexing
        #=======================================================================#
        if self.index:
            start = time.time()
            self.createRTree(cursor, self.spatialTable, self.spatialTableRTree, self.dim, self.tableSpaceIndex, self.numProcesses)
            time2 = round(time.time() - start, 4)
            time3 = 0 # The database automatically maintains and uses indexes after they are created. 
            if self.init:
                start = time.time()
                self.createIndex(cursor, self.spatialTable, self.spatialTableBTree, self.tableSpaceBtree, self.numProcesses)
                time3 = round(time.time() - start, 4)
        else:
            time2, time3 = 0, 0
        #=======================================================================#
        #                       Gather Statistics Table
        #=======================================================================#
        
        # Non - spatial counterpart
        ora.computeStatistics(cursor, self.spatialTable, self.user)
        
        #=======================================================================#
        #                      Get Sizes Table and Indexes
        #=======================================================================#
        table = float(ora.getSizeTable(cursor, self.spatialTable))
        btree = float(ora.getSizeTable(cursor, self.spatialTableBTree))
        rtree = float(ora.getSizeUserSDOIndexes(cursor, self.spatialTable)) # also, gathers statistics for spatial part
        
        #=======================================================================#
        #                      Get Sizes Number of points
        #=======================================================================#
        points = ora.getNumPoints(connection, cursor, self.spatialTable)
        return time1, time2, time3, table, btree, rtree, points

    
if __name__=="__main__":
    dataset = 'zandmotor'
    path = os.getcwd()   
    configuration = path + '/ini/' + dataset + '/validation_0_3_False_part1.ini'
    validate = Validate(configuration)
    
    load, rtree, btree, sizet, sizeb, sizer, points = validate.loadSpatialSqlldr()

    # print stats    
    hloading = ['load[s]', 'rtree[s]', 'btree[s]',  'table[MB]', 'Btree[MB]', 'Rtree[MB]', 'total[MB]', 'points']
    loading = [[load, rtree, btree, sizet, sizeb, sizer, sizet + sizeb + sizer, points]]
    print tabulate(loading, hloading, tablefmt="plain")