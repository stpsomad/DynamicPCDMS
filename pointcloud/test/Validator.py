# -*- coding: utf-8 -*-
"""
Created on Thu May 19 14:30:32 2016

@author: Stella Psomadaki
"""
from cx_Oracle import connect
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
        
        self.spatialTable = 'valid_' + self.dataset[:3]
        
        self.cols = ['X', 'Y', 'Z']
        self.lows = [69000, 449000, -100]
        self.uppers = [80000, 460000, 100]
        self.tols = [0.0005, 0.0005, 0.0005]
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
        sqlLoaderCommand = "sqlldr " + self.getConnectString() + " direct=true control=" + controlFile + ' data=\\"-\\" bad=' + badFile + " log=" + logFile
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

    def createRTree(self, cursor, tableName, dim, tablespace, numProcesses):
        indx = ''
        dims = ''
        if dims > 1:
            dims = "sdo_indx_dims=" + str(dim)
        indx = "PARAMETERS('" + dims + " tablespace=" + tablespace + "') "
        
        ora.mogrifyExecute(cursor, """
CREATE INDEX """ + tableName + "_idx ON " + tableName + """(GEOM)
INDEXTYPE IS MDSYS.SPATIAL_INDEX """ + indx + getParallelString(numProcesses) + """
""")

    def rebuildIndex(self, cursor, tableName, numProcesses):
        ora.mogrifyExecute(cursor, """ALTER INDEX """ + tableName + "_idx REBUILD" + getParallelString(numProcesses))
        
    def dropSpatialIndex(self, cursor, tableName):
        ora.mogrifyExecute(cursor, "DROP INDEX " + tableName + "_idx")
    
    def createIndex(self, cursor, tableName, tableSpace, numProcesses):
        ora.mogrifyExecute(cursor, """
CREATE INDEX """ + tableName + '_btree_idx ON ' + tableName  + """(TIME)
""" + getTableSpaceString(tableSpace) + getParallelString(numProcesses))
    
    def composeDIM_ELEMENT(self, name, low, upper, tolerance):
        return """SDO_DIM_ELEMENT
(
'{0}',
{1},
{2},
{3}
)""".format(name, low, upper, tolerance)

    def updateSpatialMeta(self, connection, tableName, dim, cols, lows, uppers, tols, srid):
        cursor = connection.cursor()
        
        diminfo = """,
""".join([self.composeDIM_ELEMENT(cols[i], lows[i], uppers[i], tols[i]) for i in range(len(cols))])
        
        ora.mogrifyExecute(cursor, """
INSERT INTO user_sdo_geom_metadata
(table_name, column_name, srid, diminfo)
VALUES
(
'""" + tableName + """',
'GEOM',
""" + str(srid) + """,
SDO_DIM_ARRAY
(
""" + diminfo + """
)
)""")
        connection.commit()


    def loadSpatialSqlldr(self, connection):
        cursor = connection.cursor()
        
        #=======================================================================#
        #                                Loading
        #=======================================================================#
        start = time.time()
        if self.init:
            self.createSpatialTable(cursor, self.spatialTable, self.tableSpaceSpatial)
            self.updateSpatialMeta(connection, self.spatialTable, self.dim, self.cols, self.lows, self.uppers, self.tols, self.srid)
        else:
            self.dropSpatialIndex(cursor, self.spatialTable)

        sqlldr = self.sqlldrSpatial(self.spatialTable)
        command = """python -m pointcloud.las2txyz {0} | """.format(self.configFile) + sqlldr
        os.system(command)
        time1 = time.time() - start

        #=======================================================================#
        #                                Indexing
        #=======================================================================#
        if self.index:
            start = time.time()
            self.createRTree(cursor, self.spatialTable, self.dim, self.tableSpaceIndex, self.numProcesses)
            time2 = time.time() - start
            time3 = 0
            if self.init:
                start = time.time()
                self.createIndex(cursor, self.spatialTable, self.tableSpaceBtree, self.numProcesses)
                time3 = time.time() - start
        else:
            time2, time3 = 0, 0
        return time1, time2, time3
            
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
            
def getParallelString(numProcesses):
    if numProcesses > 1:
        return ' PARALLEL ' + str(numProcesses)
    return ''
    
def getTableSpaceString(tableSpace):
    """
    Generates the TABLESPACE predicate of the SQL query.
    """
    if tableSpace is not None and tableSpace != '':
        return " TABLESPACE " + tableSpace + " "
    else: 
        return ""
    
if __name__=="__main__":
    dataset = 'zandmotor'
    path = os.getcwd()   
    configuration = path + '/ini/' + dataset + '/validation.ini'
    validate = Validate(configuration)
    connection = validate.getConnection()
    
    load, rtree, btree = validate.loadSpatialSqlldr(connection)

    # print stats    
    hloading = ['load', 'rtree', 'btree']
    loading = [[load, rtree, btree]]
    print tabulate(loading, hloading, tablefmt="plain")
