# -*- coding: utf-8 -*-
"""
Created on Mon Feb 8 10:13:05 2016

@author: Stella Psomadaki

Adapted from: https://github.com/NLeSC/pointcloud-benchmark/blob/master/python/pointcloud/oracle/AbstractLoader.py
Author: Oscar Martinez Rubi
Apache License
Version 2.0, January 2004
"""

import pointcloud.general as general
import pointcloud.oracleTools as ora
import os
import numpy as np
from CommonOracle import Oracle

class Loader(Oracle):
    def __init__(self, configuration):
        Oracle.__init__(self, configuration)
    
    def getParallelString(self, numProcesses):
        parallelString = ''
        if numProcesses > 1:
            parallelString = ' parallel ' + str(numProcesses) + ' '
        return parallelString 

    def createUser(self):
        connectionSuper = self.getConnection(True)
        cursorSuper = connectionSuper.cursor()
        ora.createUser(cursorSuper, self.user, self.password, self.tableSpace, self.tempTableSpace)
        connectionSuper.close()
    
    def createLASDirectory(self, lasDirVariableName, parentFolder):
        connectionSuper = self.getConnection(True)
        cursorSuper = connectionSuper.cursor()
        ora.createDirectory(cursorSuper, lasDirVariableName, parentFolder, self.userName)
        connectionSuper.close()

    def getDBColumn(self, index, includeType = False):
        column = self.cols[index]
        if column not in general.DM_FLAT:
            raise Exception('Wrong column!' + column)
        columnName = 'VAL_D' + str(index+1)
        if includeType:
            return (columnName, general.DM_FLAT[column])
        else:
            return (columnName,)
    
    def getHeapColumns(self):
        heapCols = []
        for i in range(len(self.cols)):
            heapCols.append(self.getDBColumn(i)[0] + ' ' + general.DM_FLAT[self.cols[i]])
        return heapCols
    
    def heapCols(self):
        heapCols = []
        for i in range(len(self.cols)):
            heapCols.append(self.getDBColumn(i)[0])
        return heapCols
        
    def getTableSpaceString(self, tableSpace):
        if tableSpace != None and tableSpace != '':
            return " TABLESPACE " + tableSpace + " "
        else: 
            return ""
    
    def sqlldr(self, fileAbsPath, tableName):
        commonFile = os.path.basename(fileAbsPath).replace(fileAbsPath.split('.')[-1],'')
        controlFile = commonFile + '.ctl'
        badFile = commonFile + '.bad'
        logFile = commonFile + '.log'
        
        ctfile = open(controlFile,'w')
        sqlldrCols = []
        for i in range(len(self.cols)):
            column = self.cols[i]
            if column not in general.DM_SQLLDR:
                raise Exception('Wrong column! ' + column)
            sqlldrCols.append(self.getDBColumn(i)[0] + ' ' + general.DM_SQLLDR[column][0] + ' external(' + str(general.DM_SQLLDR[column][1]) + ')')
        
        ctfile.write("""load data
append into table """ + tableName + """
fields terminated by ','
(
""" + (',\n'.join(sqlldrCols)) + """
)""")
        
        ctfile.close()
        sqlLoaderCommand = "sqlldr " + self.getConnectString() + " direct=true control=" + controlFile + ' data=\\"-\\" bad=' + badFile + " log=" + logFile
        return sqlLoaderCommand
        
    def createFlatTable(self, cursor, tableName, tableSpace):
        """ Creates a empty flat table"""
        ora.dropTable(cursor, tableName, True)
        
        ora.mogrifyExecute(cursor,"""
CREATE TABLE """ + tableName + """ (""" + (',\n'.join(self.getHeapColumns())) + """) """ + self.getTableSpaceString(tableSpace) + """ pctfree 0 nologging""")

    def createExternalTable(self, cursor, txtFiles, tableName, txtDirVariableName, numProcesses):
        ora.dropTable(cursor, tableName, True)
        print  """
CREATE TABLE """ + tableName + """ (""" + (',\n'.join(self.getHeapColumns())) + """)
organization external
(
type oracle_loader
default directory """ + txtDirVariableName + """
access parameters (
    records delimited by newline
    fields terminated by ', ')
location ('""" + txtFiles + """')
)
""" + self.getParallelString(numProcesses) + """ reject limit 0"""

        ora.mogrifyExecute(cursor, """
CREATE TABLE """ + tableName + """ (""" + (',\n'.join(self.getHeapColumns())) + """)
organization external
(
type oracle_loader
default directory """ + txtDirVariableName + """
access parameters (
    records delimited by newline
    fields terminated by ', ')
location ('""" + txtFiles + """')
)
""" + self.getParallelString(numProcesses) + """ reject limit 0""")     
        
    def createIOTTable(self, cursor, iotTableName, tableName, tableSpace, numProcesses):
        """ Create Index-Organized-Table and populate it from tableName Table"""
        ora.dropTable(cursor, iotTableName, True)
        
        cls = [', '.join(i[0] for i in self.columns)]
        
        ora.mogrifyExecute(cursor, """
CREATE TABLE """ + iotTableName + """
(""" + (', '.join(cls)) + """, 
    constraint """ + iotTableName + """_PK primary key (""" + self.index + """))
    organization index""" + self.getTableSpaceString(tableSpace) + """
    pctfree 0 nologging
""" + self.getParallelString(numProcesses) + """
as
    SELECT """ + (', '.join(self.heapCols())) + """ FROM """ + tableName)
    
    def computeStatistics(self, cursor):
        #COMPUTE STATISTICS instructs Oracle Database to compute exact statistics about the analyzed object and store them in the data dictionary.
        #Both computed and estimated statistics are used by the Oracle Database optimizer to choose the execution plan for SQL statements that access analyzed objects.
        ora.mogrifyExecute(cursor, "ANALYZE TABLE " + self.iotTableName + "  compute system statistics for table")
        # http://docs.oracle.com/cd/B28359_01/appdev.111/b28419/d_stats.htm#i1036461
        ora.mogrifyExecute(cursor,"""
BEGIN
    dbms_stats.gather_table_stats('""" + self.user + """','""" + self.iotTableName + """',NULL,NULL,FALSE,'FOR ALL COLUMNS SIZE AUTO',8,'ALL');
END;""")


    def extLoaderPrep(self, connection, configuration):
        cursor = connection.cursor()
        #Create meta table if it does not exist already
        if self.init:
            ora.createMetaTable(cursor, self.metaTable, True)
        
        #Run the morton converter
        command = """python -m pointcloud.mortonConverter {0}""".format(configuration)
        os.system(command)
        self.createExternalTable(cursor, '*.txt', self.tableName, self.ORCLdirectory, self.numProcesses)
    
    def extLoaderLoading(self, connection):
        cursor = connection.cursor()
        if self.init:
            self.createIOTTable(cursor, self.iotTableName, self.tableName, self.tableSpace, self.numProcesses)
        else:
            ora.appendData(connection, cursor, self.iotTableName, self.tableName)
        ora.dropTable(cursor, self.tableName, check = False)
    
    def sqlldrPrep(self, connection, configuration):
        cursor = connection.cursor()
        
        self.createFlatTable(cursor, self.tableName, self.tableSpace)
        if self.init:
            ora.createMetaTable(cursor, self.metaTable, True)
            
        commnandsqlldr = self.sqlldr(self.directory, self.tableName)
        command = """python -m pointcloud.mortonConverter {0} | """.format(configuration) + commnandsqlldr
        os.system(command)
        
    def sqlldrLoading(self, connection):
        cursor = connection.cursor()
        if self.init:
            self.createIOTTable(cursor, self.iotTableName, self.tableName, self.tableSpace, self.numProcesses)
        else:
            if self.update == 'dump':
                ora.appendData(connection, cursor, self.iotTableName, self.tableName)
            else:
                self.rebuildIOT(connection)
        cursor.execute('DROP TABLE {0}'.format(self.tableName))
    
    def rebuildIOT(self, connection):
        cursor = connection.cursor()
        ora.appendData(connection, cursor, self.tableName, self.iotTableName)
        ora.dropTable(cursor, self.iotTableName, check = False)
        self.createIOTTable(cursor, self.iotTableName, self.tableName, self.tableSpace, self.numProcesses)

    def sizeIOT(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        size_total = ora.getSizeTable(cursor, self.iotTableName)    
        number_total = ora.getNumPoints(connection, cursor, self.iotTableName)
        connection.close()
        return size_total, number_total
        
    def round2resolution(array, predicate):
        return np.round(array - np.mod(predicate + array, predicate), 0)