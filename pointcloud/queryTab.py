# -*- coding: utf-8 -*-
"""
Created on Tue Mar 08 13:55:14 2016

@author: Stella Psomadaki
"""
from ConfigParser import ConfigParser
from pointcloud.CommonOracle import Oracle
import pointcloud.oracleTools as ora
import sys
import os
from ast import literal_eval

class QueryTable(Oracle):
    def __init__(self, configuration):
        Oracle.__init__(self, configuration)
        
        config = ConfigParser()
        config.read(configuration)
        
        self.queriesTable = config.get('Querier', 'table')
        
    def createQueriesTable(self):
        """
        Creates the query table where the actual queries posed to the 
        database are stored. Each query has and id, a dataset it belongs to,
        a type, geometry, the date ranges, the type of date querying and the
        range of heights.
        """
        
        connection = self.getConnection(False)
        cursor = connection.cursor()
        ora.mogrifyExecute(cursor, """CREATE TABLE {0} (
ID INTEGER PRIMARY KEY,
DATASET VARCHAR2(50),
TYPE VARCHAR2(50),
GEOMETRY SDO_GEOMETRY,
START_DATE DATE,
END_DATE DATE,
DATE_TYPE VARCHAR(20),
Z_MIN NUMBER,
Z_MAX NUMBER
)""".format(self.queriesTable))

    def getInsertInto(self, data):
        connection = self.getConnection()
        cursor = connection.cursor()
        if data[3] != '':
            cursor.execute("INSERT INTO {0} VALUES (:1, :2, :3, SDO_UTIL.FROM_WKTGEOMETRY(:4), TO_DATE(:5, 'yyyy-mm-dd'), TO_DATE(:6, 'yyyy-mm-dd'), :7, :8, :9)".format(self.queriesTable), data)
        else:
            cursor.execute("INSERT INTO {0} (ID, DATASET, TYPE, START_DATE, END_DATE, DATE_TYPE, Z_MIN, Z_MAX) VALUES (:1, :2, :3, TO_DATE(:4, 'yyyy-mm-dd'), TO_DATE(:5, 'yyyy-mm-dd'), :6, :7, :8)".format(self.queriesTable), [data[0], data[1], data[2], data[4], data[5], data[6], data[7], data[8]])
        connection.commit()

def main(config):
    querier = QueryTable(config)
    querier.createQueriesTable()
    cwd = os.getcwd()
    qfile = open(cwd + '/pointcloud/queries.txt', 'r')
    for i in qfile:
        querier.getInsertInto(literal_eval(i))
    qfile.close()
    

if __name__ == "__main__":
    main(sys.argv[1])



    