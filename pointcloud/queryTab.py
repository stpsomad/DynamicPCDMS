# -*- coding: utf-8 -*-
"""
Created on Tue Mar 08 13:55:14 2016

@author: Stella Psomadaki
"""
from ConfigParser import ConfigParser
import pointcloud.oracleTools as ora
from pointcloud.general import SRID
import sys
import os
from ast import literal_eval

class QueryTable(object):
    def __init__(self, configuration):     
        config = ConfigParser()
        config.read(configuration)
        
        position = config.sections()[0]
        self.db = config.get(position, 'db')
        
        self.user = config.get(self.db, 'User')
        self.password = config.get(self.db, 'Pass')
        self.host = config.get(self.db, 'Host')
        self.port = config.get(self.db, 'Port')
        self.database = config.get(self.db, 'Name')
        self.superUserName = config.get(self.db,'SuperUser') 
        self.superPassword = config.get(self.db,'SuperPass')
        
        self.queriesTable = config.get('Querier', 'table').upper()

        self.cols = ['X', 'Y', 'Z']
        self.lows = [12628, 308179, -200]
        self.uppers = [283594, 611063, 200]
        self.tols = [0.001, 0.001, 0.001]
        self.srid = SRID
        
    def createQueriesTable(self):
        """
        Creates the query table where the actual queries posed to the 
        database are stored. Each query has and id, a dataset it belongs to,
        a type, geometry, the date ranges, the type of date querying and the
        range of heights.
        """
        
        connection = ora.getConnection(self.user, self.password, self.host, self.port, self.database)
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
        connection = ora.getConnection(self.user, self.password, self.host, self.port, self.database)
        cursor = connection.cursor()
        if data[3] != '':
            cursor.execute("INSERT INTO {0} VALUES (:1, :2, :3, SDO_UTIL.FROM_WKTGEOMETRY(:4), TO_DATE(:5, 'yyyy-mm-dd'), TO_DATE(:6, 'yyyy-mm-dd'), :7, :8, :9)".format(self.queriesTable), data)
        else:
            cursor.execute("INSERT INTO {0} (ID, DATASET, TYPE, START_DATE, END_DATE, DATE_TYPE, Z_MIN, Z_MAX) VALUES (:1, :2, :3, TO_DATE(:4, 'yyyy-mm-dd'), TO_DATE(:5, 'yyyy-mm-dd'), :6, :7, :8)".format(self.queriesTable), [data[0], data[1], data[2], data[4], data[5], data[6], data[7], data[8]])
        connection.commit()
        
    def updateSRID(self):
        """
        Updates the SRID of the query table to the specified one.
        """
        connection = ora.getConnection(self.user, self.password, self.host, self.port, self.database)
        cursor = connection.cursor()
        
        ora.mogrifyExecute(cursor, """UPDATE (SELECT GEOMETRY
FROM """ + self.queriesTable + """
WHERE GEOMETRY IS NOT NULL) t
SET t.GEOMETRY.SDO_SRID = """ + str(self.srid))
        connection.commit()
    
    def updateSpatialMeta(self):
        connection = ora.getConnection(self.user, self.password, self.host, self.port, self.database)
        cursor = connection.cursor()
        cursor.execute("""
select TABLE_NAME
from user_sdo_geom_metadata
WHERE TABLE_NAME = '""" + self.queriesTable + "'")
        
        if cursor.fetchall()[0]:
            cursor.execute("DELETE FROM user_sdo_geom_metadata t WHERE t.TABLE_NAME = '" + self.queriesTable + "'")
            connection.commit()

        ora.updateSpatialMeta(connection, self.queriesTable, 'GEOMETRY', self.cols, self.lows, self.uppers, self.tols, self.srid)
    

def main(config):
    querier = QueryTable(config)
    querier.createQueriesTable()
    cwd = os.getcwd()
    qfile = open(cwd + '/pointcloud/queries.txt', 'r')
    for i in qfile:
        querier.getInsertInto(literal_eval(i))
    qfile.close()
    
    #update to specified SRID
    querier.updateSRID()
    #update the geometry metadata
    querier.updateSpatialMeta()

if __name__ == "__main__":
    main(sys.argv[1])



    