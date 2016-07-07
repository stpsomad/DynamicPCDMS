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
        
    def updateSRID(self, srid):
        """
        Updates the SRID of the query table to the specified one.
        """
        connection = ora.getConnection(self.user, self.password, self.host, self.port, self.database)
        cursor = connection.cursor()
        
        ora.mogrifyExecute(cursor, """UPDATE (SELECT GEOMETRY
FROM """ + self.queriesTable + """
WHERE GEOMETRY IS NOT NULL) t
SET t.GEOMETRY.SDO_SRID = """ + str(srid))
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
    
    def create3DQuerier(self):
        connection = ora.getConnection(self.user, self.password, self.host, self.port, self.database)
        cursor = connection.cursor()
        
        ora.mogrifyExecute(cursor, "CREATE TABLE " + self.queriesTable + """
AS SELECT * 
FROM QUERIES
WHERE ID <= 12""")


    def Extrude3D(self):
        connection = ora.getConnection(self.user, self.password, self.host, self.port, self.database)
        cursor = connection.cursor()
        ora.mogrifyExecute(cursor, """
DECLARE
  GEOM_2D SDO_GEOMETRY;
  Q_TYPE VARCHAR2(50);
  GEOM_3D SDO_GEOMETRY;
  VALID VARCHAR2(50);

BEGIN
  UPDATE (SELECT GEOMETRY
  FROM {0}
  WHERE GEOMETRY IS NOT NULL) t 
  SET t.GEOMETRY.SDO_SRID = NULL;
  COMMIT;

  FOR i IN 1..12 LOOP
    SELECT Q.TYPE INTO Q_TYPE
    FROM QUERIES Q
    WHERE Q.ID = i;
    
    IF (LOWER(Q_TYPE) = 'space - time' OR LOWER(Q_TYPE) = 'space') THEN
      SELECT Q.GEOMETRY INTO GEOM_2D
      FROM {0} Q
      WHERE Q.ID = i;
      
       GEOM_3D := SDO_UTIL.EXTRUDE(GEOM_2D, 
                                  SDO_NUMBER_ARRAY(-100),
                                  SDO_NUMBER_ARRAY(100),
                                  0.001);
                                  
      
        dbms_output.put_line('Updating query ' || i);
        UPDATE (SELECT GEOMETRY FROM {0} WHERE ID=i) SET GEOMETRY = GEOM_3D;
  END IF;
  END LOOP;
  COMMIT;
  
  UPDATE (SELECT GEOMETRY
  FROM {0}
  WHERE GEOMETRY IS NOT NULL) t 
  set t.GEOMETRY.SDO_SRID = 28992;
  COMMIT;
END;
/""".format(self.queriesTable.upper()))


    def fixOrientation(self):
        connection = ora.getConnection(self.user, self.password, self.host, self.port, self.database)
        cursor = connection.cursor()
        cursor.execute("""
DECLARE
  GEOM_2D SDO_GEOMETRY;
  Q_TYPE VARCHAR2(50);
  LINE SDO_GEOMETRY;
  LINE_REV SDO_GEOMETRY;
  poly SDO_GEOMETRY;
  WKTGEOM CLOB;

BEGIN
  FOR i IN 1..12 LOOP
    SELECT Q.TYPE INTO Q_TYPE
    FROM QUERIES Q
    WHERE Q.ID = i;
    
    IF (LOWER(Q_TYPE) = 'space - time' OR LOWER(Q_TYPE) = 'space') THEN
      SELECT Q.GEOMETRY INTO GEOM_2D
      FROM QUERIES Q
      WHERE Q.ID = i;
        
      LINE := SDO_UTIL.POLYGONTOLINE(GEOM_2D);
      LINE_REV := SDO_UTIL.REVERSE_LINESTRING(LINE);
      
      wktgeom := 'POLYGON (' || SUBSTR (SDO_UTIL.TO_WKTGEOMETRY(LINE_REV), 12) || ')';
      DBMS_OUTPUT.PUT_LINE(to_char(wktgeom));
      poly := SDO_UTIL.FROM_WKTGEOMETRY(wktgeom);
      
      UPDATE (SELECT GEOMETRY FROM queries WHERE ID=i) SET GEOMETRY = poly;
    END IF;
  END LOOP;
  COMMIT;
END;
/""") 

        
def main(config):
    querier = QueryTable(config)
    
    if querier.queriesTable.lower() == 'queries_3d':
        querier.create3DQuerier()
        querier.Extrude3D()
    else:        
        querier.createQueriesTable()
        cwd = os.getcwd()
        qfile = open(cwd + '/pointcloud/queries.txt', 'r')
        for i in qfile:
            querier.getInsertInto(literal_eval(i))
        qfile.close()
        
        #update to specified SRID
        querier.updateSRID(querier.srid)
        querier.fixOrientation()
        #update the geometry metadata
        querier.updateSpatialMeta()

if __name__ == "__main__":
    main(sys.argv[1])



    