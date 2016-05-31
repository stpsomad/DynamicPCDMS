# -*- coding: utf-8 -*-
"""
Created on Mon May 23 11:13:39 2016

@author: Stella Psomadaki
"""
from pointcloud.test.Validator import Validate
from ConfigParser import ConfigParser
import pointcloud.oracleTools as ora
import time
import os

class Querier(Validate):
    def __init__(self, configuration):
        Validate.__init__(self, configuration)
        config = ConfigParser()
        config.read(configuration)                
        self.queriesTable = config.get('Querier', 'table')
        self.tolerance = config.get('Querier', 'tolerance')
        self.numProcesses = config.getint('Querier', 'numProcesses')
        self.tableSpace = config.get('Querier', 'tableSpace')
        self.ids = config.get('Querier', 'id').replace(' ', '').split(',')
        
        self.queryResult = self.spatialTable + "_res"
        
    def composeQuery(self, qid, operator, qtype, timeType, count=False):
        if count:
            select = "SELECT COUNT(*)"
        else:
            select = "SELECT"  + ora.getHintStatement([ora.getParallelStringQuery(self.numProcesses)]) + " t.GEOM AS GEOMETRY, t.TIME AS TIME"
        
        base = select + """
FROM """ + self.spatialTable + """ t, """ + self.queriesTable + """ q
WHERE q.ID = """ + str(qid) + """ AND 
"""
        if qtype == "space - time":
            if timeType == 'continuous':
                base += "(t.TIME BETWEEN Q.START_DATE AND Q.END_DATE)"
            else:
                base += "(t.TIME IN (Q.START_DATE, Q.END_DATE))"
            base += """
AND """ + ora.spatialOperator(operator, 't.GEOM', 'q.GEOMETRY')
        elif qtype == 'space':
            base += ora.spatialOperator(operator, 't.GEOM', 'q.GEOMETRY')
        elif qtype == 'time':
            if timeType == 'continuous':
                base += "(t.TIME BETWEEN Q.START_DATE AND Q.END_DATE)"
            else:
                base += "(t.TIME IN (Q.START_DATE, Q.END_DATE))"
        return base
        
    def query(self, qid):
        connection = ora.getConnection(self.user, self.password, self.host, self.port, self.database)
        cursor = connection.cursor()

        cursor.execute("""SELECT t.TYPE, t.DATE_TYPE 
FROM """ + self.queriesTable + """ t 
WHERE id = """ + qid + """ AND dataset = '""" + self.dataset.lower() + "'")

        qtype, timeType = cursor.fetchall()[0]
        base = self.composeQuery(qid, 'SDO_ANYINTERACT', qtype, timeType)
        
        query = ora.getCTASStatement(self.queryResult + str(qid)) + """
(""" + base + ")"
        
        start = time.time()
        cursor.execute(query)
        end = time.time()
        
        points = ora.getNumPoints(connection, cursor, self.queryResult + str(qid))
        
        #========================================================================
        #         Find out how many points are returned by 
        #         the primary filter of oracle according to MBR
        #         using the SDO_FILTER Operator
        #========================================================================
        filtering = self.composeQuery(qid, 'SDO_FILTER', qtype, timeType, True)
        cursor.execute(filtering)
        filtering_pts = cursor.fetchall()[0][0]
        
        return round(end - start, 4), points, filtering_pts
        
    def dropQueryTable(self, cursor, qid):
        ora.dropTable(cursor, self.queryResult + str(qid))
        

if __name__ == "__main__":
    dataset = 'zandmotor'
    path = os.getcwd()
       
    for num in range(1,2):
        configuration = path + '/ini/' + dataset + '/validation_part{0}.ini'.format(num)
        
        querier = Querier(configuration)
        connection = querier.connect()
        cursor = connection.cursor()
        
        cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[querier.queriesTable.upper(),])
        length = len(cursor.fetchall())
        if not length:
            os.system('python -m pointcloud.queryTab {0}'.format(configuration))

        for query in querier.ids:
            t, points, filtering_pts = querier.query(query)
            print query, t, points, filtering_pts
        
        for num in querier.ids:
            ora.dropTable(cursor, querier.queryResult + str(num))
