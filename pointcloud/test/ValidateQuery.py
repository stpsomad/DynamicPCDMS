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
        
        self.queryTable = self.spatialTable + "_res"
        
    def query(self, qid):
        connection = self.getConnection()
        cursor = connection.cursor()
       
        cursor.execute("""SELECT t.TYPE, t.DATE_TYPE 
FROM """ + self.queriesTable + """ t 
WHERE id = """ + qid + """ AND dataset = '""" + self.dataset.lower() + "'")

        self.qtype, self.timeType = cursor.fetchall()[0]

        base = "SELECT " + ora.getHintStatement([ora.getParallelStringQuery(self.numProcesses)]) + """ t.GEOM AS GEOMETRY, t.TIME AS TIME
FROM """ + self.spatialTable + """ t, """ + self.queriesTable + """ q
WHERE q.ID = """ + str(qid) + """ AND 
"""
        
        if self.qtype == "space - time":
            if self.timeType == 'continuous':
                query = base + "(t.TIME BETWEEN Q.START_DATE AND Q.END_DATE)"
            else:
                query = base + "(t.TIME IN (Q.START_DATE, Q.END_DATE))"
            query += """
AND """ + ora.spatialOperator('SDO_ANYINTERACT', 't.GEOM', 'q.GEOMETRY')
        elif self.qtype == 'space':
            query =  base + ora.spatialOperator('SDO_ANYINTERACT', 't.GEOM', 'q.GEOMETRY')
        elif self.qtype == 'time':
            if self.timeType == 'continuous':
                query = base + "(t.TIME BETWEEN Q.START_DATE AND Q.END_DATE)"
            else:
                query = base + "(t.TIME IN (Q.START_DATE, Q.END_DATE))"
        
        start = time.time()
        cursor.execute(query)
        cursor.fetchall()
        end = time.time()        
        return end - start

if __name__ == "__main__":
    dataset = 'zandmotor'
    path = os.getcwd()
    for num in range(1,2):
        configuration = path + '/ini/' + dataset + '/validation_part{0}.ini'.format(num)
        querier = Querier(configuration)
        
        for query in querier.ids:
            t = querier.query(query)
            print query, t