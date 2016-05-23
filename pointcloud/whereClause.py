# -*- coding: utf-8 -*-
"""
Created on Mon Mar 21 13:06:09 2016

@author: Stella Psomadaki

Structuring the WHERE statement
Code adapted from:
https://github.com/NLeSC/pointcloud-benchmark/blob/e0cb675c0dbb5376263d4b5b9ad0ccf938300f2f/python/pointcloud/dbops.py

Apache License
Version 2.0, January 2004
"""
import pointcloud.reader as reader


def getWhereStatement(conditions, operator = ' AND '):
    """
    Composes the WHERE clause according to the conditions specified and the
    operator given.    
    """
    
    if type(conditions) not in (list, tuple):
        conditions = [conditions,]
    cs = []
    for condition in conditions:
        if condition != '':
            cs.append(condition)
    if len(cs):
        return ' WHERE ' + operator.join(cs) + ' '
    return ''

def addZCondition(zRange, ZColumn):
    """
    Composes the predicate in the z dimension
    """    
    
    if zRange[0] == zRange[1]:
        return ''
    else:
        return "(" + ZColumn + ' BETWEEN ' + str(zRange[0]) + ' AND ' +  str(zRange[1]) + ')'

def addMortonCondition(mortonRanges, mortonColumnName):
    """
    Composes the predicate with the morton ranges.
    """
    elements = []
    for mortonRange in mortonRanges:
        elements.append('(' + mortonColumnName + ' between ' + str(mortonRange[0]) + ' and ' + str(mortonRange[1]) + ')')
    if len(elements) == 1:
        return elements[0]
    elif len(elements) > 1:
        return '(' + ' OR '.join(elements) + ')'
    return None
    
def getTime(granularity, start_date, end_date):
    """
    Formats time according to the granularity and the type of time query.
    """
    
    if start_date == None and end_date == None:
        return [[]]
    elif end_date == None and granularity == 'day':
        return [["TO_DATE('{0}', 'YYYY/MM/DD')".format(reader.formatTime(start_date)), None]] 
    elif granularity == 'year':
        return [[start_date, end_date]]
    elif granularity == 'day':
        return [["TO_DATE('{0}', 'YYYY/MM/DD')".format(reader.formatTime(start_date)), "TO_DATE('{0}', 'YYYY/MM/DD')".format(reader.formatTime(end_date))]]
        
def addTimeCondition(timeRanges, timeColumn, ttype = 'continuous'):
    """
    Composes the predicate of the time dimension    
    """
    if ttype == None:
        return ''
    if ttype.lower() == 'continuous':
        temp = []
        for timeRange in timeRanges:
            if timeRange == 1 or timeRange[1] == None:
                temp.append('(' + timeColumn + ' BETWEEN ' + str(timeRange[0]) + ' AND ' + str(timeRange[0]) + ')')
            else:
                temp.append('(' + timeColumn + ' BETWEEN ' + str(timeRange[0]) + ' AND ' + str(timeRange[1]) + ')')
        if len(temp) == 1:
            return temp[0]
        elif len(temp) > 1:
            return '(' + ' OR '.join(temp) + ')'
    else:
        if timeRanges[0][1] == None:
            return "({0} IN ({1}))".format(timeColumn, timeRanges[0][0])
        else:
            return "({0} IN ({1}))".format(timeColumn, ', '.join(map(str, timeRanges[0])))
    return None
    