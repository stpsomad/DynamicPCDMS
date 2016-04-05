# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 2015

@author: Stella Psomadaki
"""

try:
	import cx_Oracle
except ImportError:
	print "cx_Oracle module cannot be found"
	raise
 

""" This script contains Oracle related functions. The functions mostly contain
    SQL commands to be posed to the database. 
    
    Note: functions getConnectString, getNumPoints, mogrify, mogrifyExecute, 
    dropTable, createUser, createDirectory are taken from:
    https://github.com/NLeSC/pointcloud-benchmark/blob/master/python/pointcloud
    by author: Oscar Martinez Rubi
    Apache License
    Version 2.0, January 2004
"""

def connectSYSDBA():
    """ Connect to superuser for local database"""
    return cx_Oracle.connect(mode = cx_Oracle.SYSDBA)
    
def createUser(cursorSuper, userName, password, tableSpace, tempTableSpace):
    """ Creates a user in Oracle.
    If the user name already exists, the script DROPS it first"""
    
    # Check if the user already exists and drops it
    cursorSuper.execute("select * from all_users where username = '{0}'".format(userName.upper()))
    if len(cursorSuper.fetchall()) != 0:
        cursorSuper.execute('drop user ' + userName + ' CASCADE')
        cursorSuper.connection.commit()
    
    tsString = ''
    if tableSpace != None and tableSpace != '':
        tsString += ' default tablespace ' + tableSpace + ' '
    if tempTableSpace != None and tempTableSpace != '':
        tsString += ' temporary tablespace ' + tempTableSpace + ' '
    
    cursorSuper.execute('create user ' + userName + ' identified by ' + password + tsString)
    cursorSuper.execute('grant unlimited tablespace, connect, resource, create view to ' + userName)
    cursorSuper.connection.commit()

def createDirectory(cursorSuper, directoryVariableName, directoryAbsPath, userName):
    """ Creates a Oracle directory with read and write permission for the user"""
    cursorSuper.execute('select directory_name from all_directories where directory_name = :name', [directoryVariableName,])
    if len(cursorSuper.fetchall()):
        cursorSuper.execute("drop directory " + directoryVariableName)
        cursorSuper.connection.commit()
    cursorSuper.execute("create directory " + directoryVariableName + " as '" + directoryAbsPath + "'")
    cursorSuper.execute("grant read on directory " + directoryVariableName + " to " + userName)
    # without write permission not possible to create the external table in my machine
    cursorSuper.execute("grant write on directory " + directoryVariableName + " to " + userName)
    cursorSuper.connection.commit()

def createMetaTable(cursor, metaTable, check):
    """ Creates a metadata table that stores the metadata of the IOT that are used
    during the querying stage. This information is needed for the Quadtree-like 
    structure. If the table already exists it drops it.
    Stores: id, srid, minx, miny, minz, mint, maxx, maxy, maxz, maxt, scalex, scaley, scalez,
    offx, offy, offz"""
    
    dropTable(cursor, metaTable,check)
    
    cursor.execute("""CREATE TABLE {0} (id INTEGER, srid INTEGER, minx DOUBLE PRECISION, miny DOUBLE PRECISION, minz DOUBLE PRECISION, mint INTEGER,
    maxx DOUBLE PRECISION, maxy DOUBLE PRECISION, maxz DOUBLE PRECISION, maxt INTEGER, scalex DOUBLE PRECISION,
    scaley DOUBLE PRECISION, scalez DOUBLE PRECISION, offx DOUBLE PRECISION, offy DOUBLE PRECISION, offz DOUBLE PRECISION)""".format(metaTable))
    
def populateMetaTable(connection, cursor, metaTable, srid, minx, mixy, minz, mint, maxx, maxy, maxz, maxt, scalex, scaley, scalez, offx, offy, offz):
    """ Populate the metadata table with the right metadata. Used only for the first
    time data are inserted into the table."""    
    
    cursor.execute("""INSERT INTO {0} VALUES (1, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11},
    {12}, {13}, {14}, {15})""".format(metaTable, srid, minx, mixy, minz, mint, maxx, maxy, maxz, maxt, scalex, scaley, scalez, offx, offy, offz))
    connection.commit()

def updateMetaTableValues(connection, cursor, metaTable, minx, miny, minz, mint, maxx, maxy, maxz, maxt):
    """Update the metadata if the values have changed since the last import."""    
    
    cursor.execute("""UPDATE {0} SET maxx = {1}, maxy = {2}, minx = {3}, miny = {4}, mint = {5}, maxt = {6}, minz = {7}, maxz = {8}
WHERE id = 1""".format(metaTable, maxx, maxy, minx, miny, mint, maxt, minz, maxz))
    connection.commit()

def explainPlan(cursor, query, iid):
    cursor.execute("EXPLAIN PLAN SET STATEMENT_ID = '{0}' FOR ".format(iid) + query)
    
def displayPlan(cursor, iid):
    cursor.execute("SELECT PLAN_TABLE_OUTPUT FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', '{0}','ALL'))".format(iid))
    return cursor.fetchall()

def getNumPoints(connection, cursor, tableName):
    """ Returns the number of points """
    try:
        cursor.execute('select count(*) from ' + tableName)
        numpoints = int(cursor.fetchone()[0])
    except:
        numpoints = -1
        connection.rollback()
    return numpoints
  
def mogrifyMany(cursor, query, queryArgs = None):
    """ Executes the query statement"""
    query = query.upper()
    if queryArgs == None:
        cursor.execute(query)
    else:
        cursor.prepare(query) #Prepare the query so that it is not parsed all the time and can be reused.
        cursor.bindarraysize = 20000
        bindnames = cursor.bindnames() #the query must be prepared first
        if len(queryArgs[0]) != len(bindnames):
            raise Exception('Error: len(queryArgs) != len(bindnames) \n ' + str(queryArgs) + '\n' + str(bindnames))
        elif (type(queryArgs) == dict) or (type(queryArgs) == tuple):
            raise Exception('Error: queryArgs must be list')
        else:
            cursor.executemany(None, queryArgs)

def mogrify(cursor, query, queryArgs = None):
    """ Executes the query statement"""
    query = query.upper()
    if queryArgs == None:
        return query
    else:
        cursor.prepare(query)
        bindnames = cursor.bindnames() #the query must be prepared first
        if len(queryArgs) != len(bindnames):
            raise Exception('Error: len(queryArgs) != len(bindnames) \n ' + str(queryArgs) + '\n' + str(bindnames))
        if (type(queryArgs) == list) or (type(queryArgs) == tuple):
            for i in range(len(queryArgs)):
                query = query.replace(':'+bindnames[i],str(queryArgs[i]))
            return query
        elif type(queryArgs) == dict:
            upQA = {}
            for k in queryArgs:
                upQA[k.upper()] = queryArgs[k]
            for bindname in bindnames:
                query = query.replace(':'+bindname, str(upQA[bindname]))
            return query
        else:
            raise Exception('Error: queryArgs must be dict, list or tuple')
            
def mogrifyExecute(cursor, query, queryArgs = None):
    """ Execute a query"""
    mogrify(cursor, query, queryArgs)
    if queryArgs != None:
        cursor.execute(query, queryArgs)
    else:
        cursor.execute(query)
    cursor.connection.commit()

def dropTable(cursor, tableName, check = False):
    """ Drops a table"""
    if check == True or check == 'True':
        cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[tableName,])
        if len(cursor.fetchall()):
            mogrifyExecute(cursor, 'DROP TABLE ' + tableName)
    else:
        cursor.execute('DROP TABLE ' + tableName + ' PURGE')

def createIOT(cursor, iotTableName, columns, keyColumn, check = False):
    """ Creates an Index-Organized-Table
        iotTableName is the name of the table you need to add,
        columns is a list of strings with the column name and datatype
        keyColumn is the PRIMARY KEY of the iot

        returns none"""
    dropTable(cursor,iotTableName,check)

    mogrifyMany(cursor,"""CREATE TABLE """+ iotTableName +"""("""+','.join(columns)+""",
    CONSTRAINT """+iotTableName+"""_iot_idx PRIMARY KEY (""" + keyColumn + """))
    ORGANIZATION INDEX""")

def findOwner(cursor, tableName):
    """Finds the owner of a table"""
    cursor.execute("""SELECT OWNER FROM ALL_OBJECTS WHERE object_name = '"""+tableName.upper()+"'")
    return cursor.fetchone()[0]

def countColumns(cursor, tableName):
    """Counts the columns of a table"""
    cursor.execute("""SELECT COUNT(*) FROM all_tab_columns WHERE owner='"""+findOwner(cursor,tableName)+"' AND table_name ='"+tableName.upper()+"'")
    return cursor.fetchone()[0]

def insertInto(cursor, tableName, data, explain = False, iid = ''):
    string = []
    if data is not None:
        for i in range(1, countColumns(cursor, tableName)+1):
            string.append(":"+str(i))
    string = ",".join(string)
    query = "INSERT INTO "+tableName+" VALUES ("+string+")"
    if explain:
        explainPlan(cursor, query, iid)
    mogrifyMany(cursor, query, data)

def rebuildIOT(cursor, iotTableName):
    """Rebuild the index and reduce fragmentation because of incremental updates"""
    mogrifyExecute(cursor,"ALTER TABLE "+iotTableName+ " MOVE")
    #Note: you can also specify ONLINE which means people are modifying this data while we are rebuilding
    
def createTableQuery(cursor, name, columns, check = False):
    dropTable(cursor, name, check)
    cursor.execute("""CREATE TABLE {0} ({1})""".format(name, ','.join(columns)))
    
def appendData(connection, cursor, iotTableName, tableName):
    cursor.execute("""insert /*+ append */ into {0}
select *
from {1}""".format(iotTableName, tableName))
    connection.commit()
    
def getSizeTable(cursor, tableName, super = True):
    """ Get the size in MB of a table"""
    cursor.execute("""SELECT bytes/1024/1024 size_in_MB FROM user_segments WHERE segment_name = '{0}'""".format(tableName.upper() + '_PK'))
    size = cursor.fetchall()[0][0]
    if size == None:
        size = 0
    return size
