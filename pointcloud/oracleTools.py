# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 2015

@author: Stella Psomadaki

This module contains Oracle related functions. The functions mostly contain SQL 
commands to be posed to the database. 

Note: functions getConnectString, getNumPoints, mogrify, mogrifyExecute, 
    dropTable, createUser, createDirectory have the following source:
    https://github.com/NLeSC/pointcloud-benchmark/blob/master/python/pointcloud
    by: Oscar Martinez Rubi
    Apache License
    Version 2.0, January 2004
"""

try:
	import cx_Oracle
except ImportError:
	print "cx_Oracle module cannot be found"
	raise

def connectSYSDBA():
    """
    Connect to superuser for local database
    """
    return cx_Oracle.connect(mode = cx_Oracle.SYSDBA)
    
def createUser(cursorSuper, userName, password, tableSpace, tempTableSpace):
    """
    Creates a user in Oracle.If the user name already exists, it is dropped.
    
    Needs administravive privileges to perform this action.
    """
    cursorSuper.execute("SELECT * FROM all_users WHERE username = '{0}'".format(userName.upper()))
    if len(cursorSuper.fetchall()) != 0:
        cursorSuper.execute('DROP USER ' + userName + ' CASCADE')
        cursorSuper.connection.commit()
    
    tsString = ''
    if tableSpace != None and tableSpace != '':
        tsString += ' DEFAULT TABLESPACE ' + tableSpace + ' '
    if tempTableSpace != None and tempTableSpace != '':
        tsString += ' TEMPORARY TABLESPACE ' + tempTableSpace + ' '
    
    cursorSuper.execute('CREATE USER ' + userName + ' IDENTIFIED BY ' + password + tsString)
    cursorSuper.execute('GRANT UNLIMITED TABLESPACE, CONNECT, RESOURCE, CREATE VIEW TO ' + userName)
    cursorSuper.connection.commit()

def createDirectory(cursorSuper, directoryVariableName, directoryAbsPath, userName):
    """
    Creates a Oracle directory with read and write permission for the user.
    
    Needs administravive privileges to perform this action.
    """
    
    cursorSuper.execute("""SELECT directory_name
FROM all_directories
WHERE directory_name = :name""", [directoryVariableName,])
    if len(cursorSuper.fetchall()):
        cursorSuper.execute("DROP DIRECTORY " + directoryVariableName)
        cursorSuper.connection.commit()
    cursorSuper.execute("CREATE DIRECTORY " + directoryVariableName + " AS '" + directoryAbsPath + "'")
    cursorSuper.execute("GRANT READ ON DIRECTORY " + directoryVariableName + " TO " + userName)
    cursorSuper.execute("GRANT WRITE ON DIRECTORY " + directoryVariableName + " TO " + userName)
    cursorSuper.connection.commit()

def createMetaTable(cursor, metaTable, check):
    """
    Creates a metadata table that stores the metadata of the IOT that are used
    during the querying stage. This information is needed for the Quadtree-like 
    structure. If the table already exists it drops it.
    Stores: id, srid, minx, miny, minz, mint, maxx, maxy, maxz, maxt, scalex, 
    scaley, scalez, offx, offy, offz
    """
    
    dropTable(cursor, metaTable,check)
    
    cursor.execute("""CREATE TABLE {0} (
id INTEGER,
srid INTEGER,
minx DOUBLE PRECISION,
miny DOUBLE PRECISION,
minz DOUBLE PRECISION,
mint INTEGER,
maxx DOUBLE PRECISION,
maxy DOUBLE PRECISION,
maxz DOUBLE PRECISION,
maxt INTEGER,
scalex DOUBLE PRECISION,
scaley DOUBLE PRECISION,
scalez DOUBLE PRECISION,
offx DOUBLE PRECISION,
offy DOUBLE PRECISION,
offz DOUBLE PRECISION)""".format(metaTable))
    
def populateMetaTable(connection, cursor, metaTable, srid, minx, mixy, minz, mint, maxx, maxy, maxz, maxt, scalex, scaley, scalez, offx, offy, offz):
    """
    Populate the metadata table with the right metadata. Used only for the first
    time data are inserted into the table.
    """    
    
    cursor.execute("""INSERT INTO {0} VALUES (1, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11},
    {12}, {13}, {14}, {15})""".format(metaTable, srid, minx, mixy, minz, mint, maxx, maxy, maxz, maxt, scalex, scaley, scalez, offx, offy, offz))
    connection.commit()

def updateMetaTableValues(connection, cursor, metaTable, minx, miny, minz, mint, maxx, maxy, maxz, maxt):
    """
    Update the metadata if the values have changed since the last import.
    """    
    
    cursor.execute("""UPDATE {0} 
SET maxx = {1}, maxy = {2}, minx = {3}, miny = {4}, mint = {5}, maxt = {6}, minz = {7}, maxz = {8}
WHERE id = 1""".format(metaTable, maxx, maxy, minx, miny, mint, maxt, minz, maxz))
    connection.commit()

def explainPlan(cursor, query, iid):
    """
    Determine the execution plan Oracle follows to execute a specified SQL statement.
    """
    cursor.execute("EXPLAIN PLAN SET STATEMENT_ID = '{0}' FOR ".format(iid) + query)
    
def displayPlan(cursor, iid):
    """
    Query the plan_table table and return the execution plan needed.
    """    
    
    cursor.execute("""SELECT PLAN_TABLE_OUTPUT 
FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', '{0}','ALL'))""".format(iid))
    return cursor.fetchall()

def getNumPoints(connection, cursor, tableName):
    """
    Returns the number of points stored in the database the moment the SQL query
    is executed.
    """
    try:
        cursor.execute('select count(*) from ' + tableName)
        numpoints = int(cursor.fetchone()[0])
    except:
        numpoints = -1
        connection.rollback()
    return numpoints
  
def mogrifyMany(cursor, query, queryArgs = None):
    """
    Executes the query statement or prepares it for execution.
    """
    query = query.upper()
    if queryArgs == None:
        cursor.execute(query)
    else:
        cursor.prepare(query) 
        cursor.bindarraysize = 20000
        bindnames = cursor.bindnames() 
        if len(queryArgs[0]) != len(bindnames):
            raise Exception('Error: len(queryArgs) != len(bindnames) \n ' + str(queryArgs) + '\n' + str(bindnames))
        elif (type(queryArgs) == dict) or (type(queryArgs) == tuple):
            raise Exception('Error: queryArgs must be list')
        else:
            cursor.executemany(None, queryArgs)

def mogrify(cursor, query, queryArgs = None):
    """
    Executes the query statement  or prepares it for execution.
    """
    query = query.upper()
    if queryArgs == None:
        return query
    else:
        cursor.prepare(query)
        bindnames = cursor.bindnames()
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
    """
    Execute a query statement
    """
    mogrify(cursor, query, queryArgs)
    if queryArgs != None:
        cursor.execute(query, queryArgs)
    else:
        cursor.execute(query)
    cursor.connection.commit()

def dropTable(cursor, tableName, check = False):
    """
    Drops the specified table
    """
    if check == True or check == 'True':
        cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[tableName,])
        if len(cursor.fetchall()):
            mogrifyExecute(cursor, 'DROP TABLE ' + tableName)
    else:
        cursor.execute('DROP TABLE ' + tableName + ' PURGE')

def createIOT(cursor, iotTableName, columns, keyColumn, check = False):
    """
    Creates an Index-Organized-Table.
    
    Args:
        iotTableName is the name of the table you need to add,
        columns is a list of strings with the column name and datatype
        keyColumn is the PRIMARY KEY of the iot
    """
    dropTable(cursor,iotTableName,check)

    mogrifyMany(cursor,"""CREATE TABLE """+ iotTableName +"""("""+','.join(columns)+""",
    CONSTRAINT """+iotTableName+"""_iot_idx PRIMARY KEY (""" + keyColumn + """))
    ORGANIZATION INDEX""")

def findOwner(cursor, tableName):
    """
    Finds the owner of a table.
    """
    cursor.execute("""SELECT OWNER
FROM ALL_OBJECTS
WHERE object_name = '"""+tableName.upper()+"'")
    return cursor.fetchone()[0]

def countColumns(cursor, tableName):
    """
    Counts the columns of a table and returns the number.
    """
    cursor.execute("""SELECT COUNT(*) 
FROM all_tab_columns
WHERE owner='"""+findOwner(cursor,tableName)+"' AND table_name ='"+tableName.upper()+"'")
    return cursor.fetchone()[0]

def insertInto(cursor, tableName, data, explain = False, iid = ''):
    """
    Generates the INSERT INTO statement using batches.
    """
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
    """
    Rebuild the index and reduce fragmentation because of incremental updates.
    """
    mogrifyExecute(cursor,"ALTER TABLE "+iotTableName+ " MOVE")
    
def createTable(cursor, name, columns, check = False):
    """
    Creates a table.
    """
    dropTable(cursor, name, check)
    cursor.execute("""CREATE TABLE {0} ({1})""".format(name, ','.join(columns)))
    
def appendData(connection, cursor, iotTableName, tableName):
    """
    Enforces direct path insert.
    """
    cursor.execute("""INSERT /*+ APPEND */ INTO {0}
SELECT *
FROM {1}""".format(iotTableName, tableName))
    connection.commit()
    
def getSizeTable(cursor, tableName, super = True):
    """
    Get the size of a table in MB.
    """
    cursor.execute("""SELECT bytes/1024/1024 size_in_MB FROM user_segments WHERE segment_name = '{0}'""".format(tableName.upper() + '_PK'))
    size = cursor.fetchall()[0][0]
    if size == None:
        size = 0
    return size
    
def renameTable(cursor, oldName, newName):
    """
    Rename the specified table.
    """
    cursor.execute("""RENAME {0} to {1}""".format(oldName, newName))
    
def renameConstraint(cursor, table, oldConstraintName, newConstraintName):
    """
    Rename a constaint of the specified table.
    """
    cursor.execute("""ALTER TABLE {0}
RENAME CONSTRAINT {1} TO {2}""".format(table, oldConstraintName, newConstraintName))

def renameIndex(cursor, oldIndexName, newIndexName):
    """
    Rename the specified index.
    """
    cursor.execute("""ALTER INDEX {0} RENAME TO {1}""".format(oldIndexName, newIndexName))
    
