# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 2015

@author: Stella Psomadaki

This module contains Oracle related functions. The functions mostly contain SQL 
commands to be posed to the database. 

Note: functions getConnectString, getNumPoints, mogrify, mogrifyExecute, 
    dropTable, createUser, createDirectory, getSizeTable, getSizeUserSDOIndexes
    have the following source:
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
 
def getConnectString(user, password, host, port, database):
    """
    Gets a connection string to establish a database connection.
    """
    return user + "/" + password + "@//" + host + ":" + port + "/" + database

def getConnection(user, password, host, port, database):
    """
    Establishes connection to an Oracle database.
    """
    try:
        return cx_Oracle.connect(getConnectString(user, password, host, port, database))
    except:
        print "Connection could not be established"

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
    
def getSizeTable(cursor, tableName):
    """ Get the size in MB of a table. (Includes the size of the table and the 
    large objects (LOBs) contained in table."""
    tableName = tableName.upper()
    try:
        if type(tableName) == str:
            tableName = [tableName, ]
        
        queryArgs = {}
        segs = []
        tabs = []
        for i in range(len(tableName)):
            name = 'name' + str(i)
            queryArgs[name] = tableName[i]
            segs.append('segment_name = :' + name)
            tabs.append('table_name = :' + name)
        
        cursor.execute("""
SELECT sum(bytes/1024/1024) size_in_MB FROM user_segments
WHERE (""" + ' OR '.join(segs) + """
OR segment_name in (
SELECT segment_name FROM user_lobs
WHERE """ + ' OR '.join(tabs) + """
UNION
SELECT index_name FROM user_lobs
WHERE """ + ' OR '.join(tabs) + """
)       
)""", queryArgs)
        
        size = cursor.fetchall()[0][0]
        if size == None:
            size = 0
    except:
        size = 0
    return size
 
def getSizeUserSDOIndexes(cursor, tableName):
    """ Get the size of the spatial indexes related to a table"""
    tableName = tableName.upper()    
    try:
        cursor.callproc("dbms_output.enable")
        q = """
DECLARE
size_in_mb  number;
idx_tabname varchar2(32);
BEGIN
dbms_output.enable;
SELECT sdo_index_table into idx_tabname FROM USER_SDO_INDEX_INFO
where table_name = :name and sdo_index_type = 'RTREE';
execute immediate 'analyze table '||idx_tabname||' compute system statistics for table';
select blocks * 0.0078125 into size_in_mb from USER_TABLES where table_name = idx_tabname;
dbms_output.put_line (to_char(size_in_mb));
END;
    """
        cursor.execute(q, [tableName,])
        statusVar = cursor.var(cx_Oracle.NUMBER)
        lineVar = cursor.var(cx_Oracle.STRING)
        size = float(cursor.callproc("dbms_output.get_line", (lineVar, statusVar))[0])
    except:
        size = 0
    return size

def computeStatistics(cursor, tableName, user):
    """
    Gather optimiser statistics.
    """
    mogrifyExecute(cursor, "ANALYZE TABLE " + tableName + \
    "  compute system statistics for table")
 
    mogrifyExecute(cursor,"""
BEGIN
dbms_stats.gather_table_stats('""" + user + """','""" + tableName + \
"""',NULL,NULL,FALSE,'FOR ALL COLUMNS SIZE AUTO',8,'ALL');
END;""")

def spatialOperator(operator, table_geometry, query_geometry, parameter_string = ''):
    params = ''    
    if parameter_string:
        params = ', ' + parameter_string  
    return operator + "(" + table_geometry + ", " + query_geometry + params + ") = 'TRUE'"
    
def getParallelStringQuery(numProcesses):
    """
    Generates the hint for parallel execution.
    """
    parallelString = ''
    if numProcesses > 1:
        parallelString = ' PARALLEL(' + str(numProcesses) + ') '
    return parallelString
        
def getHintStatement(hints):
    """
    Composes the SQL statement for using optimizer hints.
    """
    if hints == ['']:
        return ''
    return ' /*+ ' + ' '.join(hints) + ' */ '
    
def getTableSpaceString(tableSpace):
    """
    Generates the TABLESPACE predicate of the SQL query.
    """
    if tableSpace is not None and tableSpace != '':
        return " TABLESPACE " + tableSpace + " "
    else: 
        return ""

def getParallelString(numProcesses):
    if numProcesses > 1:
        return 'PARALLEL ' + str(numProcesses)
    return ''
    
def getCTASStatement(tableName, tableSpace = ''):
    """
    Generates a CREATE TABLE ... AS SELECT ... statement.
    """
    return "CREATE TABLE " + tableName + """
""" + tableSpace + """ 
AS """

def getSelectStatement(table, columns = '*', hints =''):
    """
    Generates a SELECT ... statement.
    """
    return "SELECT " + hints + ' ' + columns + " FROM " + table
    
def getAlias(column, alias = ''):
    """
    Composes an alias for the column specified. 
    """
    if alias:
        return column + ' AS ' + alias
    return column
    
def getSelectColumns(columns):
    """
    Prepare the columns to be selected by the SELECT statement.
    """
    if columns == '*':
        return columns
    else:
        return ', '.join(columns)

def composeDIM_ELEMENT(dimension_name, SDO_LB, SDO_UB, tolerance):
    """
    Composes the DIMINFO column of the USER_SDO_GEOM_METADATA table.
    
    Args:
        dimension_name (string) : the name of the dimension,
        SDO_LB (float) : the lower bound of the dimension specified,
        SDO_UB (float) : the upper bound of the dimension specified,
        tolerance (float) : the tolerance of the dimension
    """
    return """SDO_DIM_ELEMENT
(
'{0}',
{1},
{2},
{3}
)""".format(dimension_name.upper(), SDO_LB, SDO_UB, tolerance)

def updateSpatialMeta(connection, table_name, column_name, dimension_names, SDO_LBs, SDO_UBs, tolerances, srid):
    """
    Update the USER_SDO_GEOM_METADATA view. This is required before the Spatial index can be created.
    
    Args:
        connection : an Oracle connection,
        table_name (string) : the name of a feature table,
        column_name (string) : the name of the column of type SDO_GEOMETRY,
        dimension_names (list) : the list of names of the dimensions for which the metadata
                                    are updated. e.g. ['X', 'Y', 'Z'],
        SDO_LBs (list) : the lower bounds of the dimensions e.g. [-100, -1000, 10],
        SDO_UBs (list) : the upper bounds of the dimensions e.g. [100, 1000, 100],
        tolerances (list) : the tolerances of the dimensions e.g. [0.001, 0.001, 0.001]
        srid (int) : the Spatial Reference System Identifier used
    """
    cursor = connection.cursor()
    
    diminfo = """,
""".join([composeDIM_ELEMENT(dimension_names[i], SDO_LBs[i], SDO_UBs[i], tolerances[i]) for i in range(len(dimension_names))])
    
    mogrifyExecute(cursor, """
INSERT INTO user_sdo_geom_metadata
(table_name, column_name, srid, diminfo)
VALUES
(
'""" + table_name.upper() + """',
'""" + column_name.upper() + """',
""" + str(srid) + """,
SDO_DIM_ARRAY
(
""" + diminfo + """
)
)""")
    connection.commit()