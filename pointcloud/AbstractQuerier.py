# -*- coding: utf-8 -*-
"""
Created on Sat Feb 13 19:19:06 2016

@author: Stella Psomadaki
"""
from ConfigParser import ConfigParser
from shapely.wkt import loads
from shapely.geometry import Polygon, box
import time
import pointcloud.general as general
import pointcloud.structures.QuadTree as QuadTree
import pointcloud.structures.Octree as Octree
import pointcloud.structures.HexadecTree as HexadecTree
import pointcloud.structures.dynamicOctree as dynamicOctree
import pointcloud.oracleTools as ora
from pointcloud.CommonOracle import Oracle
import pointcloud.reader as reader
import pointcloud.whereClause as whereClause
from pointcloud.structures.geometry import Polygon3D, dynamicPolygon, Polygon4D
import os

#TODO: PUT ITERATOR WHEN FETCHING THE DATA

class Querier(Oracle):
    def __init__(self, configuration):
        Oracle.__init__(self, configuration)
        config = ConfigParser()
        config.read(configuration)
        
        self.queriesTable = config.get('Querier', 'table')
        self.tolerance = config.get('Querier', 'tolerance')
        self.method = config.get('Querier', 'method')
        self.numProcesses = config.getint('Querier', 'numProcesses')
        self.tableSpace = config.get('Querier', 'tableSpace')

        self.maxRanges = config.getint('Querier', 'maxRanges')
        
        self.ids = config.get('Querier', 'id').replace(' ', '').split(',')
        self.numBits = config.getint('Querier', 'numBits')       
        
        connection = self.getConnection(False)
        cursor = connection.cursor()
        ora.mogrifyExecute(cursor, """SELECT srid, minx, miny, minz, mint, maxx,
maxy, maxz, maxt, scalex, scaley, scalez, offx, offy, offz FROM {0}""".format(self.metaTable))
        (self.srid, self.minx, self.miny, self.minz, self.mint, self.maxx,
         self.maxy, self.maxz, self.maxt, self.scalex, self.scaley, self.scalez,
         self.offx, self.offy, self.offz) = cursor.fetchone()
        connection.close()
        self.roundNum = len(str(self.scalex)) - 2
        
        self.numLevels = 'auto'
        if self.integration.lower() == 'loose':
            if self.parse.lower() == 'xyt':
                self.domain = (0, 0, int((self.maxx - self.offx)/self.scalex),
                               int((self.maxy - self.offy)/self.scaley))
                               
                self.structure = QuadTree.QuadTree(self.domain, self.numLevels, self.numBits)
                self.case = 1
            elif self.parse.lower() == 'xyzt':
                self.domain = (0, 0, 0, int((self.maxx - self.offx)/self.scalex),
                               int((self.maxy - self.offy)/self.scaley), 
                                int((self.maxz - self.offz)/self.scalez))
                                
                self.structure = Octree.Octree(self.domain, self.numLevels, self.numBits)
                self.case = 2
        elif self.integration.lower() == 'deep':
            if self.parse.lower() == 'xyt':
                self.domain = (0, 0, 0, int(self.maxt * self.scale), 
                               int((self.maxx - self.offx)/self.scalex), 
                                int((self.maxy - self.offy)/self.scaley))
                                
                self.structure = dynamicOctree.dynamicOctree(self.domain, self.numLevels, self.numBits)
                self.case = 3
            elif self.parse.lower() == 'xyzt':
                self.domain = (0, 0, 0, 0, int(self.maxt * self.scale), 
                               int((self.maxx - self.offx)/self.scalex), 
                                int((self.maxy - self.offy)/self.scaley), 
                                int((self.maxz - self.offz)/self.scalez))
                                
                self.structure = HexadecTree.HexadecTree(self.domain, self.numLevels, self.numBits)
                self.case = 4
                
        self.queryTable = self.iotTableName + "_res"
        self.rangeTable = 'ranges_{0}{1}{2}{3}_'.format(self.dataset[0], self.integration[0], self.parse, self.scale).upper()
        
        self.queryTableColumns = ['id', 'dataset', 'type', 'geometry', 'time', 'time_type', 'z']
        self.joinColumns = ['low NUMBER', 'upper NUMBER']
        self.mortonJoinWhere = '(t.morton BETWEEN r.low AND r.upper)'
        
        if self.granularity == 'day':
            self.queryColumns = ['time VARCHAR2(20)', 'X NUMBER', 'Y NUMBER', 'Z FLOAT']
            self.params = [1, 4, 4, 3, 1, -2, 0, 3, 4, 0, 0, 4, 3, 0, 3, 4, 1, 4]
        else:
            self.queryColumns = ['time INTEGER', 'X NUMBER','Y NUMBER', 'Z FLOAT']
            self.params = [0, 4, 4, 3, 8, 1, 2, 8, 9, 2, 2, 9, 7, 3, 8, 12, 3, 11]
            
        self.ozmin, self.ozmax  = 0, 0
        self.wkt = None
        self.start_date, self.end_date = None, None
        self.qtype, self.timeType = None, None

    
    def prepareQuery(self, qid):
        """The preparation of the query.
        
        This function reads a query stored in the query table, prepares the
        parameters and creates the appropriate SQL statement for the first 
        fetching of the data."""
        
        connection = self.getConnection()
        cursor = connection.cursor()

        if self.granularity == 'day':
            extractTime = "TO_CHAR(t.START_DATE, 'yyyy,mm,dd'), TO_CHAR(t.END_DATE, 'yyyy,mm,dd')"
        elif self.granularity == 'year':
            extractTime = "EXTRACT(YEAR FROM t.START_DATE), EXTRACT(YEAR FROM t.END_DATE)"
        
        cursor.execute("SELECT t.TYPE, t.GEOMETRY.Get_WKT(), " + extractTime + "," + \
"t.DATE_TYPE, t.Z_MIN, t.Z_MAX FROM " + self.queriesTable + """ t 
WHERE id = """ + qid + """ AND dataset = '""" + self.dataset.lower() + "'")

        self.qtype, self.wkt, self.start_date, self.end_date, self.timeType, self.ozmin, self.ozmax  = cursor.fetchall()[0]

        if self.wkt is not None:
            self.wkt = str(self.wkt)
        connection.close()
        
        # Setting up the missing variables along with transformations to the time encoding. 
        if self.granularity == 'day':
            if self.start_date is None and self.end_date is None:
                times = [[self.mint * self.scale, self.maxt * self.scale]]
            elif self.start_date is not None and self.end_date is not None:
                self.start_date = map(int, self.start_date.split(','))
                self.end_date = map(int, self.end_date.split(','))
                times = [[reader.daySinceEpoch(self.start_date[0], 
                        self.start_date[1], self.start_date[2]) * self.scale, 
                        reader.daySinceEpoch(self.end_date[0], 
                        self.end_date[1], self.end_date[2]) * self.scale]]
            elif self.end_date is None:
                self.start_date = map(int, self.start_date.split(','))
                times = [[reader.daySinceEpoch(self.start_date[0], self.start_date[1], self.start_date[2]) * self.scale, None]]
        else:
            if self.start_date is None and self.end_date is None:
                times = [[self.mint * self.scale, self.maxt * self.scale]]
            elif self.start_date is not None and self.end_date is not None:
                times = [[self.start_date * self.scale, self.end_date * self.scale]]
            elif self.end_date is None:
                times = [[self.start_date * self.scale, None]]

        if self.ozmin is None or self.ozmax is None: #no selectivity on z
            zmin = int(round((self.minz - self.offz)/self.scalez, 0))
            zmax = int(round((self.maxz - self.offz)/self.scalez, 0))
        else:
            zmin = int(round((self.ozmin - self.offz)/self.scalez, 0))
            zmax = int(round((self.ozmax - self.offz)/self.scalez, 0))

        # Preparing the different types of queries: Space and space - time
        continuous = True
        if self.wkt:
            if self.qtype.replace(' ', '').lower() != 'nn-search':
                ordinates = list(loads(self.wkt).exterior.coords)
            else:
                ordinates = list(loads(self.wkt).coords)
            
            if self.case == 1: #lxyt
                geometry = Polygon(self.list2ScaleOffset(ordinates)).wkt
                if self.qtype.lower() == 'space':
                    coarser = self.params[0] #0, 0
                else:
                    coarser = self.params[1] #4, 4
            
            elif self.case == 2: #lxyzt
                geometry = Polygon3D(Polygon(self.list2ScaleOffset(ordinates)), zmin, zmax)

                if self.qtype.lower() == 'space':
                    coarser = self.params[2] #4, 4
                else:
                    coarser = self.params[3] #3, 3

            elif self.case == 3: #dxyt
                geom = Polygon(self.list2ScaleOffset(ordinates))                
                if times[0][1] is None:
                    continuous = False
                    times[0][1] = times[0][0]
                    coarser = self.params[4] #1, 8
                elif self.qtype.lower() == 'space':
                    coarser = self.params[5] #-2, 1
                elif self.timeType == 'continuous':
                    coarser = self.params[6] #0, 2
                elif self.timeType == 'discrete':
                    coarser = self.params[7] #3, 8
                    
                if self.timeType == 'discrete' and (self.start_date is not None) and (self.end_date is not None):
                    geometry = [dynamicPolygon(geom, times[0][0], times[0][0]),
                                dynamicPolygon(geom, times[0][1], times[0][1])]
                else:
                    geometry = dynamicPolygon(geom, times[0][0], times[0][1])                    
                
            elif self.case == 4: #dxyzt
                geom = Polygon(self.list2ScaleOffset(ordinates))
                if times[0][1] == None:
                    continuous = False
                    coarser = self.params[8] #4, 9
                    times[0][1] = times[0][0]
                elif self.qtype.lower() == 'space':
                    coarser = self.params[9] #0, 2
                elif self.timeType == 'continuous':
                    coarser = self.params[10] #0, 2
                elif self.timeType == 'discrete':
                    coarser = self.params[11] #4, 9
                
                if self.timeType == 'discrete' and self.start_date is not None and self.end_date is not None:
                    geometry = [Polygon4D(geom, zmin, zmax, times[0][0], times[0][0]),
                                Polygon4D(geom, zmin, zmax, times[0][1], times[0][1])]
                else:
                    geometry = Polygon4D(geom, zmin, zmax, times[0][0], times[0][1])
                
        else: #time queries
            if self.case == 1:
                geometry = []
                
            elif self.case == 2:
                geometry = []
                
            elif self.case == 3:
                temp_geom = self.list2ScaleOffset([(self.minx, self.miny), (self.maxx, self.maxy)])
                geom = box(temp_geom[0][0], temp_geom[0][1], temp_geom[1][0], temp_geom[1][1])
                
                if times[0][1] is None:
                    times[0][1] = times[0][0]
                    coarser = self.params[12] #3, 7
                    continuous = False
                elif self.timeType == 'continuous':
                    coarser = self.params[13] #0, 3
                else:
                    coarser = self.params[14] #3, 8
                
                if self.timeType == 'discrete' and self.start_date is not None and self.end_date is not None:
                    geometry = [dynamicPolygon(geom, times[0][0], times[0][0]),
                                dynamicPolygon(geom, times[0][1], times[0][1])]
                else:
                    geometry = dynamicPolygon(geom, times[0][0], times[0][1])

            elif self.case == 4:
                temp_geom = self.list2ScaleOffset([(self.minx, self.miny),(self.maxx, self.maxy)])
                geom = box(temp_geom[0][0], temp_geom[0][1], temp_geom[1][0], temp_geom[1][1])
                if times[0][1] is None:
                    times[0][1] = times[0][0]
                    coarser = self.params[15] #4, 12
                    continuous = False
                elif self.timeType == 'continuous':
                    coarser = self.params[16] #1, 3
                elif self.timeType == 'discrete':
                    coarser = self.params[17] #4, 11
                
                if self.timeType == 'discrete' and self.start_date is not None and self.end_date is not None:
                    geometry = [Polygon4D(geom, zmin, zmax, times[0][0], times[0][0]),
                                Polygon4D(geom, zmin, zmax, times[0][1], times[0][1])]
                else:           
                    geometry = Polygon4D(geom, zmin, zmax, times[0][0], times[0][1])
                    

        """The final lines have to do with the way of posing the query to the 
        database. Two options are possible:
        (a) sql: A SQL query is posed to the database. The number of ranges is
        limited by a maximum number.
        (b) join: The table is joined explicitly with a table containing the 
        ranges."""
        if geometry == []:
            mortonWhere, self.mortonJoinWhere, ranges, rangeTab, morPrep, insert = ('', '', 0, None, 0, 0)
        else:
            if self.method == 'join':
                rangeTab = (self.rangeTable + qid).upper()
                ranges, morPrep, insert = self.join(geometry, coarser, rangeTab, continuous)
                mortonWhere = self.mortonJoinWhere
            elif self.method == 'sql':
                rangeTab, insert = None, 0
                mortonWhere, ranges, morPrep = self.sql(geometry, coarser, continuous)
        
        # if deep the time is in the morton code
        if self.integration == 'deep' or (self.start_date is None and self.end_date is None and self.integration == 'loose'): 
            timeWhere = ''
        elif self.integration == 'loose': 
            timeWhere = whereClause.addTimeCondition(times, 'time', self.timeType)
        
        return whereClause.getWhereStatement([timeWhere, mortonWhere]), ranges, morPrep, insert, rangeTab

        
    def join(self, geometry, coarser, rangeTable, continuous = True):
        """ This function creates and populates the table with the morton ranges
        in order to later perform a join with the data table."""
        
        connection = self.getConnection()
        cursor = connection.cursor()
        cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[rangeTable.upper(),])
        length = len(cursor.fetchall())
        connection.close()
        if length:
            return 0, 0, 0
        else:
            start1 = time.time()
            if isinstance(geometry, list):
                data1 = self.structure.getMortonRanges(geometry[0], coarser, 
                                                       continuous = False,
                                                       maxRanges = self.maxRanges,
                                                       numLevels = self.numLevels)[1]
                data2 = self.structure.getMortonRanges(geometry[1], coarser, 
                                                       continuous = False,
                                                       maxRanges = self.maxRanges,
                                                       numLevels = self.numLevels)[1]
                morPrep = time.time() - start1
                ranges = len(data1) + len(data2)
                start2 = time.time()
                if len(data1) == 0 and len(data2) == 0:
                    print 'None morton range in specified extent!'
                    self.mortonJoinWhere = ''
                elif len(data1) or len(data2):
                    connection = self.getConnection()
                    cursor = connection.cursor()
                    ora.createIOT(cursor, rangeTable, self.joinColumns, 'low', True)
                    if len(data1):
                        sqlldrCommand = self.sqlldr(rangeTable, ['LOW', 'UPPER'], format_lst(data1)) #TODO: separate time between formatting and performing the loading
                        os.system(sqlldrCommand) 
                    if len(data2):
                        sqlldrCommand = self.sqlldr(rangeTable, ['LOW', 'UPPER'], format_lst(data2))
                        os.system(sqlldrCommand)
            else:
                data = self.structure.getMortonRanges(geometry, coarser, 
                                                      continuous, 
                                                      maxRanges = self.maxRanges,
                                                       numLevels = self.numLevels)[1]
                morPrep = time.time() - start1
                start2 = time.time()
                if len(data) == 0:
                    print 'None morton range in specified extent!'
                    ranges = 0
                else:
                    connection = self.getConnection()
                    cursor = connection.cursor()
                    ora.createIOT(cursor, rangeTable, self.joinColumns, 'low', True)
                    sqlldrCommand = self.sqlldr(rangeTable, ['LOW', 'UPPER'], format_lst(data))
                    os.system(sqlldrCommand)
                    ranges = len(data)
            insert = time.time() - start2
            return ranges, morPrep, insert
        
    
    def sql(self, geometry, coarser, continuous = True):
        """ This function generates the morton codes for the normal case of posing
        a SQL command to the database.
        
        Returns the where clause with the morton ranges."""
        
        start1 = time.time()
        if isinstance(geometry, list):
            (mimranges, mxmranges1, range1) = self.structure.getMortonRanges(geometry[0], 
                                                coarser, continuous = False, 
                                                maxRanges = int(self.maxRanges / 2),
                                                numLevels = self.numLevels)
            (mimranges, mxmranges2, range2) = self.structure.getMortonRanges(geometry[1], 
                                                coarser, continuous = False, 
                                                maxRanges = int(self.maxRanges / 2),
                                                numLevels = self.numLevels)
            morPrep = time.time() - start1            
            ranges = range1 + range2

            if len(mimranges) == 0 and len(mxmranges1) == 0 and len(mxmranges2) == 0:
                print 'None morton range in specified extent!'
                mortonWhere = ''
            if len(mxmranges1):
                mortonWhere1 = whereClause.addMortonCondition(mxmranges1, 'morton')
                
            if len(mxmranges2):
                mortonWhere2 = whereClause.addMortonCondition(mxmranges2, 'morton')         
            mortonWhere = ' OR '.join(['(' + mortonWhere1 + ')', '(' + mortonWhere2 +')'])
        else:
            (mimranges, mxmranges, ranges) = self.structure.getMortonRanges(geometry,
                                                 coarser, continuous, 
                                                 maxRanges = self.maxRanges,
                                                 numLevels = self.numLevels)
            morPrep = time.time() - start1            
            if len(mimranges) == 0 and len(mxmranges) == 0:
                print 'None morton range in specified extent!'
                mortonWhere = ''
            if len(mxmranges):
                mortonWhere = whereClause.addMortonCondition(mxmranges, 'morton')
        return mortonWhere, ranges, morPrep


    def pointInPolygon(self, tempName, qid, check = False):
        """
        The point in polygon function. This function is the validation step 
        of the querying process. It checks whether the points actually overlap 
        with the specific area. 
        
        Uses the PointInPolygon operator of Oracle.
        """
        
        if self.case == 1 or self.case == 2:
            zWhere = whereClause.getWhereStatement([whereClause.addZCondition([self.ozmin, self.ozmax], 'Z')])
        elif self.case == 3 or self.case == 4:
            timeWhere = whereClause.addTimeCondition(getTime(self.granularity, self.start_date, self.end_date), 'TIME', self.timeType)
            zWhere = whereClause.getWhereStatement([timeWhere, whereClause.addZCondition([self.ozmin, self.ozmax], 'Z')])
        else:
            zWhere = ''
        
        queryTab = self.iotTableName + '_res_' + str(qid)
        
        if self.qtype.replace(' ', '').lower() != 'nn-search':
            if self.granularity == 'day':
                query =  ora.getCTASStatement(queryTab, ora.getTableSpaceString(self.tableSpace)) + \
'(' + self.getPointInPolygonStatement(tempName, '*', ['X', 'Y', 'Z',
ora.getAlias("""TO_DATE(TIME, 'yyyy/mm/dd')""", 'TIME')], zWhere) + ')'
            else:
                query = ora.getCTASStatement(queryTab, ora.getTableSpaceString(self.tableSpace)) + \
'(' + self.getPointInPolygonStatement(tempName, '*', ['X', 'Y', 'Z', 'TIME'], zWhere) + ')'
        

        connection = self.getConnection()
        cursor = connection.cursor()
        ora.dropTable(cursor, queryTab, check)
        start = time.time()
        cursor.execute(query)
        connection.commit()

        end = round(time.time() - start, 6) #Point in Polygon time
        final = ora.getNumPoints(connection, cursor, queryTab)
        ora.dropTable(cursor, tempName)
        return final, end
        
        
    def query(self, qid):
        """
        This function is responsible for glueing together all the steps needed to
        pose a query to the spatio-temporal point cloud database.
        """
        connection = self.getConnection()
        cursor = connection.cursor()
        lst = []

        #========================================================================
        #       Preparation
        #========================================================================
        whereStatement, ranges, morPrep, insert, rangeTab  = self.prepareQuery(qid)
        lst.append(round(morPrep, 6)) # preparation
        lst.append(round(insert, 6)) # insert ranges into table
        lst.append(ranges) #number of ranges

        #========================================================================
        #       First approximation of query region
        #========================================================================

        if whereStatement is not '':
            if rangeTab is not None:
                query = "SELECT " + ora.getHintStatement(['USE_NL (t r)', ora.getParallelStringQuery(self.numProcesses)]) + \
" " + ', '.join(['t.'+ i for i in self.columnNames]) + """
FROM """ + self.iotTableName + " t, " + rangeTab + """ r 
""" + whereStatement

            else:
                query = "SELECT "+ ora.getHintStatement([ora.getParallelStringQuery(self.numProcesses)]) + ', '.join(self.columnNames) + """ 
FROM """ + self.iotTableName + """ 
""" + whereStatement

            start1 = time.time()
            ora.mogrifyExecute(cursor, query)
            result = cursor.fetchall()

            lst.append(round(time.time() - start1, 10)) # fetching
            
            if (self.integration == 'loose' and self.qtype.lower() != 'time') or self.integration == 'deep':
                qTable = self.queryTable + '_temp_' + qid
            else:      
                qTable = self.queryTable + '_' + qid
            
            start1 = time.time()
            decoded = self.decodeSpaceTime(result)
            lst.append(round(time.time() - start1, 6)) #decoding

            start1 = time.time()
            res = self.storeQuery(qTable, self.queryColumns, decoded, True)
            lst.append(round(time.time() - start1, 6)) #storing
            if res != []:
                ptsInTemp = res
                lst.append(res) #approximate points
            else:
                ptsInTemp = 0
                
            #==================================================================
            #     Secondary filtering of query region
            #==================================================================

            if (self.qtype.lower() == 'time' and self.integration == 'loose') or res == []:
                # no data returned or it is a time query in the loose integration
                lst.append(ptsInTemp) #approximate points
                lst.append(0) # point in polygon time
                return lst
            else:
                
                if self.integration.lower() == 'deep' and self.qtype.lower() == 'time':
                    queryTab = self.iotTableName + '_res_' + str(qid)
                    timeWhere = whereClause.addTimeCondition(getTime(self.granularity, self.start_date, self.end_date), 'TIME', self.timeType)
                    zWhere = whereClause.addZCondition([self.ozmin, self.ozmax], 'Z')
                    whereValue = whereClause.getWhereStatement([timeWhere, zWhere])
                    
                    
                    if self.granularity == 'day':
                        query = "CREATE TABLE " + queryTab + """
""" + ora.getTableSpaceString(self.tableSpace) + """
AS SELECT * 
FROM (
    SELECT """ + ora.getHintStatement([ora.getParallelStringQuery(self.numProcesses)]) + \
    """ X, Y, Z, TO_DATE(TIME, 'yyyy/mm/dd') as TIME 
    FROM """ + qTable +"""
    ) 
""" + whereValue
                    else:
                        query = "CREATE TABLE " + queryTab + """
""" + ora.getTableSpaceString(self.tableSpace) + """ 
    AS SELECT """ + ora.getHintStatement([ora.getParallelStringQuery(self.numProcesses)]) + \
    """ X, Y, Z, TIME 
    FROM """+ qTable + """" 
    """ + whereValue
                    
                    start1 = time.time()
                    cursor.execute(query)
                    end = round(time.time() - start1, 2)

                    ora.dropTable(cursor, qTable, False)
                    final = ora.getNumPoints(connection, cursor, queryTab)
                    lst.append(final) #final points
                    lst.append(end) #point in polygon time
                    return lst
                
                else:
                    final, end = self.pointInPolygon(qTable, qid, True)
                    
                    lst.append(final) #final points
                    lst.append(end) #point in polygon time
                    return lst
        else:
            print 'No data returned'
            return [lst[0], lst[1], '-', '-','-','-','-']
    
            
    def decodeSpaceTime(self, result):
        """
        Decode the morton codes according to the specified integration of space 
        and time and the granularity of time.
        """
        if self.case ==  1:
            if self.granularity == 'day':
                return map(lambda x: [reader.formatTime(reader.inverseDaySinceEpoch(int(x[0]/self.scale))),
                                      reader.morton2coordsX2D(x[1], self.offx, self.scalex, self.roundNum), 
                                      reader.morton2coordsY2D(x[1], self.offy, self.scaley, self.roundNum), x[2]], result)
            else:
                return map(lambda x: [int(x[0]/self.scale), reader.morton2coordsX2D(x[1], self.offx, self.scalex, self.roundNum), 
                                      reader.morton2coordsY2D(x[1], self.offy, self.scaley, self.roundNum), x[2]], result)
        elif self.case == 2:
            if self.granularity == 'day':
                return map(lambda x: [reader.formatTime(reader.inverseDaySinceEpoch(int(x[0]/self.scale))), 
                                      reader.morton2coordsX3D(x[1], self.offx, self.scalex, self.roundNum), 
                                        reader.morton2coordsY3D(x[1], self.offy, self.scaley, self.roundNum), 
                                        reader.morton2coordsZ3D(x[1], self.offz, self.scalez, self.roundNum)], result)
            else:
                return map(lambda x: [int(x[0]/self.scale), reader.morton2coordsX3D(x[1], self.offx, self.scalex, self.roundNum), 
                                      reader.morton2coordsY3D(x[1], self.offy, self.scaley, self.roundNum), 
                                        reader.morton2coordsZ3D(x[1], self.offz, self.scalez, self.roundNum)], result)
        elif self.case == 3:
            if self.granularity == 'day':
                return map(lambda x: [reader.formatTime(reader.inverseDaySinceEpoch(int(reader.morton2coordst3D(x[0])/self.scale))), 
                                      reader.morton2coordsY3D(x[0], self.offx, self.scalex, self.roundNum),
                                    reader.morton2coordsZ3D(x[0], self.offy, self.scaley, self.roundNum), x[1]], result)
            else:
                return map(lambda x: [int(reader.morton2coordst3D(x[0])/self.scale), 
                                      reader.morton2coordsY3D(x[0], self.offx, self.scalex, self.roundNum), 
                                    reader.morton2coordsZ3D(x[0], self.offy, self.scaley, self.roundNum), x[1]], result)
        elif self.case == 4:
            if self.granularity == 'day':
                return map(lambda x: [reader.formatTime(reader.inverseDaySinceEpoch(int(reader.morton2coordst4D(x[0])/self.scale))), 
                                      reader.morton2coordsX4D(x[0], self.offx, self.scalex, self.roundNum), 
                                    reader.morton2coordsY4D(x[0], self.offy, self.scaley, self.roundNum), 
                                    reader.morton2coordsZ4D(x[0], self.offz, self.scalez, self.roundNum)], result)
            else:
                return map(lambda x: [int(reader.morton2coordst4D(x[0])/self.scale), 
                                      reader.morton2coordsX4D(x[0], self.offx, self.scalex, self.roundNum), 
                                    reader.morton2coordsY4D(x[0], self.offy, self.scaley, self.roundNum), 
                                    reader.morton2coordsZ4D(x[0], self.offz, self.scalez, self.roundNum)], result)
    
    def list2ScaleOffset(self, lst):
        """
        Offsets and scales the provided list.
        """
        t = []
        if len(lst[0]) == 2:
            for i in lst:
                t.append((int(round((i[0] - self.offx) / self.scalex, 0)), int(round((i[1] - self.offy) / self.scaley, 0))))
        if len(lst[0]) == 3:
            for i in lst:
                t.append((int(round((i[0] - self.offx) / self.scalex, 0)), int(round((i[1] - self.offy) / self.scaley, 0)), int(round((i[2] - self.offz) / self.scalez, 0))))
        return t

    def storeQuery(self, tableName, columns, data, check = False):
        """
        Store the first query approximation into a temporary table.
        """
        if len(data) > 0:
            connection = self.getConnection()
            cursor = connection.cursor()
            ora.createTable(cursor, tableName, columns, check)
            ora.insertInto(cursor, tableName, data)
            connection.commit()
            return len(data)
        else:
            print 'No data returned'
            return []
            
    def sqlldr(self, tableName, cols, data):
        """
        Generates the control file for the sqlldr and composes the sqlloader
        command.
        """
        commonFile = 'ranges'
        controlFile = commonFile + '.ctl'
        badFile = commonFile + '.bad'
        logFile = commonFile + '.log'
        
        ctfile = open(controlFile,'w')
        sqlldrCols = []
        for i in range(len(cols)):
            column = cols[i]
            
            sqlldrCols.append(column + ' ' + general.DM_SQLLDR[column][0] + ' external(' + str(general.DM_SQLLDR[column][1]) + ')')
        
        ctfile.write("""load data
INFILE *
append into table """ + tableName + """
fields terminated by ','
(""" + (',\n'.join(sqlldrCols)) + """)""")
        ctfile.write('\nBEGINDATA\n')
        ctfile.write(data)
        ctfile.close()
        sqlLoaderCommand = "sqlldr " + self.getConnectString() + " direct=true control=" + controlFile + ' bad=' + badFile + " log=" + logFile
        
        print "Sqlldr command for inserting the ranges\n"
        print sqlLoaderCommand
        print 
        
        return sqlLoaderCommand
              
    def getPointInPolygonStatement(self, approxTable, columns, columnsPIP, condition):
        """
        Composes the Point In Polygon SQL statement.
        """
        return 'SELECT ' + ora.getHintStatement(ora.getParallelStringQuery(self.numProcesses)) + ora.getSelectColumns('*') + """ 
FROM TABLE(mdsys.sdo_PointInPolygon(CURSOR(
""" + ora.getSelectStatement(approxTable, ora.getSelectColumns(columnsPIP)) + """), 
MDSYS.SDO_GEOMETRY('""" + self.wkt + """', """ + str(self.srid) + """), """ + str(self.tolerance) +"""))
""" + condition


def getTime(granularity, start, end):
    if start == None and end == None:
        return [[]]
    elif end == None and granularity == 'day':
        return [["TO_DATE('{0}', 'YYYY/MM/DD')".format(reader.formatTime(start)), None]] 
    elif granularity == 'year':
        return [[start, end]]
    elif granularity == 'day':
        return [["TO_DATE('{0}', 'YYYY/MM/DD')".format(reader.formatTime(start)),
                 "TO_DATE('{0}', 'YYYY/MM/DD')".format(reader.formatTime(end))]]
                 
def format_lst(lst):
    return '\n'.join([','.join(map(str,i)) for i in lst])
