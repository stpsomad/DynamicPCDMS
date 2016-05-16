# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 14:38:47 2016

@author: Stella Psomadaki

The morton converter
"""

from pointcloud.AbstractLoader import Oracle
import pointcloud.oracleTools as ora
from pointcloud.general import getFiles, OFFSET_ZANDMOTOR, OFFSET_COASTLINE, SCALE_ZANDMOTOR, SCALE_COASTLINE, SRID
import pointcloud.reader as reader
import numpy as np
import sys
import time

def perform(function, *args):
    return function(*args)
    
def mortonXYTloose(f, t, offx, offy, scalex, scaley):
    timear = np.empty(len(f)).transpose()
    timear.fill(t)
    return map(lambda x: [x[0], reader.Encode2Morton2D(x[1], x[2], offx, offy, scalex, scaley), x[3]], np.vstack((timear, f.x, f.y, f.z)).transpose())

def mortonXYZTloose(f, t, offx, offy, offz, scalex, scaley, scalez):
    timear = np.empty(len(f)).transpose()
    timear.fill(t)
    return map(lambda x: [int(x[0]), reader.Encode2Morton3D(x[1], x[2], x[3], offx, offy, offz, scalex, scaley, scalez)], np.vstack((timear, f.x, f.y, f.z)).transpose())

def mortonXYTdeep(f, t, offx, offy, scalex, scaley):
     return map(lambda x: [reader.Encode2Morton3D(t, x[0], x[1], 0, offx, offy, 1, scalex, scaley), x[2]], np.vstack((f.x, f.y, f.z)).transpose())

def mortonXYZTdeep(f, t, offx, offy, offz, scalex, scaley, scalez):
    timear = np.empty(len(f)).transpose()
    timear.fill(t)
    return map(lambda x: [reader.Encode2Morton4D(int(x[0]), x[1], x[2], x[3], offx, offy, offz, scalex, scaley, scalez)], np.vstack((timear, f.x, f.y, f.z)).transpose())

def converter(ini_file):
    initialise = Oracle(ini_file)
    connection = initialise.getConnection()
    files = getFiles(initialise.directory, ['laz'], True)
    files.sort()
    cursor = connection.cursor()
    
    if initialise.dataset.lower() == 'zandmotor':
        offx, offy, offz, offt = OFFSET_ZANDMOTOR
        scalex, scaley, scalez, scalet = SCALE_ZANDMOTOR
    elif initialise.dataset.lower() == 'coastline':
        offx, offy, offz, offt = OFFSET_COASTLINE
        scalex, scaley, scalez, scalet = SCALE_COASTLINE    
    
    if initialise.integration == 'loose':
        if initialise.parse == 'xyt':
            funct = mortonXYTloose
            args = offx, offy, scalex, scaley
        elif initialise.parse == 'xyzt':
            funct = mortonXYZTloose
            args = offx, offy, offz, scalex, scaley, scalez
    elif initialise.integration == 'deep':
        if initialise.parse == 'xyt':
            funct = mortonXYTdeep
            args = offx, offy, scalex, scaley
        elif initialise.parse == 'xyzt':
            funct = mortonXYZTdeep
            args = offx, offy, offz, scalex, scaley, scalez
    
    index = True
    init = initialise.init
    counter = 0
    for cfile in files:
        start = time.time()
        f = reader.readFileLaspy(cfile)
        minxyz, maxxyz = reader.getMinMaxLaspy(f)
        t = parseTimeFromFilename(cfile, initialise.dataset)
        updateMetaTable(connection, cursor, initialise.metaTable, SRID, minxyz[0], minxyz[1], minxyz[2], maxxyz[0], maxxyz[1], maxxyz[2], t, scalex, scaley, scalez, offx, offy, offz, init)
        
        
        morton = perform(funct, f, t * initialise.scale, *args)
        index = False 
        
        #Making sure the meta is not initialised again
        if (init == 'True' or init == True) and index == False:
            init = False 
        else:
            init = False
        
        a = formatMorton(morton)
        counter += (time.time() - start)
        if initialise.loader == 'sqlldr':
            print a
        else:
            fh = open(file[file.rfind('/') + 1:file.rfind('.')] + '.txt', 'w')
            fh.write(a)
            fh.close()
            
    #ugly workaround, how else?
    f = open('morton_{0}.txt'.format(initialise.iotTableName), 'a')
    f.write(str(counter))
    f.write(str('\n'))
    f.close()
    return counter

def formatMorton(lst):
    return '\n'.join([', '.join(map(str,i)) for i in lst])
 
def parseTimeFromFilename(name, dataset):
    """This function extracts the time information from the file name depending
    on the use case.
    Zandmotor are in the format yyyy_mm_dd
    Coastline are in the format yyyy_[other]"""
    if dataset.lower() in ['zandmotor']:
        date = map(int, name[name.rfind('/')+1:-4].split('_'))
        return reader.daySinceEpoch(date[0], date[1], date[2])
    elif dataset.lower() in ['coastline']:
        return int(name[name.rfind('/')+1:name.rfind('/')+5])
   
def updateMetaTable(connection, cursor, metaTable, srid, minx, miny, minz, maxx, maxy, maxz, t, scalex, scaley, scalez, offx, offy, offz, typel):    
    if typel == False or typel == 'False':
        cursor.execute("SELECT minx, miny, minz, mint, maxx, maxy, maxz, maxt FROM {0}".format(metaTable))
        res = cursor.fetchall()[0]
        mint, maxt = t, t
        if res[0] <= minx: minx = res[0]
        if res[1] <= miny: miny = res[1]
        if res[2] <= minz: minz = res[2]
        if res[3] <= mint: mint = res[3]
        if res[4] >= maxx: maxx = res[4]
        if res[5] >= maxy: maxy = res[5]
        if res[6] >= maxz: maxz = res[6]
        if res[7] >= maxt: maxt = res[7]
        
        ora.updateMetaTableValues(connection, cursor, metaTable, minx, miny, minz, mint, maxx, maxy, maxz, maxt)
    else:
        ora.populateMetaTable(connection, cursor, metaTable, srid, minx, miny, minz, t, maxx, maxy, maxz, t, scalex, scaley, scalez, offx, offy, offz)
 
if __name__ == "__main__":
    converter(sys.argv[1])
