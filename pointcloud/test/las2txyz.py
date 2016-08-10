# -*- coding: utf-8 -*-
"""
Created on Thu May 19 08:50:37 2016

@author: Stella Psomadaki
"""

import pointcloud.reader as reader
from pointcloud.test.Validator import Validate
from pointcloud.general import getFiles, SRID
import numpy as np
import sys
import time

def load(ini):
    initialise = Validate(ini)
    
    # fresh reload or not
    directories = []
    if initialise.reload is True:
        i = int(initialise.ORCLdirectory[-1])
        while i >= 1:
            directories.append(initialise.directory[:-2] + str(i))
            i -= 1
        directories.sort()
    else:
        directories = initialise.directory
    

    # get the name of the laz files in the directories
    files = getFiles(directories, ['laz'], True)
    files.sort()
    
    counter = 0 # for timing the morton conversion - workaround
    
    for cfile in files:
        start = time.time()
        f = reader.readFileLaspy(cfile)
        t = parseTimeFromFilename(cfile, initialise.dataset)
        charar = np.chararray(len(f), itemsize=10)
        charar[:] = t
        gtype = np.empty(len(f)).transpose()
        gtype.fill(3001)
        srid = np.empty(len(f)).transpose()
        srid.fill(SRID)
        data = np.vstack((charar, gtype, srid, f.x, f.y, f.z)).transpose()
        counter += (time.time() - start)
        print format(data)
        
    #work-around
    fl = open('validate_prep.txt', 'a')
    fl.write(str(counter))
    fl.write(str('\n'))
    fl.close()

def format(lst):
    return '\n'.join([','.join(map(str,i)) for i in lst])
    
def parseTimeFromFilename(name, dataset):
    """This function extracts the time information from the file name depending
    on the use case.
    Zandmotor are in the format yyyy_mm_dd
    Coastline are in the format yyyy_[other]"""
    if dataset.lower() in ['zandmotor']:
        return name[name.rfind('/')+1:-4] #YYYY_MM_DD
    elif dataset.lower() in ['coastline']:
        return name[name.rfind('/')+1:name.rfind('/')+5] + "_01_01" # YYYY_01_01       
        
if __name__ == "__main__":
    load(sys.argv[1])
