# -*- coding: utf-8 -*-
"""
Created on Thu May 19 08:50:37 2016

@author: Stella Psomadaki
"""

import pointcloud.reader as reader
from pointcloud.Validator import Validate
from pointcloud.general import getFiles
import numpy as np
import sys

def load(ini):
    initialise = Validate(ini)
    files = getFiles(initialise.directory, ['laz'], True)
    files.sort()
    
    for cfile in files:
        f = reader.readFileLaspy(cfile)
        t = parseTimeFromFilename(cfile, initialise.dataset)
        charar = np.chararray(len(f), itemsize=10)
        charar[:] = t
        gtype = np.empty(len(f)).transpose()
        gtype.fill(3001)
        srid = np.empty(len(f)).transpose()
        srid.fill(28992)
        data = np.vstack((charar, gtype, srid, f.x, f.y, f.z)).transpose()
        print format(data)

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
