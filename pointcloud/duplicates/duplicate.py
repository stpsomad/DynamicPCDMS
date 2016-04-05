# -*- coding: utf-8 -*-
"""
Created on Sun Jan 03 20:20 2016

@author: Stella Psomadaki

This module takes a laz, las file and removes duplicate points in the 
(x,y) dimensions. This is an important preprocessing step because the IOT
does not allow duplicates in the index.
"""

import numpy as np
from pandas import DataFrame
import time 
from laspy.file import File
import os
from pointcloud.reader import readFileLaspy
from pointcloud.general import getFiles


def removeDuplicate(cfile):
    """Removes duplicate points based on X, Y coordinates

       Returns a numpy array"""
    df = DataFrame(np.vstack((cfile.x, cfile.y, cfile.z)).transpose(), columns=['X', 'Y', 'Z'])
    df.drop_duplicates(subset=['X', 'Y'], inplace=True)
    return df.values


def writeFile(directory, name, header, coords):
    """Write a laz file using laspy and numpy arrays"""
    output = File(directory + name, mode="w", header=header)
    output.x = coords[0]
    output.y = coords[1]
    output.z = coords[2]
    output.close()


def checkDirectory(directory):
    """ Checks if the specified directory exists,
    and otherwise it creates it"""
    try:
        os.makedirs(directory)
    except OSError:
        if not os.path.isdir(directory):
            raise

def lasDuplicateFree(directory, output):
    """ Takes a directory with las [laz] files and an output directory

        Returns las, [laz] files free from duplicates"""
    files = getFiles(directory,['laz'], False)
    checkDirectory(output)
    for cfile in files:
        print cfile
        fh = readFileLaspy(cfile)
        writeFile(output,cfile[cfile.rfind('\\')+1:],fh.header,removeDuplicate(fh).transpose())

if __name__ =="__main__":
    paths  = ['D:/Dropbox/Thesis/Thesis/Data/from2016/']
    pathOutput = 'D:/Dropbox/Thesis/Thesis/Data/Cleaned_datasets/Zandmotor_bench/'
    outputs = [pathOutput + 'noise/']
    
    start = time.time()
    for path in range(len(paths)):
        lasDuplicateFree(paths[path], outputs[path])
    end = time.time()
    print "Finished within {0} seconds".format(end - start)
