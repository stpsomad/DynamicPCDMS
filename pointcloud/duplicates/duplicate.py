# -*- coding: utf-8 -*-
"""
Created on Sun Jan 03 20:20 2016

@author: Stella Psomadaki

Command line executable to remove duplicates. Only one folder per run.

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
from pointcloud.utils import getFiles
import sys, getopt

def removeDuplicate(file):
    """Removes duplicate points based on X, Y coordinates

       Returns a numpy array"""
    df = DataFrame(np.vstack((file.x, file.y, file.z)).transpose(), columns=['X', 'Y', 'Z'])
    df.drop_duplicates(subset=['X','Y'], inplace=True)
    return df.values

def writeFile(directory,name,header,coords):
    """Write a laz file using laspy and numpy arrays"""
    output = File(directory + name, mode = "w", header=header)
    output.x = coords[0]
    output.y = coords[1]
    output.z = coords[2]
    output.close()

def checkDirectory(directory):
    """ Checks if the specified directory exists, and otherwise it creates it"""
    try: 
        os.makedirs(directory)
    except OSError:
        if not os.path.isdir(directory):
            raise

def lasDuplicateFree(directory, output):
    """ Takes a directory with las [laz] files and an output directory

        Returns las, [laz] files free from duplicates"""
    files = getFiles(directory,['laz'],True)
    checkDirectory(output)
    for file in files:
        print file
        fh = readFileLaspy(file)
        writeFile(output,file[file.rfind('\\')+1:],fh.header,removeDuplicate(fh).transpose())

def main(argv):
    inputdir = ''
    outputdir = ''
    try:                                
        opts, args = getopt.getopt(argv, "hi:o:", ["help", "input=", "output="])
    except getopt.GetoptError:
        print 'lasduplicate.py -i <inputDirectory> -o <outputDirectory>'
        sys.exit(2)                     
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print argv[0] +' -i <inputDirectory> -o <outputDirectory>'
            sys.exit()                  
        elif opt in ("-i", "--input"):
            inputdir = arg            
        elif opt in ("-o", "--output"):
            outputdir = arg
    lasDuplicateFree(inputdir, outputdir)

if __name__ =="__main__":
    start = time.time()
    main(sys.argv[1:])
    end = time.time()
    print "Finished in ", end - start
    #Example run: python duplicate.py -i D:\ -o D:\output\
