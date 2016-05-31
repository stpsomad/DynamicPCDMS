# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 2015

@author: Stella Psomadaki

This module contains general purpose functions and variables used in the majority
of the scripts
"""
import os


#default file formats used
PC_FILE_FORMATS = ['las','laz','LAS', 'LAZ'] 

#offsets
OFFSET_ZANDMOTOR = 69000, 440000, -100, 0
OFFSET_COASTLINE = 4000, 374000, -100, 0

#scales
SCALE_ZANDMOTOR = 0.001, 0.001, 0.001, 1
SCALE_COASTLINE = 0.01, 0.01, 0.01, 1

#Dutch SRID
SRID = 28992

DM_SQLLDR = {
    'x': ('float', 10),
    'y': ('float', 10),
    'z': ('float', 8),
    'X': ('integer', 20),
    'Y': ('integer', 20),
    'Z': ('integer', 8),
    't': ('integer', 10),
    'm': ('integer', 40), #40 to be suitable for the deep in 4D integration also
    'LOW': ('integer', 40),
    'UPPER': ('integer', 40),
    }

DM_FLAT = { # The name of the column in the DB is computed with getDBColumn
    'x': 'NUMBER',
    'y': 'NUMBER',
    'z': 'FLOAT',
    'X': 'NUMBER',
    'Y': 'NUMBER',
    'Z': 'NUMBER',
    't': 'NUMBER',
    'm': 'NUMBER', # Extra dimension for the Morton code
    'LOW': 'NUMBER',
    'UPPER': 'NUMBER'
}

EQ_DIM = {
    'morton' : 'm',
    'z' : 'z',
    'time' : 't'
}

REVERSE_DIM = {
    'm': 'morton',
    'z': 'z',
    't': 'time'}

DIRS = {
    'part1': '/pak2/usrdata/stella/Zandmotor_bench/part1/',
    'part2': '/pak2/usrdata/stella/Zandmotor_bench/part2/',
    'part3': '/pak2/usrdata/stella/Zandmotor_bench/part3/',
    'c2012': '/pak2/usrdata/stella/Coastline_bench/medium/2012/',
    'c2013': '/pak2/usrdata/stella/Coastline_bench/medium/2013/',
    'c2014': '/pak2/usrdata/stella/Coastline_bench/medium/2014/',
    'c2015': '/pak2/usrdata/stella/Coastline_bench/medium/2015/'}
    
def getFiles(inputElement, extensions = PC_FILE_FORMATS, recursive = False):
    """ Get the list of files with certain extensions contained in the folder 
    (and possible subfolders) given by inputElement. 
    If inputElement is directly a file it returns a list with only one element, 
    the given file.
    To search the subfolders use recursive = True
    Adapted from: https://github.com/NLeSC/pointcloud-benchmark/blob/master/python/pointcloud/utils.py
    
    Apache License
    Version 2.0, January 2004"""

    if type(extensions) == str:
        extensions = [extensions,]
    if type(inputElement) == str:
        inputElement = [inputElement,]
        
    absPaths=[]

    for directory in inputElement:
        inputElementAbsPath = os.path.abspath(directory)
        if os.path.isdir(inputElementAbsPath):
            elements = sorted(os.listdir(inputElementAbsPath), key=str.lower)
            for element in elements:
                elementAbsPath = os.path.join(inputElementAbsPath,element) 
                if os.path.isdir(elementAbsPath):
                    if recursive:
                        absPaths.extend(getFiles(elementAbsPath, extensions))
                else: 
                    isValid = False
                    for extension in extensions:
                        if elementAbsPath.endswith(extension):
                            isValid = True
                    if isValid:
                        absPaths.append(elementAbsPath)
        elif os.path.isfile(inputElementAbsPath):
            isValid = False
            for extension in extensions:
                if inputElementAbsPath.endswith(extension):
                    isValid = True
            if isValid:
                absPaths = [inputElementAbsPath,]
        else:
            raise Exception("ERROR: inputElement is neither a valid folder nor file")
    return absPaths