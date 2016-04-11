## -*- coding: utf-8 -*-
#"""
#Created on Tue Dec 22 09:20 2015
#Expanded on Thu Feb 04 2016
#
#@author: Stella Psomadaki
#"""
#
try:
	from laspy.file import File as LaspyFile
except ImportError:
	print "Laspy module cannot be found"
	raise

import morton as morton
import time 
from time import strptime, localtime
import datetime
# use numba for efficiency
from numba import jit

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug",
          "Sep", "Oct", "Nov", "Dec"]

def readFileLaspy(filename):
    """Function to read a file using Laspy
    Takes a file name as input
    Returns a file object"""
    return LaspyFile(filename, mode = "r")

def getMinMaxLaspy(f):
    """Returns the minimum and maximum in the x, y, z dimensions
    found in the laz files"""
    return f.header.min, f.header.max
    
###############################################################################
######################  Morton conversions related code  ######################
###############################################################################
    
# Encode in 2D case
@jit
def Encode2Morton2D(x, y, offx, offy, scalex, scaley):
    """ offx: The offset in the x axis,
        offy: The offset in the y axis
        scalex, scaley: the scale in x and y axis

        Returns: A 2D morton code
    """
    return morton.EncodeMorton2D(int(round((x - offx)/scalex,0)), int(round((y - offy)/scaley,0)))

# Encode in 3D case
@jit
def Encode2Morton3D(x, y, z, offx, offy, offz, scalex, scaley, scalez):
    """Calculate a 3D morton code. 
        Takes as input the three dimensions to include in the code in the order
        inserted. Also, requires the offsets and scales per included dimension.

        Returns: A 3D morton code
    """
    return morton.EncodeMorton3D(int(round((x - offx)/scalex, 0)), int(round((y - offy)/scaley, 0)), int(round((z - offz)/scalez, 0)))
    
# Encode in 4D case
@jit
def Encode2Morton4D(t, x, y, z, offx, offy, offz, scalex, scaley, scalez):
    """
        Returns: A 4D morton code
    """
    return morton.EncodeMorton4D(t, int(round((x - offx)/scalex, 0)), int(round((y - offy)/scaley, 0)), int(round((z - offz)/scalez, 0)))

# Decode in 2D case
@jit
def morton2coordsX2D(m, off, scale, res):
    return round((morton.DecodeMorton2DX(m))*scale + off, res)

@jit
def morton2coordsY2D(m, off, scale, res):
    return round(morton.DecodeMorton2DY(m)*scale + off, res)

# Decode in 3D case
def morton2coordst3D(m, res = 0):
    """Use only when the integration is deep.
    Returns time in Unix time"""
    return round(morton.DecodeMorton3DX(m), 0)

def morton2coordsX3D(m, offx, scalex, res):
    return round(morton.DecodeMorton3DX(m)*scalex + offx, res)

def morton2coordsY3D(m, offy, scaley, res):
    return round(morton.DecodeMorton3DY(m)*scaley + offy ,res)

def morton2coordsZ3D(m, offz, scalez, res):
    return round(morton.DecodeMorton3DZ(m)*scalez + offz ,res) #depends on the use case how to decode
    
# Decode in 4D case
def morton2coordst4D(m, res = 0):
    return round(morton.DecodeMorton4Dt(m), res)   
    
def morton2coordsX4D(m, offx, scalex, res):
    return round(morton.DecodeMorton4DX(m)*scalex + offx, res)

def morton2coordsY4D(m, offy, scaley, res):
    return round(morton.DecodeMorton4DY(m)*scaley + offy ,res)

def morton2coordsZ4D(m, offz, scalez, res):
    return round(morton.DecodeMorton4DZ(m)*scalez + offz ,res) #depends on the use case how to decode


###############################################################################
######################   Time conversion related code    ######################
###############################################################################

def time2integer(year, month, day):
    """ Takes a time tuple in the format (day, month, year)
    and returns an integer of the number of seconds since the start of the epoch
    1st January 1970"""
    if day < 10:
        return int(time.mktime(strptime("0{0} {1} {2}".format(day,MONTHS[month-1], year), "%d %b %Y")))
    else:
        return int(time.mktime(strptime("{0} {1} {2}".format(day,MONTHS[month-1], year), "%d %b %Y")))  #Y if 4-digit year

def integer2time(t):
    tup = localtime(t)
    return tup[0], tup[1], tup[2]
      
def time2offset(t):
    return t - 1200000000
    
def offset2time(t):
    return t + 1200000000
    
def encodeTime(t):
    return time2integer(t[0],t[1],t[2])
    
def daySinceEpoch(year, month, day, start = 1990):
    return (datetime.datetime(year, month, day) - datetime.datetime(start, 1, 1)).days + 1
    
def inverseDaySinceEpoch(num):
    d = datetime.date.fromordinal(726467 + num)
    return d.year, d.month, d.day

def decodeTime(t):
    """Decode from Unix time to a tuple in the format (yyyy, mm, dd)"""
    return integer2time(t)

def formatTime(t):
    if t[1] < 10:
        if t[2] < 10:
            return '{0}/0{1}/0{2}'.format(t[0],t[1],t[2])
        else:
            return '{0}/0{1}/{2}'.format(t[0],t[1],t[2])
    else:
        return '{0}/{1}/{2}'.format(t[0],t[1],t[2])