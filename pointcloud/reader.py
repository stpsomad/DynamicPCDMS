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
    """
    Function to read a file using Laspy.
    
    Args:
        filename (str): the name of the las/laz file

    Returns:
        LaspyFile: returns a Laspy file object
    """
    return LaspyFile(filename, mode = "r")

def getMinMaxLaspy(f):
    """
    Function that returns the minimum and maximum of the spatial dimensions as stored
    in the las file header.
    
    Args:
        f (Laspy file object): a laspy file object
        
    Returns:
        List of minimun and list of maximum of the x, y, z dimensions.
        [xmin, ymin, zmin], [xmax, ymax, zmax]
    """
    return f.header.min, f.header.max
    
###############################################################################
######################  Morton conversions related code  ######################
###############################################################################
    
@jit
def Encode2Morton2D(x, y, offx, offy, scalex, scaley):
    """
    Encodes the morton code in the 2D space by applying a linear transformation 
    to the coordinates.

    Args:
        x (float): the x dimension,
        y (float): the y dimension,
        offx (float): the offset of the x dimension,
        offy (float): the offset of the y dimension,
        scalex (float): the scale in the x axis,
        scaley (float): the scale in the y axis,
        
    Returns: 
        int: A 2D morton code
    """
    return morton.EncodeMorton2D(int(round((x - offx)/scalex,0)),
                                 int(round((y - offy)/scaley,0)))

@jit
def Encode2Morton3D(x, y, z, offx, offy, offz, scalex, scaley, scalez):
    """
    Encodes the morton code in the 3D space by applying a linear transformation 
    to the coordinates.

    Args:
        x (float): the x dimension,
        y (float): the y dimension,
        z (float): the z dimension,
        offx (float): the offset of the x dimension,
        offy (float): the offset of the y dimension,
        offz (float): the offset of the z dimension,
        scalex (float): the scale of the x axis,
        scaley (float): the scale of the y axis,
        scalez (float): the scale of the z axis

    Returns: 
        int: A 3D morton code
    """

    return morton.EncodeMorton3D(int(round((x - offx)/scalex, 0)), 
                                 int(round((y - offy)/scaley, 0)),
                                 int(round((z - offz)/scalez, 0)))
    
@jit
def Encode2Morton4D(t, x, y, z, offx, offy, offz, scalex, scaley, scalez):
    """
    Encodes the morton code in the 4D space by applying a linear transformation 
    to the coordinates.

    Args:
        t (int): the time dimension
        x (float): the x dimension
        y (float): the x dimension
        z (float): the z dimension
        offx (float): the offset of the x dimension,
        offy (float): the offset of the y dimension,
        offz (float): the offset of the z dimension,
        scalex (float): the scale of the x axis,
        scaley (float): the scale of the y axis,
        scalez (float): the scale of the z axis

    Returns: 
        int: A 4D morton code
    """
    return morton.EncodeMorton4D(t, int(round((x - offx)/scalex, 0)),
                                 int(round((y - offy)/scaley, 0)),
                                 int(round((z - offz)/scalez, 0)))

@jit
def morton2coordsX2D(m, off, scale, res):
    """
    Decodes to the original x dimension by applying a linear transformation 
    
    Args:
        m (int): the 2D morton code
        off (float): the offset of the x dimension
        scale (float): the scale of the x dimension
        res (int): the number of decimal digits
        
    Returns:
        float: the original scaled and translated x dimension
    """
    return round((morton.DecodeMorton2DX(m))*scale + off, res)

@jit
def morton2coordsY2D(m, off, scale, res):
    """
    Decodes to the original y dimension by applying a linear transformation 
    
    Args:
        m (int): the 2D morton code
        off (float): the offset in the y dimension
        scale (float): the scale in the y dimension
        res (int): the number of decimal digits
        
    Returns:
        float: the original scaled and translated y dimension
    """
    return round(morton.DecodeMorton2DY(m)*scale + off, res)

def morton2coordst3D(m, res = 0):
    """
    Decodes to the original t dimension by applying a linear transformation 
    
    Args:
        m (int): the 3D morton code
        res (int): the number of decimal digits
        
    Returns:
        int: years/days/seconds since epoch
    """
    return round(morton.DecodeMorton3DX(m), res)

def morton2coordsX3D(m, off, scale, res):
    """
    Decodes to the original x dimension by applying a linear transformation 
    
    Args:
        m (int): the 3D morton code
        off (float): the offset in the x dimension
        scale (float): the scale in the x dimension
        res (int): the number of decimal digits
        
    Returns:
        float: the original scaled and translated x dimension
    """
    return round(morton.DecodeMorton3DX(m)*scale + off, res)

def morton2coordsY3D(m, off, scale, res):
    """
    Decodes to the original y dimension by applying a linear transformation 
    
    Args:
        m (int): the 3D morton code
        off (float): the offset of the y dimension
        scale (float): the scale of the y dimension
        res (int): the number of decimal digits
        
    Returns:
        float: the original scaled and translated y dimension
    """
    return round(morton.DecodeMorton3DY(m)*scale + off, res)

def morton2coordsZ3D(m, off, scale, res):
    """
    Decodes to the original z dimension by applying a linear transformation 
    
    Args:
        m (int): the 3D morton code
        off (float): the offset of the z dimension
        scale (float): the scale of the z dimension
        res (int): the number of decimal digits
        
    Returns:
        float: the original scaled and translated z dimension
    """
    return round(morton.DecodeMorton3DZ(m)*scale + off, res) 
    
def morton2coordst4D(m, res = 0):
    """
    Decodes to the original t dimension by applying a linear transformation 
    
    Args:
        m (int): the 4D morton code
        res (int): the number of decimal digits
        
    Returns:
        int: days since 1/1/1990
    """
    return round(morton.DecodeMorton4Dt(m), res)   
    
def morton2coordsX4D(m, offx, scalex, res):
    """
    Decodes to the original x dimension by applying a linear transformation 
    
    Args:
        m (int): the 4D morton code
        off (float): the offset of the x dimension
        scale (float): the scale of the x dimension
        res (int): the number of decimal digits
        
    Returns:
        float: the original scaled and translated x dimension
    """
    return round(morton.DecodeMorton4DX(m)*scalex + offx, res)

def morton2coordsY4D(m, offy, scaley, res):
    """
    Decodes to the original y dimension by applying a linear transformation 
    
    Args:
        m (int): the 4D morton code
        off (float): the offset of the y dimension
        scale (float): the scale of the y dimension
        res (int): the number of decimal digits
        
    Returns:
        float: the original scaled and translated y dimension
    """
    return round(morton.DecodeMorton4DY(m)*scaley + offy ,res)

def morton2coordsZ4D(m, offz, scalez, res):
    """
    Decodes to the original z dimension by applying a linear transformation 
    
    Args:
        m (int): the 4D morton code
        off (float): the offset of the z dimension
        scale (float): the scale of the z dimension
        res (int): the number of decimal digits
        
    Returns:
        float: the original scaled and translated z dimension
    """
    return round(morton.DecodeMorton4DZ(m)*scalez + offz ,res)
    

###############################################################################
######################   Time conversion related code    ######################
###############################################################################

def time2integer(year, month, day):
    """
    Encodes time into the Unix time i.e. the number of seconds since the start of
    the epoch 1st January 1970 
    
    Args:
        year (int): year (in 4 digit format e.g. 2000)
        month (int): month
        day (int): day
        
    Returns:
        int: the number of seconds since the start of the epoch 1st January 1970
    """
    if day < 10:
        return int(time.mktime(strptime("0{0} {1} {2}".format(day, MONTHS[month-1], year), "%d %b %Y")))
    else:
        return int(time.mktime(strptime("{0} {1} {2}".format(day, MONTHS[month-1], year), "%d %b %Y")))

def decodeFromUnix(t):
    """
    Decodes the Unix time in the format (yyyy, mm, dd)
    
    Args:
        t (int): Unix time

    Returns:
        tuple: year, month, day
    """
    tup = localtime(t)
    return tup[0], tup[1], tup[2]
   
def encodeTime(t):
    """
    Calls the time2integer function by unpacking the list or tuple provided

    Args:
        date (tuple): year, month, day
        
    Returns:
        int: the number of seconds since the start of the epoch 1st January 1970
    """
    return time2integer(t[0],t[1],t[2])
    
def daySinceEpoch(year, month, day, start = 1990):
    """
    Encodes time into the number of days since the start of epoch specified
    
    Args:
        year (int): year (in 4 digit format e.g. 2000)
        month (int): month
        day (int): day
        start (int): the starting year
        
    Returns:
        int: the number of seconds since the start of the epoch
    """
    return (datetime.datetime(year, month, day) - datetime.datetime(start, 1, 1)).days + 1
    
def inverseDaySinceEpoch(num, start = 1990):
    """
    Decodes time into the date format (year, month, day) according to the start of
    the epoch specified
    
    Args:
        num (int): the days passes since the start of the epoch
        start (int): the starting year
        
    Returns:
        tuple: the year, month and day
    """
    d = datetime.date.fromordinal(datetime.datetime(start, 1, 1) + num  - 1)
    return d.year, d.month, d.day

def formatTime(t):
    """
    Formats time into the format yyyy/mm/dd.
    
    Args:
        t (tuple): the original date
        
    Returns:
        string: yyyy/mm/dd
        
    Examples:
        1990/1/1 --> 1990/01/01
    
    """
    if t[1] < 10:
        if t[2] < 10:
            return '{0}/0{1}/0{2}'.format(t[0], t[1], t[2])
        else:
            return '{0}/0{1}/{2}'.format(t[0], t[1], t[2])
    else:
        return '{0}/{1}/{2}'.format(t[0], t[1], t[2])