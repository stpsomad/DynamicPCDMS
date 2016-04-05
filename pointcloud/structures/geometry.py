# -*- coding: utf-8 -*-
"""
Created on Sat Feb 20 16:40:42 2016

@author: Stella Psomadaki
"""
from math import pi
from shapely.geometry import box
from de9im.patterns import intersects, contains, pattern
excluding_interiors = pattern('F********')

#==============================================================================
# Point geometries in 3D, 3D (time, x, y) and 4D
#==============================================================================

class Point3D(object):
    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z


class Point4D(object):
    def __init__(self, t, x, y, z):
        self.t = t
        self.x = x
        self.y = y
        self.z = z


class dynamicPoint(object):
    def __init__(self, t, x, y):
        self.t = t
        self.x = x
        self.y = y
        

#==============================================================================
# 3D geometries: Cube, Polygon3D and Sphere
#==============================================================================

class Cube(object):
    def __init__(self, pt_ll, pt_ur):
        """Constructor. 
        Takes the lower left and upper right point defining the Cube.
        """
        assert isinstance(pt_ll, Point3D)
        assert isinstance(pt_ur, Point3D)
        self.ll = pt_ll
        self.ur = pt_ur
        
    def volume(self):
        return (self.ur.x - self.ll.x)*(self.ur.y - self.ll.y)*(self.ur.z - self.ll.z)
        
    def getWkt(self):
        return box(self.ll.x, self.ll.y, self.ur.x, self.ur.y)
    
    def relationship(self, other):
        """ self = geom1
            other = geom2
            0 if they are disjoint
            1 if geom2 completely in geom1
            2 if there is some intersection between geom1 and geom2"""        
        
        if isinstance(other, Cube):
            if self.ll.x <= other.ll.x and self.ur.x >= other.ur.x and self.ll.y <= other.ll.y and self.ur.y >= other.ur.y:
                if self.ll.z <= other.ll.z and self.ur.z >= other.ur.z:
                    return 1
                elif self.ll.z >= other.ur.z or self.ur.z <= other.ll.z:
                    return 0
                else:
                    return 2
            elif self.ll.x >= other.ur.x or self.ll.y >= other.ur.y or self.ll.z >= other.ur.z or self.ur.x <= other.ll.x or self.ur.y <= other.ll.y or self.ur.z <= other.ll.z:            
                return 0
            return 2
            
        elif isinstance(other, Sphere):
            if self.ll.x <= other.ctr.x - other.radius and self.ll.y <= other.ctr.y - other.radius and self.ll.z <= other.ctr.z - other.radius and self.ur.x >= other.ctr.x + other.radius and self.ur.y >= other.ctr.y + other.radius and self.ur.z >= other.ctr.z + other.radius:
                return 1
            
            # Source: A SIMPLE METHOD FOR BOX – SPHERE INTERSECTION TESTING
            # James Arvo - Graphic Gems 1
            # C code in http://www.realtimerendering.com/resources/GraphicsGems/gems/BoxSphere.cs
            d = 0            
            if other.ctr.x < self.ll.x:
                e = other.ctr.x - self.ll.x
                d += e**2
            elif other.ctr.x > self.ur.x:
                e = other.ctr.x -self.ur.x
                d += e**2
                
            if other.ctr.y < self.ll.y:
                e = other.ctr.y - self.ll.y
                d += e**2
            elif other.ctr.y > self.ur.y:
                e = other.ctr.y - self.ur.y
                d += e**2
            if other.ctr.z < self.ll.z:
                e = other.ctr.z - self.ll.z
                d += e**2
            elif other.ctr.z > self.ur.z:
                e = other.ctr.z -self.ur.z
                d += e**2
                
            if d >= other.radius**2:
                return 0
            return 2
            
        elif isinstance(other, Polygon3D):
            geom1 = self.getWkt()
            minz, maxz = self.ll.z, self.ur.z
            relation = geom1.relate(other.wkt)
            if not intersects.matches(relation):
                return 0 # they are disjoint
            elif contains.matches(relation):
                if  minz <= other.zmin <= maxz and minz <= other.zmax <= maxz:
                    return 1
                elif maxz < other.zmax or minz > other.zmin:
                    return 0
                else:
                    return 2
            else: # there is some overlap
                if excluding_interiors.matches(relation):
                    return 0 # overlap only in boundaries, we do not count it
                else:
                    if maxz < other.zmin or minz > other.zmax:
                        return 0
                    else:
                        return 2 # some interior of geom2 is in geom1
    
class Polygon3D(object):
    def __init__(self, wkt, zmin, zmax):
        self.wkt = wkt
        self.zmin = zmin
        self.zmax = zmax
        
    def volume(self):
        return self.wkt.area * (self.zmax - self.zmin)
        
    def relationship(self, other):
        """ self = geom1
            other = geom2
            0 if they are disjoint
            1 if geom2 completely in geom1""" 
        if isinstance(other, Cube):
            return other.relationship(self)

class Sphere(object):
    def __init__(self, pt_ctr, radius):
        assert isinstance(pt_ctr, Point3D)
        self.ctr = pt_ctr
        self.radius = radius
        
    def volume(self): 
        return (4 / 3.0) * pi * self.radius**3
        
    def relationship(self, other):
        """ self = geom1
            other = geom2
            0 if they are disjoint
            1 if geom2 completely in geom1""" 
        if isinstance(other, Cube):
            return other.relationship(self)
            
    
#==============================================================================
# Dynamic geometries in 3D
#==============================================================================
            

class dynamicCube(object):
    def __init__(self, pt_ll, pt_ur):
        """Constructor. 
        Takes the lower left and upper right point defining the Cube.
        """
        assert isinstance(pt_ll, dynamicPoint)
        assert isinstance(pt_ur, dynamicPoint)
        self.ll = pt_ll
        self.ur = pt_ur

    def volume(self):
        return (self.ur.t - self.ll.t)*(self.ur.x - self.ll.x)*(self.ur.y - self.ll.y)

    def area(self):
        return (self.ur.x - self.ll.x)*(self.ur.y - self.ll.y)
    
    def wkt2D(self):
        return box(self.ll.x, self.ll.y, self.ur.x, self.ur.y)
        
    def  relationship(self, other):
        if isinstance(other, dynamicCube):
            if self.ll.x <= other.ll.x and self.ur.x >= other.ur.x and self.ll.y <= other.ll.y and self.ur.y >= other.ur.y:
                if self.ll.t <= other.ll.t and self.ur.t >= other.ur.t:
                    return 1
                elif self.ll.t >= other.ur.t or self.ur.t <= other.ll.t:
                    return 0
                else:
                    return 2
            elif self.ll.x >= other.ur.x or self.ll.y >= other.ur.y or self.ll.z >= other.ur.z or self.ur.x <= other.ll.x or self.ur.y <= other.ll.y or self.ur.z <= other.ll.z:            
                return 0
            else:
                return 2
            
        elif isinstance(other, dynamicPolygon):
            """Uses the 2D DE9IM topological relationships in order to check the x and y dimensions.
            It is assumed that the time dimension will be given as upper - lower boundary."""
            geom1 = self.wkt2D()
            relation = geom1.relate(other.wkt)
            if other.tmin >= self.ll.t and other.tmax <= self.ur.t: #self contains other in time
                if not intersects.matches(relation):
                    #self and other are disjoint in space
                    return 0 # they are disjoint
                elif contains.matches(relation):
                    #self contains other in 2d space
                    return 1
                else:#self contains other in time and some overlap in 2d space
                    return 2
            elif other.tmax <= self.ll.t or other.tmin >= self.ur.t:
                #self and other are disjoint is time
                return 0 
            else: #there is some overlap in time
                if not intersects.matches(relation):
                    return 0 #self and other are disjoint in 2d space
                else: #some overlap in 2d space
                    return 2


class dynamicPolygon(object):
    def __init__(self, wkt, tmin, tmax):
        self.wkt = wkt
        self.tmin = tmin
        self.tmax = tmax
        
    def relationship(self, other):
        """ self = geom1
            other = geom2
            0 if they are disjoint
            1 if geom2 completely in geom1""" 
        if isinstance(other, dynamicCube):
            return other.relationship(self)
        
    def volume(self):
#        if self.tmax - self.tmin == 0:
#            return self.wkt.area
        return self.wkt.area * (self.tmax - self.tmin)
    
    def area(self):
        return self.wkt.area
        
        
#==============================================================================
# 4D geometries        
#==============================================================================
class Tesseract(object):
    def __init__(self, pt_ll, pt_ur):
        """Constructor. 
        Takes the lower left and upper right point defining the Cube.
        """
        assert isinstance(pt_ll, Point4D)
        assert isinstance(pt_ur, Point4D)
        self.ll = pt_ll
        self.ur = pt_ur

    def width(self):
        return self.ur.x - self.ll.x

    def volume(self):
        return (self.ur.x - self.ll.x)*(self.ur.y - self.ll.y)*(self.ur.z - self.ll.z)*(self.ur.t - self.ll.t)
    
    def volume3D(self):
        return (self.ur.x - self.ll.x)*(self.ur.y - self.ll.y)*(self.ur.z - self.ll.z)
        
    def wkt2D(self):
        return box(self.ll.x, self.ll.y, self.ur.x, self.ur.y)
        
    def  relationship(self, other):
        if isinstance(other, Tesseract):
            if other.ll.t >= self.ll.t and other.ur.t <= self.ur.t: #self contains other in time
                if other.ll.x >= self.ll.x and other.ur.x <= self.ur.x and other.ll.y >= self.ll.y and other.ur.y <= self.ur.y:
                    #self contains other in 2d space
                    if other.ll.z >= self.ll.z and other.ur.z <= self.ur.z: #self contains other in height
                        return 1
                    elif other.ur.z <= self.ll.z or other.ll.z >= self.ur.z: #self contains other in 2d space and time but not height
                        return 0
                    else: #self contains other in 2d space and time and some overlap in height
                        return 2
                elif other.ur.x <= self.ll.x or other.ll.x >= self.ur.x or other.ur.y <= self.ll.y or other.ll.y >= self.ur.y:
                    #self and other are disjoint in space
                    return 0
                else: #self contains other in time and some overlap in 2d space
                    if other.ur.z <= self.ll.z or other.ll.z >= self.ur.z: #self and other disjoint in height
                        return 0
                    else: #some overlap in height
                        return 2
            elif other.ur.t <= self.ll.t or other.ll.t >= self.ur.t:
                #self and other are disjoint is time
                return 0            
            else: #there is some overlap in time
                if other.ur.x <= self.ll.x or other.ll.x >= self.ur.x or other.ur.y <= self.ll.y or other.ll.y >= self.ur.y:
                    #self and other are disjoint in 2d space
                    return 0
                else: #some overlap in 2d space
                    if other.ur.z <= self.ll.z or other.ll.z >= self.ur.z: #self and other disjoint in height
                        return 0
                    else: #some overlap in height
                        return 2
                
        elif isinstance(other, Polygon4D):
            """Uses the 2D DE9IM topological relationships in order to check the x and y dimensions.
            It is assumed that the z and time dimension will be given as upper - lower boundary."""
            geom1 = self.wkt2D()
            relation = geom1.relate(other.wkt)
            if other.tmin >= self.ll.t and other.tmax <= self.ur.t: #self contains other in time
                if not intersects.matches(relation):
                    #self and other are disjoint in space
                    return 0 # they are disjoint
                elif contains.matches(relation):
                    #self contains other in 2d space
                    if other.zmin >= self.ll.z and other.zmax <= self.ur.z: #self contains other in height
                        return 1
                    elif other.zmax <= self.ll.z or other.zmin >= self.ur.z: #self contains other in 2d space and time but not height
                        return 0
                    else: #self contains other in 2d space and time and some overlap in height
                        return 2
                else:#self contains other in time and some overlap in 2d space
                    if other.zmax <= self.ll.z or other.zmin >= self.ur.z: #self contains other in 2d space and time but not height
                        return 0
                    else: #some overlap in height
                        return 2
            elif other.tmax <= self.ll.t or other.tmin >= self.ur.t:
                #self and other are disjoint is time
                return 0 
            else: #there is some overlap in time
                if not intersects.matches(relation):
                    return 0 #self and other are disjoint in 2d space
                else: #some overlap in 2d space
                    if other.zmax <= self.ll.z or other.zmin >= self.ur.z: #self contains other in 2d space and time but not height
                        return 0
                    else: #some overlap in height
                        return 2

            
class Polygon4D(object):
    def __init__(self, wkt, zmin, zmax, tmin, tmax):
        self.wkt = wkt
        self.zmin = zmin
        self.zmax = zmax
        self.tmin = tmin
        self.tmax = tmax
        
    def relationship(self, other):
        """ self = geom1
            other = geom2
            0 if they are disjoint
            1 if geom2 completely in geom1""" 
        if isinstance(other, Tesseract):
            return other.relationship(self)
        
    def volume(self):
        if self.tmax - self.tmin == 0:
            return self.wkt.area * (self.zmax - self.zmin)
        return self.wkt.area * (self.zmax - self.zmin) * (self.tmax - self.tmin)     
