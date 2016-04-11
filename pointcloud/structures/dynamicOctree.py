# -*- coding: utf-8 -*-
"""
Created on Fri Feb 19 16:37:13 2016

@author: Stella Psomadaki
"""
import pointcloud.morton as morton
import numpy
import math
from pointcloud.structures.geometry import dynamicPoint, dynamicCube

MAX_NUMBITS = 28

class dynamicOctree:
    def __init__(self, domain, numLevels, numBits):  
        if min(domain) < 0:
            raise Exception('ERROR: Domain must contain only positive X, Y and t numbers!')
        self.numBits = numBits
        
        if numLevels != 'auto' and numLevels > 0:
            if numLevels > self.numBits:
                raise Exception('ERROR: quadTree numLevels must be lower or equal to the number of bits of X, Y and t')
            else:
                self.numLevels = numLevels
        else:
            self.numLevels = 'auto'   
        
        mindomain = 0
        maxdomain = 1 << self.numBits
        
        parentOctant = (mindomain, mindomain, mindomain, maxdomain, maxdomain, maxdomain)
        startLevel = 0
        self.domainRegion = dynamicCube(dynamicPoint(*parentOctant[:3]), dynamicPoint(*parentOctant[3:]))
        fits = True
        while fits:
            numCodes = len(self._overlapCodes(startLevel, 0, 0, self.domainRegion, *parentOctant)[0])
            if numCodes == 1:
                startLevel += 1
            else:
                fits = False
                startLevel -= 1
        
        if startLevel > 0:
            (self.startOctCode, self.startLevel, startFullIn, startMRange) = self._overlapCodes(startLevel, 0, 0, self.domainRegion, *parentOctant)[0][0]
            self.startOct = self.getCoords(startMRange)
        else:
            self.startLevel = 0
            self.startOctCode = 0
            self.startOct = parentOctant
            
#        print 'domain', domain
#        print 'domain numBits', self.numBits
#        print 'octree numLevels', self.numLevels
#        print 'octree startLevel', self.startLevel
#        print 'octree startOctCode', self.startOctCode
#        print 'octree startOct', self.startOct
            
    def _relation(self, geom1, geom2):
        """ Returns the relationship between two geometries. 
              0 if they are disjoint, 
              1 if geom2 is completely in geom1,
              2 if geom2 is partly in geom1"""
        return geom1.relationship(geom2)

    def _overlapCodes(self, maxDepth, parentLevel, parentCode, region, minx, miny, minz, maxx, maxy, maxz): 
        """ Recursive method that return morton ranges overlapping with the region for the specified domain"""
        cx = minx + ((maxx - minx) >> 1)
        cy = miny + ((maxy - miny) >> 1)
        cz = minz + ((maxz - minz) >> 1)
        
        # Z order octans
        octans = [
           (minx, miny, minz, cx, cy, cz), #0
           (cx, miny, minz, maxx, cy, cz), #1
           (minx, cy, minz, cx, maxy, cz), #2
           (cx, cy, minz, maxx, maxy, cz), #3
           (minx, miny, cz, cx, cy, maxz), #4
           (cx, miny, cz, maxx, cy, maxz), #5 
           (minx, cy, cz, cx, maxy, maxz), #6
           (cx, cy, cz, maxx, maxy, maxz)  #7
        ]
        
                
        level = parentLevel + 1
          
        codes = []
        c = 0
        for octIndex in range(8):
            octan = octans[octIndex]
            relation = self._relation(region, dynamicCube(dynamicPoint(*octan[:3]), dynamicPoint(*octan[3:])))
            
            if relation: #1 or 2
                octCode = (parentCode << 3) + octIndex
                if parentLevel == maxDepth:
                        codes.append((octCode, level, relation == 1, self.octCodeToMortonRange(octCode, level))) # relation = 1 indicates that this morton range is fully within query region
                        c += 1
                else:
                    (tcodes, tc) = self._overlapCodes(maxDepth, level, octCode, region, *octan)
                    if tc == 8:
                        codes.append((octCode, level, False, self.octCodeToMortonRange(octCode, level)))
                        c += 1
                    else:
                        codes.extend(tcodes)
                        
        return (codes,c)
    
    def octCodeToMortonRange(self, octCode, level):
        diff = (self.numBits - level) * 3
        minr = octCode << diff
        maxr = ((octCode + 1) << diff) - 1
        return (minr, maxr)
        
    def overlapCodes(self, region, coarser, continuous, numLevels = None):
        if numLevels == None:
            numLevels = self.numLevels
        if (numLevels == 'auto') or (numLevels < 0):
            if continuous:
                numLevels = math.floor(math.log(self.domainRegion.volume() / region.volume(), 4)) - coarser
            else:
                numLevels = math.floor(math.log(self.domainRegion.area() / region.area(), 2)) - coarser

        if dynamicCube(dynamicPoint(*self.startOct[:3]), dynamicPoint(*self.startOct[3:])).relationship(region):
            return self._overlapCodes(numLevels, self.startLevel, self.startOctCode, region, *self.startOct)[0]
        return []
    
    def mergeConsecutiveRanges(self, mranges):
        if len(mranges) == 0:
            return []
        omranges = []
        (mrangemin, mrangemax) = mranges[0]
        for rangeIndex in range(1, len(mranges)):
            mrange = mranges[rangeIndex]
            if mrangemax == mrange[0] - 1:
                mrangemax = mrange[1]
            else:
                omranges.append((mrangemin, mrangemax))
                (mrangemin, mrangemax) = mrange
        omranges.append((mrangemin, mrangemax))
        return omranges
    
    def mergeRanges(self, mranges, maxRanges):
        numRanges = len(mranges)
        if numRanges <= maxRanges or numRanges < 2:
            return mranges 
        numRangesToMerge = numRanges - maxRanges
        b = numpy.array(numpy.array(mranges).flat)
        diffs = b[::2][1:] - b[1::2][:-1]
        tDiff = sorted(diffs)[numRangesToMerge-1]
        lowerDiffs = len(diffs[diffs < tDiff])
        equalToMerge = numRangesToMerge - lowerDiffs
        equalCounter = 0
        omranges = []
        mrangemin = None    
        for rangeIndex in range(numRanges):
            if mrangemin == None:
                mrangemin = mranges[rangeIndex][0]
            if rangeIndex < numRanges-1:
                if diffs[rangeIndex] > tDiff:
                    omranges.append((mrangemin, mranges[rangeIndex][1]))
                    mrangemin = None
                elif diffs[rangeIndex] == tDiff:
                    equalCounter += 1
                    if equalCounter > equalToMerge:
                        omranges.append((mrangemin, mranges[rangeIndex][1]))
                        mrangemin = None
            else:
                omranges.append((mrangemin, mranges[rangeIndex][1]))
        return omranges
            
    def getAllRanges(self, codes):
        mranges = []
        for code in codes:
            mranges.append(code[-1])
        return mranges
    
    def getDiffRanges(self, codes):
        imranges = []
        omranges = []
        for code in codes:
            if code[2]:
                imranges.append(code[-1])
            else:
                omranges.append(code[-1])
        return (imranges, omranges)
    
    def getCoords(self, mortonRange):
        (minr, maxr) = mortonRange
        minx = morton.DecodeMorton3DX(minr)
        miny = morton.DecodeMorton3DY(minr)
        minz = morton.DecodeMorton3DZ(minr)
        maxx = morton.DecodeMorton3DX(maxr)
        maxy = morton.DecodeMorton3DY(maxr)
        maxz = morton.DecodeMorton3DZ(maxr)
        return (minx, miny, minz, maxx + 1, maxy + 1, maxz + 1)
    
    def getMortonRanges(self, region, coarser = 3, continuous = True, distinctIn = False, numLevels = None, maxRanges = None):
        codes = self.overlapCodes(region, coarser, continuous)

        if distinctIn:
            mmranges = self.getAllRanges(codes)
            mxmranges = self.mergeConsecutiveRanges(mmranges)
            return (mxmranges, len(codes))
        else:
            mmranges = self.mergeConsecutiveRanges(self.getAllRanges(codes))
            if maxRanges != None:
                maxmranges = self.mergeRanges(mmranges, maxRanges)
                return ([], maxmranges, len(codes))
            else:
                return ([], mmranges, len(codes))
