# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 20:07:05 2016

@author: Stella Psomadaki
"""
import pointcloud.morton as morton
import numpy
import math
from pointcloud.structures.geometry import Tesseract, Point4D

class HexadecTree:
    def __init__(self, domain, numLevels, numBits):
        if min(domain) < 0:
            raise Exception('ERROR: Domain must contain only positive X and Y numbers!')
        self.numBits = numBits
       
        if numLevels != 'auto' and numLevels > 0:
            if numLevels > self.numBits:
                raise Exception('ERROR: quadTree numLevels must be lower or equal to the number of bits of X and Y')
            else:
                self.numLevels = numLevels
        else:
            self.numLevels = 'auto'   
        
        mindomain = 0
        maxdomain = 1 << self.numBits
        
        parentHexadectant = (mindomain, mindomain, mindomain, mindomain, maxdomain, maxdomain, maxdomain, maxdomain)
        startLevel = 0
        self.domainRegion = Tesseract(Point4D(*parentHexadectant[:4]), Point4D(*parentHexadectant[4:]))
        fits = True
        while fits:
            numCodes = len(self._overlapCodes(startLevel, 0, 0, self.domainRegion, *parentHexadectant)[0])
            if numCodes == 1:
                startLevel += 1
            else:
                fits = False
                startLevel -= 1
        
        if startLevel > 0:
            (self.startHexadecCode, self.startLevel, startFullIn, startMRange) = self._overlapCodes(startLevel, 0, 0, self.domainRegion, *parentHexadectant)[0][0]
            self.startHexadec = self.getCoords(startMRange)
        else:
            self.startLevel = 0
            self.startHexadecCode = 0
            self.startHexadec = parentHexadectant
            

    def _relation(self, geom1, geom2):
        """ Returns the relationship between two geometries. 
              0 if they are disjoint, 
              1 if geom2 is completely in geom1,
              2 if geom2 is partly in geom1"""
        return geom1.relationship(geom2)

    def _overlapCodes(self, maxDepth, parentLevel, parentCode, region, mint, minx, miny, minz, maxt, maxx, maxy, maxz): 
        """ Recursive method that return morton ranges overlapping with the region for the specified domain"""
        ct = mint + ((maxt - mint) / 2)        
        cx = minx + ((maxx - minx) / 2)
        cy = miny + ((maxy - miny) / 2)
        cz = minz + ((maxz - minz) / 2)
        
         # Z order hexadectans        
        hexadectans = [
           (mint, minx, miny, minz, ct, cx, cy, cz),   #0
           (ct, minx, miny, minz, maxt, cx, cy, cz),   #1
           (mint, cx, miny, minz, ct, maxx, cy, cz),   #2
           (ct, cx, miny, minz, maxt, maxx, cy, cz),   #3
           (mint, minx, cy, minz, ct, cx, maxy, cz),   #4
           (ct, minx, cy, minz, maxt, cx, maxy, cz),   #5
           (mint, cx, cy, minz, ct, maxx, maxy, cz),   #6
           (ct, cx, cy, minz, maxt, maxx, maxy, cz),   #7
           (mint, minx, miny, cz, ct, cx, cy, maxz),   #8
           (ct, minx, miny, cz, maxt, cx, cy, maxz),   #9
           (mint, cx, miny, cz, ct, maxx, cy, maxz),   #10
           (ct, cx, miny, cz, maxt, maxx, cy, maxz),   #11
           (mint, minx, cy, cz, ct, cx, maxy, maxz),   #12
           (ct, minx, cy, cz, maxt, cx, maxy, maxz),   #13
           (mint, cx, cy, cz, ct, maxx, maxy, maxz),   #14
           (ct, cx, cy, cz, maxt, maxx, maxy, maxz)    #15
         ]
        level = parentLevel + 1
          
        codes = []
        c = 0
        for hexadecIndex in range(16):
            hexadectan = hexadectans[hexadecIndex]

            relation = self._relation(region, Tesseract(Point4D(*hexadectan[:4]), Point4D(*hexadectan[4:])))
            if relation: #1 or 2

                hexadecCode = (parentCode << 4) + hexadecIndex
                if parentLevel == maxDepth:
                        codes.append((hexadecCode, level, relation == 1, self.quadCodeToMortonRange(hexadecCode, level))) # relation = 1 indicates that this morton range is fully within query region
                
                else:
                    (tcodes, tc) = self._overlapCodes(maxDepth, level, hexadecCode, region, *hexadectan)
                    
                    if tc == 16:
                        codes.append((hexadecCode, level, False, self.quadCodeToMortonRange(hexadecCode, level)))
                        c += 1
                    else:
                        codes.extend(tcodes)
        return (codes,c)
    
    def quadCodeToMortonRange(self, hexadecCode, level):
        diff = (self.numBits - level) * 4
        minr = hexadecCode << diff
        maxr = ((hexadecCode + 1) << diff) - 1
        return (minr, maxr)
        
    def overlapCodes(self, region, coarser, continuous, numLevels = None):
        if numLevels == None:
            numLevels = self.numLevels
        if (numLevels == 'auto') or (numLevels < 0):
            if continuous:
                numLevels = math.ceil(math.log(self.domainRegion.volume() / region.volume(), 8)) - coarser
            else:
                numLevels = math.floor(math.log(self.domainRegion.volume3D() / region.volume(), 3)) - coarser
        if Tesseract(Point4D(*self.startHexadec[:4]), Point4D(*self.startHexadec[4:])).relationship(region):
            return self._overlapCodes(numLevels, self.startLevel, self.startHexadecCode, region, *self.startHexadec)[0], numLevels
        return [], numLevels
    
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
        mint = morton.DecodeMorton4Dt(minr)
        minx = morton.DecodeMorton4DX(minr)
        miny = morton.DecodeMorton4DY(minr)        
        minz = morton.DecodeMorton4DY(minr)
        maxt = morton.DecodeMorton4Dt(maxr)
        maxx = morton.DecodeMorton4DX(maxr)
        maxy = morton.DecodeMorton4DY(maxr)
        maxz = morton.DecodeMorton4DZ(maxr)
        return (mint, minx, miny, minz, maxt + 1, maxx + 1, maxy + 1, maxz + 1)
    
    def getMortonRanges(self, region, coarser = 5, continuous = True, distinctIn = False, numLevels = None, maxRanges = None):
        codes, Levels = self.overlapCodes(region, coarser, continuous, numLevels)
        if distinctIn:
            (imranges, xmranges) = self.getDiffRanges(codes)
            mimranges = self.mergeConsecutiveRanges(imranges)
            mxmranges = self.mergeConsecutiveRanges(xmranges)
            return (mimranges, mxmranges, len(codes), Levels)
        else:
            mmranges = self.mergeConsecutiveRanges(self.getAllRanges(codes))
            if maxRanges != None:
                maxmranges = self.mergeRanges(mmranges, maxRanges)
                return ([], maxmranges, len(codes), Levels)
            else:
                return ([], mmranges, len(codes), Levels)