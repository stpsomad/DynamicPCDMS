################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
#                                                                              #
#    Extended by: Stella Psomadaki using  code found in                        #
#    Stackoverflow: http://bit.ly/1YrXQdj                                      #
################################################################################
from numba import jit, int32, int64

"""Divide and conquer approach for separating the bits"""

###############################################################################
######################      Morton conversion in 2D      ######################
###############################################################################

@jit(int64(int32))
def Expand2D(n):
    """Encoding the 64 bit morton code for two 31 bit numbers. 1 bit is not used
    because the integers are not unsigned"""
    b = n & 0x7fffffff                         
    b = (b ^ (b <<  16)) & 0x0000ffff0000ffff 
    b = (b ^ (b <<  8))  & 0x00ff00ff00ff00ff 
    b = (b ^ (b <<  4))  & 0x0f0f0f0f0f0f0f0f
    b = (b ^ (b <<  2))  & 0x3333333333333333
    b = (b ^ (b <<  1))  & 0x5555555555555555
    return b

@jit(int64(int32, int32))
def EncodeMorton2D(x, y):
    return Expand2D(x) + (Expand2D(y) << 1)

@jit(int32(int64))
def Compact2D(m):
    m &= 0x5555555555555555
    m = (m ^ (m >> 1))  & 0x3333333333333333
    m = (m ^ (m >> 2))  & 0x0f0f0f0f0f0f0f0f
    m = (m ^ (m >> 4))  & 0x00ff00ff00ff00ff
    m = (m ^ (m >> 8))  & 0x0000ffff0000ffff
    m = (m ^ (m >> 16)) & 0x00000000ffffffff # return a 32 bit integer
    return m

@jit(int32(int64))
def DecodeMorton2DX(mortonCode):
    return Compact2D(mortonCode)

@jit(int32(int64))
def DecodeMorton2DY(mortonCode):
    return Compact2D(mortonCode >> 1)
    
###############################################################################
######################      Morton conversion in 3D      ######################
######################       21 bits per dimension       ######################
###############################################################################

@jit(int64(int32))
def Expand3D_21bit(x):
    """This code is appropriate for 21 bit numbers and gives a 64 bit morton
    code. 21 bit means x,y,z must be up to 2097151. In this case, it is not
    enough.
    
    Source: Stackoverflow: http://bit.ly/1YrXQdj by user Gabriel"""
    x = (x ^ (x << 32)) & 0x7fff00000000ffff 
    x = (x ^ (x << 16)) & 0x00ff0000ff0000ff
    x = (x ^ (x <<  8)) & 0x700f00f00f00f00f
    x = (x ^ (x <<  4)) & 0x30c30c30c30c30c3
    x = (x ^ (x <<  2)) & 0x1249249249249249
    return x

@jit(int32(int64))    
def Compact3D_21bit(x):
    """Decoding the 3D Morton made from the 21 bit numbers.
    Not used here because it is not enough."""
    x &= 0x1249249249249249
    x = (x ^ (x >> 2)) & 0x30c30c30c30c30c3
    x = (x ^ (x >> 4)) & 0x700f00f00f00f00f
    x = (x ^ (x >> 8)) & 0x00ff0000ff0000ff
    x = (x ^ (x >> 16)) & 0x7fff00000000ffff
    x = (x ^ (x >> 32)) & 0x00000000ffffffff
    return x

@jit
def EncodeMorton3D_21bit(x, y, z):
    return Expand3D_21bit(x) + (Expand3D_21bit(y) << 1) + (Expand3D_21bit(z) << 2)

@jit  
def DecodeMorton3DX_21bit(mortonCode):
    return Compact3D_21bit(mortonCode)

@jit  
def DecodeMorton3DY_21bit(mortonCode):
    return Compact3D_21bit(mortonCode >> 1)

@jit 
def DecodeMorton3DZ_21bit(mortonCode):
    return Compact3D_21bit(mortonCode >> 2)

###############################################################################
######################      Morton conversion in 3D      ######################
######################       31 bits per dimension       ######################
###############################################################################

@jit
def Expand3D(x):
    """This code is appropriate for 31 bit numbers and gives a 93 bit morton
    code. 
    
    Source: Stackoverflow: http://bit.ly/1YrXQdj by user Gabriel"""
    x &= 0x7fffffffL
    x = (x ^ x << 32) & 0x7fff00000000ffffL
    x = (x ^ x << 16) & 0x7f0000ff0000ff0000ffL
    x = (x ^ x << 8) & 0x700f00f00f00f00f00f00fL
    x = (x ^ x << 4) & 0x430c30c30c30c30c30c30c3L
    x = (x ^ x << 2) & 0x49249249249249249249249L
    return x

def EncodeMorton3D(x, y, z):
    return Expand3D(x) + (Expand3D(y) << 1) + (Expand3D(z) << 2)

def Compact3D(x):
    x &= 0x49249249249249249249249L
    x = (x ^ (x >> 2)) & 0x430c30c30c30c30c30c30c3L
    x = (x ^ (x >> 4)) & 0x700f00f00f00f00f00f00fL
    x = (x ^ (x >> 8)) & 0x7f0000ff0000ff0000ffL
    x = (x ^ (x >> 16)) & 0x7fff00000000ffffL
    x = (x ^ (x >> 32)) & 0x7fffffffL
    return x

def DecodeMorton3DX(mortonCode):
    return Compact3D(mortonCode)

def DecodeMorton3DY(mortonCode):
    return Compact3D(mortonCode >> 1)

def DecodeMorton3DZ(mortonCode):
    return Compact3D(mortonCode >> 2)
    
###############################################################################
######################      Morton conversion in 4D      ######################
###############################################################################

@jit
def Expand4D(x):  
    """This code  is appropriate for 31bit numbers and gives 124bit morton code
    in 4D. 
    
    Source: Stackoverflow: http://bit.ly/1YrXQdj by user Gabriel"""
    x &= 0x7fffffffL
    x = (x ^ x << 64) & 0x7fc0000000000000003fffffL
    x = (x ^ x << 32) & 0x7fc00000003ff800000007ffL
    x = (x ^ x << 16) & 0x780007c0003f0000f80007c0003fL
    x = (x ^ x << 8) & 0x40380700c0380700c0380700c03807L
    x = (x ^ x << 4) & 0x430843084308430843084308430843L
    x = (x ^ x << 2) & 0x1090909090909090909090909090909L
    x = (x ^ x << 1) & 0x1111111111111111111111111111111L
    return x

def EncodeMorton4D(x, y, z, t):
    return Expand4D(x) + (Expand4D(y) << 1) + (Expand4D(z) << 2) + (Expand4D(t) << 3)

def Compact4D(x):
    x &= 0x1111111111111111111111111111111L
    x = (x ^ (x >> 1)) & 0x1090909090909090909090909090909L
    x = (x ^ (x >> 2)) & 0x430843084308430843084308430843L
    x = (x ^ (x >> 4)) & 0x40380700c0380700c0380700c03807L
    x = (x ^ (x >> 8)) & 0x780007c0003f0000f80007c0003fL
    x = (x ^ (x >> 16)) & 0x7fc00000003ff800000007ffL
    x = (x ^ (x >> 32)) & 0x7fc0000000000000003fffffL
    x = (x ^ (x >> 64)) & 0x7fffffffL
    return x

def DecodeMorton4Dt(mortonCode):
    return Compact4D(mortonCode)

def DecodeMorton4DX(mortonCode):
    return Compact4D(mortonCode >> 1)

def DecodeMorton4DY(mortonCode):
    return Compact4D(mortonCode >> 2)

def DecodeMorton4DZ(mortonCode):
    return Compact4D(mortonCode >> 3)
