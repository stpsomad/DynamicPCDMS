################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
#   Apache License                                                             #
#   Version 2.0, January 2004                                                  #
#                                                                              #
#    Extended by: Stella Psomadaki                                             #
#    Source: Stackoverflow http://bit.ly/1YrXQdj                               #
################################################################################
from numba import jit, int32, int64

###############################################################################
######################      Morton conversion in 2D      ######################
###############################################################################

@jit(int64(int32))
def Expand2D(n):
    """
    Encodes the 64 bit morton code for a 31 bit number in the 2D space using
    a divide and conquer approach for separating the bits. 
    1 bit is not used because the integers are not unsigned
    
    Args:
        n (int): a 2D dimension
        
    Returns:
        int: 64 bit morton code in 2D
        
    Raises:
        Exception: ERROR: Morton code is valid only for positive numbers
    """
    if n < 0:
        raise Exception("""ERROR: Morton code is valid only for positive numbers""")
    
    b = n & 0x7fffffff                         
    b = (b ^ (b <<  16)) & 0x0000ffff0000ffff 
    b = (b ^ (b <<  8))  & 0x00ff00ff00ff00ff 
    b = (b ^ (b <<  4))  & 0x0f0f0f0f0f0f0f0f
    b = (b ^ (b <<  2))  & 0x3333333333333333
    b = (b ^ (b <<  1))  & 0x5555555555555555
    return b

@jit(int64(int32, int32))
def EncodeMorton2D(x, y):
    """
    Calculates the 2D morton code from the x, y dimensions
    
    Args:
        x (int): the x dimension
        y (int): the y dimension
        
    Returns:
        int: 64 bit morton code in 2D

    """
    return Expand2D(x) + (Expand2D(y) << 1)

@jit(int32(int64))
def Compact2D(m):
    """
    Decodes the 64 bit morton code into a 32 bit number in the 2D space using
    a divide and conquer approach for separating the bits. 
    1 bit is not used because the integers are not unsigned
    
    Args:
        n (int): a 64 bit morton code
        
    Returns:
        int: a dimension in 2D space
        
    Raises:
        Exception: ERROR: Morton code is always positive
    """
    if m < 0:
        raise Exception("""ERROR: Morton code is always positive""")
    m &= 0x5555555555555555
    m = (m ^ (m >> 1))  & 0x3333333333333333
    m = (m ^ (m >> 2))  & 0x0f0f0f0f0f0f0f0f
    m = (m ^ (m >> 4))  & 0x00ff00ff00ff00ff
    m = (m ^ (m >> 8))  & 0x0000ffff0000ffff
    m = (m ^ (m >> 16)) & 0x00000000ffffffff
    return m

@jit(int32(int64))
def DecodeMorton2DX(mortonCode):
    """
    Calculates the x coordinate from a 64 bit morton code
    
    Args:
        mortonCode (int): the 64 bit morton code
        
    Returns:
        int: 32 bit x coordinate in 2D

    """
    return Compact2D(mortonCode)

@jit(int32(int64))
def DecodeMorton2DY(mortonCode):
    """
    Calculates the y coordinate from a 64 bit morton code
    
    Args:
        mortonCode (int): the 64 bit morton code
        
    Returns:
        int: 32 bit y coordinate in 2D

    """
    return Compact2D(mortonCode >> 1)
    
###############################################################################
######################      Morton conversion in 3D      ######################
######################       21 bits per dimension       ######################
###############################################################################

@jit(int64(int32))
def Expand3D_21bit(x):
    """
    Encodes the 64 bit morton code for a 21 bit number in the 3D space using
    a divide and conquer approach for separating the bits. 
    
    Args:
        x (int): the requested 3D dimension
        
    Returns:
        int: 64 bit morton code in 3D
        
    Raises:
        Exception: ERROR: Morton code is valid only for positive numbers
        
    """
    
    if x < 0:
        raise Exception("""ERROR: Morton code is valid only for positive numbers""")
    x = (x ^ (x << 32)) & 0x7fff00000000ffff 
    x = (x ^ (x << 16)) & 0x00ff0000ff0000ff
    x = (x ^ (x <<  8)) & 0x700f00f00f00f00f
    x = (x ^ (x <<  4)) & 0x30c30c30c30c30c3
    x = (x ^ (x <<  2)) & 0x1249249249249249
    return x

@jit(int32(int64))    
def Compact3D_21bit(x):
    """
    Decodes the 64 bit morton code into a 21 bit number in the 3D space  using
    a divide and conquer approach for separating the bits. 
    
    Args:
        x (int): a 64 bit morton code
        
    Returns:
        int: a dimension in 3D space
        
    Raises:
        Exception: ERROR: Morton code is always positive
    """

    if x < 0:
        raise Exception("""ERROR: Morton code is always positive""")
    x &= 0x1249249249249249
    x = (x ^ (x >> 2)) & 0x30c30c30c30c30c3
    x = (x ^ (x >> 4)) & 0x700f00f00f00f00f
    x = (x ^ (x >> 8)) & 0x00ff0000ff0000ff
    x = (x ^ (x >> 16)) & 0x7fff00000000ffff
    x = (x ^ (x >> 32)) & 0x00000000ffffffff
    return x

@jit
def EncodeMorton3D_21bit(x, y, z):
    """
    Calculates the 3D morton code from the x, y, z dimensions
    
    Args:
        x (int): the x dimension
        y (int): the y dimension
        z (int): the z dimension
        
    Returns:
        int: 64 bit morton code in 3D

    """
    return Expand3D_21bit(x) + (Expand3D_21bit(y) << 1) + (Expand3D_21bit(z) << 2)

@jit  
def DecodeMorton3DX_21bit(mortonCode):
    """
    Calculates the x coordinate from a 64 bit morton code
    
    Args:
        mortonCode (int): the 64 bit morton code
        
    Returns:
        int: 21 bit x coordinate in 3D

    """
    return Compact3D_21bit(mortonCode)

@jit  
def DecodeMorton3DY_21bit(mortonCode):
    """
    Calculates the y coordinate from a 64 bit morton code
    
    Args:
        mortonCode (int): the 64 bit morton code
        
    Returns:
        int: 21 bit y coordinate in 3D

    """
    return Compact3D_21bit(mortonCode >> 1)

@jit 
def DecodeMorton3DZ_21bit(mortonCode):
    """
    Calculates the z coordinate from a 64 bit morton code
    
    Args:
        mortonCode (int): the 64 bit morton code
        
    Returns:
        int: 21 bit z coordinate in 3D

    """
    return Compact3D_21bit(mortonCode >> 2)

###############################################################################
######################      Morton conversion in 3D      ######################
######################       31 bits per dimension       ######################
###############################################################################

@jit
def Expand3D(x):
    """
    Encodes the 93 bit morton code for a 31 bit number in the 3D space using
    a divide and conquer approach for separating the bits. 

    
    Args:
        x (int): the requested 3D dimension
        
    Returns:
        int: 93 bit morton code in 3D
        
    Raises:
        Exception: ERROR: Morton code is valid only for positive numbers
        
    """
    
    if x < 0:
        raise Exception("""ERROR: Morton code is valid only for positive numbers""")
    x &= 0x7fffffffL
    x = (x ^ x << 32) & 0x7fff00000000ffffL
    x = (x ^ x << 16) & 0x7f0000ff0000ff0000ffL
    x = (x ^ x << 8) & 0x700f00f00f00f00f00f00fL
    x = (x ^ x << 4) & 0x430c30c30c30c30c30c30c3L
    x = (x ^ x << 2) & 0x49249249249249249249249L
    return x

def EncodeMorton3D(x, y, z):
    """
    Calculates the 3D morton code from the x, y, z dimensions
    
    Args:
        x (int): the x dimension of 31 bits
        y (int): the y dimension of 31 bits
        z (int): the z dimension of 31 bits
        
    Returns:
        int: 93 bit morton code in 3D

    """
    return Expand3D(x) + (Expand3D(y) << 1) + (Expand3D(z) << 2)

def Compact3D(x):
    """
    Decodes the 93 bit morton code into a 31 bit number in the 3D space using
    a divide and conquer approach for separating the bits. 
    
    Args:
        x (int): a 93 bit morton code
        
    Returns:
        int: a dimension in 3D space
        
    Raises:
        Exception: ERROR: Morton code is always positive
    """

    if x < 0:
        raise Exception("""ERROR: Morton code is always positive""")
    
    x &= 0x49249249249249249249249L
    x = (x ^ (x >> 2)) & 0x430c30c30c30c30c30c30c3L
    x = (x ^ (x >> 4)) & 0x700f00f00f00f00f00f00fL
    x = (x ^ (x >> 8)) & 0x7f0000ff0000ff0000ffL
    x = (x ^ (x >> 16)) & 0x7fff00000000ffffL
    x = (x ^ (x >> 32)) & 0x7fffffffL
    return x

def DecodeMorton3DX(mortonCode):
    """
    Calculates the x coordinate from a 93 bit morton code
    
    Args:
        mortonCode (int): the 93 bit morton code
        
    Returns:
        int: 31 bit x coordinate in 3D

    """
    return Compact3D(mortonCode)

def DecodeMorton3DY(mortonCode):
    """
    Calculates the y coordinate from a 93 bit morton code
    
    Args:
        mortonCode (int): the 93 bit morton code
        
    Returns:
        int: 31 bit y coordinate in 3D

    """
    return Compact3D(mortonCode >> 1)

def DecodeMorton3DZ(mortonCode):
    """
    Calculates the z coordinate from a 93 bit morton code
    
    Args:
        mortonCode (int): the 93 bit morton code
        
    Returns:
        int: 31 bit z coordinate in 3D

    """
    return Compact3D(mortonCode >> 2)
    
###############################################################################
######################      Morton conversion in 4D      ######################
###############################################################################

@jit
def Expand4D(x):
    """
    Encodes the 124 bit morton code for a 31 bit number in the 4D space using
    a divide and conquer approach for separating the bits. 

    
    Args:
        x (int): the requested 3D dimension
        
    Returns:
        int: 124 bit morton code in 3D
        
    Raises:
        Exception: ERROR: Morton code is valid only for positive numbers
        
    """
    if x < 0:
        raise Exception("""ERROR: Morton code is valid only for positive numbers""")
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
    """
    Calculates the 4D morton code from the x, y, z, t dimensions
    
    Args:
        x (int): the x dimension of 31 bits
        y (int): the y dimension of 31 bits
        z (int): the z dimension of 31 bits
        t (int): the time dimension of 31 bits
        
    Returns:
        int: 124 bit morton code in 4D

    """
    return Expand4D(x) + (Expand4D(y) << 1) + (Expand4D(z) << 2) + (Expand4D(t) << 3)

def Compact4D(x):
    """
    Decodes the 124 bit morton code into a 31 bit number in the 4D space using
    a divide and conquer approach for separating the bits. 
    
    Args:
        x (int): a 124 bit morton code
        
    Returns:
        int: a dimension in 4D space
        
    Raises:
        Exception: ERROR: Morton code is always positive
    """
    if x < 0:
        raise Exception("""ERROR: Morton code is always positive""")
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
    """
    Calculates the t coordinate from a 124 bit morton code
    
    Args:
        mortonCode (int): the 124 bit morton code
        
    Returns:
        int: 31 bit t coordinate in 4D

    """
    return Compact4D(mortonCode)

def DecodeMorton4DX(mortonCode):
    """
    Calculates the x coordinate from a 124 bit morton code
    
    Args:
        mortonCode (int): the 124 bit morton code
        
    Returns:
        int: 31 bit x coordinate in 4D

    """
    return Compact4D(mortonCode >> 1)

def DecodeMorton4DY(mortonCode):
    """
    Calculates the y coordinate from a 124 bit morton code
    
    Args:
        mortonCode (int): the 124 bit morton code
        
    Returns:
        int: 31 bit y coordinate in 4D

    """
    return Compact4D(mortonCode >> 2)

def DecodeMorton4DZ(mortonCode):
    """
    Calculates the z coordinate from a 124 bit morton code
    
    Args:
        mortonCode (int): the 124 bit morton code
        
    Returns:
        int: 31 bit z coordinate in 4D

    """
    return Compact4D(mortonCode >> 3)
