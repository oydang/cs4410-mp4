FILENAMELEN = 28
BLOCKSIZE = 1024 # in bytes
DISKSIZE  = 1024 * BLOCKSIZE  # a 1MB disk
SEGMENTSIZE = 256  # blocks
NUMSEGMENTS = DISKSIZE / (BLOCKSIZE * SEGMENTSIZE)
