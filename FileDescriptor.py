import sys, struct
import Segment

from threading import Thread, Lock, Condition, Semaphore
from Inode import Inode
import InodeMap
from FSE import FileSystemException

class FileDescriptor(object):
    def __init__(self, inodenumber):
        object.__init__(self)
        self.inodenumber = inodenumber
        self.position = 0
        self.isopen = True

    def close(self):
        if not self.isopen:
            raise FileSystemException("The File is Already Closed!")
        self.isopen = False

    def _getinode(self):
        # find the inode's position on disk
        inodeblocknumber = InodeMap.inodemap.lookup(self.inodenumber)
        # get the inode
        inodeobject = Inode(str=Segment.segmentmanager.blockread(inodeblocknumber))
        return inodeobject

    def getlength(self):
        inodeobject = self._getinode()
        return inodeobject.filesize

    def read(self, readlength):
        inodeobject = self._getinode()
        data = inodeobject.read(self.position, readlength)
        self.position += len(data)
        return data

    def write(self, data):
        # XXXDONE - do this tomorrow! after the meteor shower!
        inodeobject = self._getinode()
        inodeobject.write(self.position, data, False)
        self.position += len(data)

    def _writetoposition(self, data, pos):
        inodeobject = self._getinode()
        inodeobject.write(pos, data, False)

    def _readfromposition(self, readlength, pos):
        inodeobject = self._getinode()
        data = inodeobject.read(pos, readlength)
        return data

    def _markstale(self):
        inodeblocknumber = InodeMap.inodemap.lookup(self.inodenumber)
        inodeobject = Inode(str=Segment.segmentmanager.blockread(inodeblocknumber))
        inodeobject.recyclewholeinode()#DELETE ITSELF
