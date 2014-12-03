import pickle
import sys, struct, os, random, math, pickle, string
from threading import Thread, Lock, Condition, Semaphore

from Disk import *
from Constants import FILENAMELEN, DELETEDFILE
from FileDescriptor import FileDescriptor
from FSE import FileSystemException

class DirectoryDescriptor(FileDescriptor):
    def __init__(self, inodenumber):
        super(DirectoryDescriptor, self).__init__(inodenumber)
        inodeobject = self._getinode()
        if not inodeobject.isDirectory:
            raise FileSystemException("Not a directory - inode %d" % inodenumber)

    def enumerate(self):
        length = self.getlength()
        numentries = length / (FILENAMELEN + 4)  # a directory entry is a filename and an integer for the inode number
        print ("numentries is %d", numentries)
        for i in range(0, numentries):
            data = self.read(FILENAMELEN + 4)
            name, inode = struct.unpack("%dsI" % (FILENAMELEN,), data[0:(FILENAMELEN + 4)])
            name = name.strip('\x00')
            print 'DAH DATAS NAME IS %s' %  data
            if data != DELETEDFILE:      #If the file hasn't been deleted before
                print('not a deleted file!!')
                yield name, inode

    def delete_entry(self, filename):
        length = self.getlength()
        numentries = length / (FILENAMELEN + 4)  # a directory entry is a filename and an integer for the inode number
        print ("numentries is %d", numentries)
        for i in range(0, numentries):
            data = self.read(FILENAMELEN + 4)
            print ('data is %s' % data)
            name, inode = struct.unpack("%dsI" % (FILENAMELEN,), data[0:(FILENAMELEN + 4)])
            name = name.strip('\x00')
            if name == filename:
                #delete the entry!!
                self._writetoposition(DELETEDFILE, self.position - len(data))
                print ('data %s has been deleted ' % data)
            print ('remaining is %s' % self._readfromposition(1000000, 0))
