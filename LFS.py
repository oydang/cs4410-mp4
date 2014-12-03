#!/usr/bin/python
import sys, struct
import Segment
import InodeMap
import re

from threading import Thread, Lock, Condition, Semaphore
from Segment import SegmentManagerClass
from Disk import DiskClass
from Inode import Inode, getmaxinode, setmaxinode
from InodeMap import InodeMapClass
from FileDescriptor import FileDescriptor
from DirectoryDescriptor import DirectoryDescriptor
from Constants import FILENAMELEN
from FSE import FileSystemException
import Disk

def find_parent_name(path):
    parent, sep, element = path.rpartition("/")
    if parent == '':
        parent = '/'
    return parent

def find_filename(path):
    parent, sep, element = path.rpartition("/")
    return element

#takes an absolute path, iterates through the components in the name
def get_path_components(path):
    for component in path[1:].strip().split("/"):
        yield component

class LFSClass:
    def __init__(self, initdisk=True):
        pass

    # open an existing file or directory
    def open(self, path, isdir=False):
        inodenumber = self.searchfiledir(path)
        if inodenumber is None:                                         
            raise FileSystemException("Path Does Not Exist")
        # create and return a Descriptor of the right kind
        if isdir:
            return DirectoryDescriptor(inodenumber)
        else:
            return FileDescriptor(inodenumber)

    def create(self, filename, isdir=False):
        fileinodenumber = self.searchfiledir(filename)                                                                 
        if fileinodenumber is not None:
            raise FileSystemException("File Already Exists")

        # create an Inode for the file
        # Inode constructor writes the inode to disk and implicitly updates the inode map
        newinode = Inode(isdirectory=isdir)

        # now append the <filename, inode> entry to the parent directory
        parentdirname = find_parent_name(filename)
        parentdirinodenumber = self.searchfiledir(parentdirname)
        if parentdirinodenumber is None:
            raise FileSystemException("Parent Directory Does Not Exist")
        parentdirblockloc = InodeMap.inodemap.lookup(parentdirinodenumber)
        parentdirinode = Inode(str=Segment.segmentmanager.blockread(parentdirblockloc))
        self.append_directory_entry(parentdirinode, find_filename(filename), newinode)

        if isdir:
            return DirectoryDescriptor(newinode.id)
        else:
            return FileDescriptor(newinode.id)

    # return metadata about the given file
    def stat(self, pathname):
        inodenumber = self.searchfiledir(pathname)
        if inodenumber is None:
            raise FileSystemException("File or Directory Does Not Exist")

        inodeblocknumber = InodeMap.inodemap.lookup(inodenumber)
        inodeobject = Inode(str=Segment.segmentmanager.blockread(inodeblocknumber))
        return inodeobject.filesize, inodeobject.isDirectory

    # delete the given file
    def unlink(self, pathname):
        # XXX - do this tomorrow! after the meteor shower!
        pass

    # write all in memory data structures disk
    def sync(self):
        # XXXDONE - do this tomorrow! after the meteor shower!

        serialMap, generation = InodeMap.inodemap.save_inode_map(getmaxinode())
        iminode = Inode()
        iminode.write(0, serialMap, False) #Write inode map into special inode
        iminodeloc = InodeMap.inodemap.lookup(iminode.id)
        Segment.segmentmanager.update_inodemap_position(iminodeloc, generation)

        Segment.segmentmanager.flush()

    # restore in memory data structures (e.g. inode map) from disk
    def restore(self):
        imlocation = Segment.segmentmanager.locate_latest_inodemap()
        iminode = Inode(str=Disk.disk.blockread(imlocation))
        imdata = iminode.read(0, 10000000)
        # restore the latest inodemap from wherever it may be on disk
        setmaxinode(InodeMap.inodemap.restore_inode_map(imdata))

    # for a given file or directory named by path,
    # return its inode number if the file or directory exists,
    # else return None
    def searchfiledir(self, path):  
        # XXXDONE - do this tomorrow! after the meteor shower!
        if path == '/': return 1

        if path[-1] == '/':
            path = path[0:-1]      #Truncate trailing '/'
        if path[0] == '/':
            path = path[1:]         #Truncate leading '/'

        dirs = path.split('/')
        
        #Start at root inode
        currinodeno = 1
        currinode = Inode(str=Segment.segmentmanager.blockread(InodeMap.inodemap.lookup(1)))


        #Go through directories in path
        for dirname in dirs:
            foundmatch = False
            if currinode.isDirectory:
                dd = DirectoryDescriptor(currinodeno)
                for name, inodeid in dd.enumerate():
                    if name == dirname:
                        currinodeno = inodeid
                        currinode = Inode(str = Segment.segmentmanager.blockread(InodeMap.inodemap.lookup(currinodeno)))
                        foundmatch = True
                        break
                if not foundmatch:
                    return None
            else: 
                if dirs[-1] != dirname:
                    return None
        return currinode.id


    # add the new directory entry to the data blocks,
    # write the modified inode to the disk,
    # and update the inode map
    def append_directory_entry(self, dirinode, filename, newinode):
        dirinode.write(dirinode.filesize, struct.pack("%dsI" % FILENAMELEN, filename, newinode.id))

filesystem = None
