
import json
import pickle
import StringIO
import sys
import zerorpc
import gevent
from termcolor import colored
import cloudpickle
import logging
import string
import re

logging.basicConfig()

class RDD(object):
    def __init__(self):
        pass
    

    def collectToDic(self, p):
        elements = {}
        while True:
            element = self.get(p)
            if element == None:
                break
            # print element
            elements.update(element)
            # print '-----'
            # print elements
            # print '-----'
        return elements

    def collect(self, p):
        elements = []
        while True:
            element = self.get(p)
            if element == None:
                break
            # print element
            elements.append(element)
            # print '-----'
            # print elements
            # print '-----'
        return elements

    def count(self, p):
        return len(self.collect(p))

    def reduce(self, func):
        pass

    def save(self, p):
        
        if p != 10:
            
            stringWriteToFile = {}
            while True:

                temp = self.get(p)
                if temp == None:
                    break
                stringWriteToFile.update(temp)

        return stringWriteToFile
        
 

class TextFile(RDD):

    def __init__(self, filename):
        self.filename = filename
        self.lines = None
        self.index = 0

    def get(self, p):    
        start = p['start']
        end = p['end'] 

        lines = open(self.filename, 'r').read()[start:end]
        # print 'lines:%s' %lines
        self.lines = lines.strip().split('\n')
 
        if self.index == len(self.lines):
            self.index = 0
            return None
            
        else:
            line = self.lines[self.index]
            # line = re.sub('[!@#$,.\"\']', '', line)
         
            self.index += 1
            # print 'line: %s' %line
            return line


class FlatMap(RDD):

    def __init__(self, parent, func):
        self.parent = parent
        self.func = func

    def get(self, p):
        # print 'flat map'
        element = self.parent.get(p)
        # print '~~!~~~!~~'
        # print element
        # print colored('------', 'red')
        # print 'run flat map'
        # print p
        # print self.get(p)
        # print colored('------', 'red')
        if element == None:
            return None
        else:
            # print type(element)
            element_new = self.func(element)
            # print 'flat map result: %s' % element_new
            return element_new

class Map(RDD):

    def __init__(self, parent, func):
        self.parent = parent
        self.func = func

    def get(self, p):
        # print 'mapping!'
        elements = self.parent.get(p)
        # print '~~!~~~!~~~!~~!~~~'
        # print elements
        # print colored('------', 'red')
        # print 'run map'
        # print p
        # print self.get(p)
        # print colored('------', 'red')

        # print elements
        if elements == None:
            return None
        else:
            elements_new = []
            for element in elements:
                a = self.func(element)
                # print 'self func element: %s' % a
                elements_new.append(a)
            # print 'map result: % s' % elements_new
            return elements_new

class MapValues(RDD):

    def __init__(self, parent, func):
        self.parent = parent
        self.func = func

    def get(self, p):
        elements = self.parent.get(p)
        # print elements
        if elements == None:
            return None
        else:
            for k, v in elements.iteritems():
                v = self.func(v)
        return elements

class Filter(RDD):
    
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func

    def get(self, p):
        while True:
            element = self.parent.get(p)
            # print 'test'
            # print p
            if element == None:
                return None
            else:
                if self.func(element):
                    return element


class GroupByKey(RDD):
    def __init__(self, parent):
        self.parent = parent
        # self.func = func
        self.new_element = {}
    def get(self, p):
        element = self.parent.get(p)
        if element == None:
            self.new_element = {}
            return None
        else:
            # print element
            for k in element:
                if k in self.new_element:
                    self.new_element[k].append(element[k])
                else:
                    self.new_element[k] = [element[k]]
            return self.new_element

class ReduceByKey(RDD):

    def __init__(self, parent, func):
        super(ReduceByKey, self).__init__()
        self.parent = parent
        self.func = func
        self.new_element = {}

    def get(self, p):
        element = self.parent.get(p)
        if element == None:
            self.new_element = {}
            return None
        else:
            for k, v in element:
                # print 'k%s'% k
                # print 'k%s'% v
                if k in self.new_element:
                    self.new_element[k] = self.func(self.new_element[k], v)
                else:
                    self.new_element[k] = v
            # print 'reduce by key result new_element: %s' % self.new_element
            return self.new_element

# class Join(RDD, inMap):
#     def __init__(self, parent):
#         super(Join, self).__init__()
#         self.parent = parent
#         self.new_element = {}

#     def get(self, p):
#         element = self.parent.get(p)
#         if element







