import StringIO
import cloudpickle
import sys

import gevent
import zerorpc
import re

from rdd import *


from termcolor import colored
import pickle




class StatusEnum(object):
    def __init__(self):
        self.Status_Ready = 'READY'
        self.Status_Down = 'DOWN'
        self.Status_Working = 'WORKING'

class Worker(object):

    def __init__(self):

        self.ip = ''
        self.port = 0
        
        self.rddLineageList = []
        self.partitions = []
        self.workFinished = True

        gevent.spawn(self.controller)

    # Check the worker is alive or not.
    def controller(self):
         
        while True: 
            print colored('[Worker:]', 'green')
            gevent.sleep(1)

  
    def getPartitionList(self, list):
        input = StringIO.StringIO(list)
        unpickler = pickle.Unpickler(input)
        recieve_partitions = unpickler.load()
        self.partitions.extend(recieve_partitions)
        # print 'test'
        print colored('[Worker:]', 'green'), colored('recieve partiton list: %s','red') % recieve_partitions 


 
# Get ping from master
    def ping(self):
        print colored('[Worker:]', 'green'), colored('Get Ping from Master', 'red')
        
        
    def getRddLineage(self, rddL):
        
        input = StringIO.StringIO(rddL)
        unpickler = pickle.Unpickler(input)
        lineage = unpickler.load()
        # print lineage
        self.rddLineageList = lineage
        print colored('[Worker:]', 'green'), colored('recieve rdd lineage list: %s','red') % lineage 

        # print colored('rdd list : %s', 'red') % self.rddLineageList
 

    def collect(self):
        self.workFinished = False
        while True:
            if self.workFinished:
                break

            if len(self.partitions) == 0:
                gevent.sleep(1)
                continue
            else:
                for p in self.partitions:
                    string_toSent = self.rddLineageList[-1].collect(p)
                    print colored('[Worker:]', 'green'), colored('sent result: %s to driver') % string_toSent
                    self.sendResultToDriver(string_toSent, p['partition_name'])
                    self.partitions.remove(p)
                    print colored('[Worker:]', 'green'),colored('One partiton finshed', 'red')  


   # http://segmentfault.com/a/1190000000711128 : mapper()   
    def doPageRank(self, ranksReviced):
        input = StringIO.StringIO(ranksReviced)
        unpickler = pickle.Unpickler(input)
        ranks = unpickler.load()
        self.workFinished = False

        while True:
            if self.workFinished:
                break

            if len(self.partitions) == 0:
                gevent.sleep(1)
                continue
            else:
                for p in self.partitions:
                    links = self.rddLineageList[-1].collectToDic(p)
                    
                    contribute = []
                    for k, v in links.iteritems():
                        print ranks[k][1], ranks[k][0]
                        size = len(v)
                        for url in v:
                            contribute.append((url, (k, ranks[k][1]/ranks[k][0])))
                    print contribute
                    # print colored('[Worker:]', 'green'), colored('sent result: %s to driver') % number_toSent
                    self.sendResultToDriver(contribute, p['partition_name'])
                    self.partitions.remove(p)
                    print colored('[Worker:]', 'green'),colored('One partiton finshed', 'red')  

    def count(self):
        # gevent.sleep(5)
        self.workFinished = False
        while True:
            if self.workFinished:
                break

            if len(self.partitions) == 0:
                gevent.sleep(1)
                continue
            else:
                for p in self.partitions:
                    number_toSent = self.rddLineageList[-1].count(p)
                    print colored('[Worker:]', 'green'), colored('sent result: %s to driver') % number_toSent
                    self.sendResultToDriver(number_toSent, p['partition_name'])
                    self.partitions.remove(p)
                    print colored('[Worker:]', 'green'),colored('One partiton finshed', 'red')  


    #   Called by driver 
    def saveAsTextFile(self):
        # gevent.sleep(5)
        # print '1@~~~~~'
        self.workFinished = False

        while True:
            # print '2@~~~~'

            if self.workFinished:
                break

            if len(self.partitions) == 0:
                gevent.sleep(1)
                continue
            else:
                # print '3@~~~'
                for p in self.partitions:
                    gevent.sleep(5)

                    string_toSent =  self.rddLineageList[-1].save(p)
                    print colored('[Worker:]', 'green'), colored('sent result: %s to driver') % string_toSent

                    self.sendResultToDriver(string_toSent, p['partition_name'])
                    self.partitions.remove(p)
                    # print '9@~~~~'
                    # c.changePartitionStatus(p['partition_name'])
                    print colored('[Worker:]', 'green'),colored('One partiton finshed', 'red')  
    
    def stopWorking(self):
        self.workFinished = True
        print colored('[Worker:]', 'green'), colored('Totoal partitons finished! Initialized worker','red')
        self.rddLineageList = []

    def sendResultToDriver(self, result, pName):
        if type(result) is int:
            objstr = result
        else:   
            output = StringIO.StringIO()
            pickler = cloudpickle.CloudPickler(output)
            pickler.dump(result)
            objstr = output.getvalue()
        # print '6@~~~'

        
        c = zerorpc.Client()
        c.connect('tcp://0.0.0.0:4040')
        c.getSingleResult_FromWorker(objstr, self.port, pName, async=True)

if __name__ == "__main__":
    a = Worker()
    
    a.ip = '0.0.0.0'
   
    a.port = sys.argv[1]

    
    master_addr = 'tcp://0.0.0.0:4040'

    c = zerorpc.Client()
    c.connect(master_addr)
    
    c.register(a.ip, a.port)
    

    # worker = Worker()
    s = zerorpc.Server(a)
    s.bind('tcp://' + a.ip + ':' + a.port)
    s.run()
    # worker.driver = c
    
    s.run()
    
    
