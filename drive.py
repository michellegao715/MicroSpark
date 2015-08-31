import pickle
import StringIO
import sys
import zerorpc
import gevent
from termcolor import colored
from rdd import *
import cloudpickle
import time
import math
import copy
 
import os.path
import string 


import code
from gevent import fileobject
from zerorpc.exceptions import TimeoutExpired



class StatusEnum(object):
    
    Status_Ready = 'READY'
    Status_Down = 'DOWN'
    Status_Working = 'WORKING'
    


class Spark(object):

    def __init__(self):
        self.persistedRDDlineageList = []
        self.current_WorkingOn_FileName = ''
        self.partitions = []
        self.workers = []
        self.tasks = []
        self.chunk = 5

        self.rddLineageList = []

        self._green_stdin = fileobject.FileObject(sys.stdin)
        self._green_stdout = fileobject.FileObject(sys.stdout)

        self.method = 'a'
        self.single_ResultList = []
        self.total_ResultList = []
        self.totalResult = None
        self.filename = None


        self.currentWork = {}
        print colored('======Spark initialized, plz w8 until workers online======', 'white')
    

        gevent.spawn(self.controller)
        gevent.spawn(self.run_console)

    def _green_raw_input(self, prompt):
        self._green_stdout.write(prompt)
        return self._green_stdin.readline()[:-1]

    def run_console(self, local=None, prompt=">>>"):
        code.interact(prompt, self._green_raw_input, local=dict(globals(), **locals()) or {})  

#  
# TODO
# 

    def partitionFile(self, filename):
        self.partitions = []
        partitionList = [] 
        
        input_file_bytes = os.path.getsize(filename)
        print input_file_bytes
        f=open(filename)
        # f.close()

        split_size = 50
        end = 0
        start = 0
        p_num = 0
        while  end < input_file_bytes:
            # print 'input file bytes: %s'% input_file_bytes
            # print 'end%s' %end
            # print 'start%s'%start
            # gevent.sleep(2)
            
            print end
            partition = {}
            partition['status'] = False
            partition['start'] = start
            
            if input_file_bytes < split_size:
                partition['end'] = input_file_bytes
                partition['partition_name'] = p_num
                self.partitions.append(partition)
                break

            if start < input_file_bytes - split_size:
                seekNum = start + split_size
                f.seek(seekNum)

                endValue = f.read(1)
                # print 'end v: %s' %endValue

                if endValue == '\n':
                    end = f.tell()

                else:
                    # print 'endValue:%s'% endValue
                    while endValue != '\n':
                        endValue = f.read(1)
                        if f.tell == input_file_bytes:
                            break
                    end = f.tell()
                    # print 'end :%s' % end
                start = end
            else:
                end = input_file_bytes
                print 'here', end
            partition['end'] = end
            partition['partition_name'] = p_num
            p_num+=1
            # print 'p:%s' % partition
            self.partitions.append(partition)
        print self.partitions


    # def getPartitions(self):
    #     return self.partitions
#    
    def createRank(self, filename):
        f = open(filename, 'r')
        lines = f.read()
        lines = lines.split('\n')
        dic = {}
        for line in lines:
            values = line.split()
            if values[0] in dic.keys():
                dic[values[0]] += 1
            else:
                dic[values[0]] = 1
        for k, v in dic.iteritems():
            dic[k] = [v, 1.0/len(dic)]

        return dic


    def setPartionsToWorker(self, partitions):
        partitions_usedTosetTask = copy.deepcopy(partitions)
        # print '1'
        worker_num = len(self.workers)
        # print worker_num
        partition_num = len(partitions_usedTosetTask)
        # print partition_num

        if partition_num % worker_num == 0:
            pop_num = partition_num/worker_num
        else:
            pop_num = partition_num / worker_num + 1

        for worker in self.workers:
            # print '-------'
            # print partitions_usedTosetTask
            # print '-------'
            if len(partitions_usedTosetTask) == 0:
                break
            # print '2'
            i = 0
            while i < pop_num:
                p = partitions_usedTosetTask.pop()
                worker['current_task'].append(p)
                i+=1
                # print i
                if len(partitions_usedTosetTask) == 0:
                    break;
            # print 'test2'       
            # print worker['current_task']
            # # sent partitions to workers
            worker['status'] = StatusEnum.Status_Working
            partition_List_toSent = []
            p_name_toPrint = []
            for ct in worker['current_task']:
                # print ct['status']
                if not ct['status']:
                    partition_List_toSent.append(ct)
                    p_name_toPrint.append(ct['partition_name'])
            try:

                c = zerorpc.Client()
                c.connect('tcp://' + worker['ip'] + ':' + worker['port'])

                output = StringIO.StringIO()
                pickler = cloudpickle.CloudPickler(output)
                pickler.dump(partition_List_toSent)
                sent = output.getvalue()

                print colored('[Master:]', 'white'), colored('Sent partitons:%s to worker: %s', 'red') % (p_name_toPrint, worker['port'])

                c.getPartitionList(sent, async=True)

                output = StringIO.StringIO()
                pickler = cloudpickle.CloudPickler(output)
                pickler.dump(self.rddLineageList)
                objstr = output.getvalue()

                print colored('[Master:]', 'white'), colored('sent rdd lineage to worker: %s', 'red') % worker['port']

                c.getRddLineage(objstr, async=True)
            except TimeoutExpired:
                print colored('[Exception:]exception happened when sent partition and rdd lineage to workers!', 'red')
                continue
            except LostRemote:
                continue
                                        
    def textFile(self, fileName):
        self.current_WorkingOn_FileName = fileName
        # print '1~~'
        self.rddLineageList.append(TextFile(fileName))
        # print self.rddLineageList
        return self

# 
    def map(self, func):
        if not self.rddLineageList:
            parent = ''
        else:
            parent = self.rddLineageList[-1]
        self.rddLineageList.append(Map(parent,func))
        # print self.rddLineageList
        return self

# 
    def flatMap(self, func):
        # print '3~~'
        if not self.rddLineageList:
            parent = ''
        else:
            parent = self.rddLineageList[-1]
        self.rddLineageList.append(FlatMap(parent,func))
        # print self.rddLineageList
        return self

# 
    def mapValues(self, func):
        # print '3~~'
        if not self.rddLineageList:
            parent = ''
        else:
            parent = self.rddLineageList[-1]
        self.rddLineageList.append(MapValues(parent,func))
        # print self.rddLineageList
        return self

# 
    def reduceByKey(self, func):
        # print '4~~'
        if not self.rddLineageList:
            parent = ''
        else:
            parent = self.rddLineageList[-1]
        self.rddLineageList.append(ReduceByKey(parent,func))
        # print self.rddLineageList
        return self

#   
    def filter(self, func):
        if not self.rddLineageList:
            parent = ''
        else:
            parent = self.rddLineageList[-1]
        self.rddLineageList.append(Filter(parent, func))
        return self

    def groupByKey(self):
        if not self.rddLineageList:
            parent = ''
        else:
            parent = self.rddLineageList[-1]

        self.rddLineageList.append(GroupByKey(parent))
        return self


    def persist(self):
        self.persistedRDDlineageList = copy.deepcopy(self.rddLineageList)

    def saveAsTextFile(self, filename):
        self.single_ResultList = []
        self.filename = filename
        self.partitionFile(self.current_WorkingOn_FileName)

        
        # print len(self.partitions)
        # print temp
        self.setPartionsToWorker(self.partitions)
        # print 'self.partitions: %s'% self.partitions


        # print 'aaa~~~~~'
        # print self.workers
        for worker in self.workers:
            # print 'call next worker start work'
            
            try:
                c = zerorpc.Client(timeout = 9999999)
                c.connect('tcp://' + worker['ip'] + ':' + worker['port'])
                print colored('[Master:]', 'white'), colored('call worker: %s start working', 'red') % worker['port']
                c.saveAsTextFile(async=True)
                # worker['status'] = StatusEnum.Status_Ready
                # print '----test-----'

            except TimeoutExpired:
                # print 'ignoring timeout'
                continue
            except LostRemote:
                # print 'LostRemote event ignored'

                # print '2aaa'
                continue

    def collect(self):
        self.single_ResultList = []
        self.partitionFile(self.current_WorkingOn_FileName)
        self.setPartionsToWorker(self.partitions)

        for worker in self.workers:
            try:
                c = zerorpc.Client()
                c.connect('tcp://' + worker['ip'] + ':' + worker['port'])
                print colored('[Master:]', 'white'), colored('call worker: %s start working', 'red') % worker['port']
                c.collect(async=True)

            except TimeoutExpired:
                continue
            except LostRemote:
                continue   
        while not self.totalResult:
            gevent.sleep(0.5)
        return self.totalResult

    def count(self):
        self.single_ResultList = []
        self.partitionFile(self.current_WorkingOn_FileName)
        self.setPartionsToWorker(self.partitions)
        for worker in self.workers:
            try:
                c = zerorpc.Client()
                c.connect('tcp://' + worker['ip'] + ':' + worker['port'])
                print colored('[Master:]', 'white'), colored('call worker: %s start working', 'red') % worker['port']
                c.count(async=True)

            except TimeoutExpired:
                continue
            except LostRemote:
                continue


    def controller(self):
        gevent.sleep(10)
        # print '1~~'
        while True:

            # print '2~~'
            if self.method != 'repl':
                # print '3~~'
               
                printList = []
                for worker in self.workers:
                    

                    # print '4~~'
                    try:
                        # print '5~~'
                        c = zerorpc.Client(timeout=1)
                        c.connect('tcp://' + worker['ip'] + ':' + worker['port'])
                        c.ping()
                        printList.append(worker['port'])
                    except TimeoutExpired:
                        # print '6~~~'
                   
                        # if not worker['current_task']:
                        #     # print '7~~~'
                        #     # continue
                        # print '1test~~~~'
                        self.workers.remove(worker)
                        if worker['current_task']:
                            # print '2test~~~~'
                            temp = []
                            for p in worker['current_task']:
                                if not p['status']:
                                    temp.append(p)

                            # print 'temp %s' % temp
                            
                            # partiton_num = len(temp)
                            # worker_num = len(self.workers)


                            if len(temp) % len(self.workers) == 0:
                                pop_num = len(temp)/len(self.workers)
                            else:
                                pop_num = len(temp)/len(self.workers) + 1

                            # print 'pop_num: %s' % pop_num

                            for w in self.workers:
                                if worker['status'] == StatusEnum.Status_Down:
                                    continue
                                else:
                                    # print '3test~~~'
                                    
                                    i = 0
                                    partition_List_toSent_inRecovery = []
                                    partitonName_toPrint = []

                                    if len(temp) == 0:
                                        break
                                    else:
                                        while i < pop_num:

                                            temp_p = temp.pop()
                                            partition_List_toSent_inRecovery.append(temp_p)
                                            partitonName_toPrint.append(temp_p['partition_name'])

                                            i+=1
                                            if len(temp) == 0:
                                                break
                                        try:

                                            c = zerorpc.Client()
                                            c.connect('tcp://' + w['ip'] + ':' + w['port'])

                                            # print w['current_task']
                                            w['status'] = StatusEnum.Status_Working

                                            

                                            output = StringIO.StringIO()
                                            pickler = cloudpickle.CloudPickler(output)
                                            pickler.dump(partition_List_toSent_inRecovery)
                                            sent = output.getvalue()



                                            c.getPartitionList(sent)
                                            print colored('[Master:]', 'white'), colored('Sent partitons:%s to worker: %s', 'red') % (partitonName_toPrint, w['port'])

                                            worker['current_task'].extend(partition_List_toSent_inRecovery)
                                        except LostRemote:
                                            continue
                                        except TimeoutExpired:
                                            continue
                        print colored('[Master:]', 'white'), colored('(%s:%s) down', 'red') % (worker['ip'],worker['port'])
                    except LostRemote:
                        print 'LostRemote event ignored'
                        continue
                # print printList

            gevent.sleep(1)



 # Register new worker asynchronously
    def register_async(self, ip, port):
       
        
        worker = {}
        worker['ip'] = ip
        worker['port'] = port
        worker['status'] = StatusEnum.Status_Ready
        worker['current_task'] = [] 
        # worker['_task'] = []
        self.workers.append(worker)
        print colored('[Master:]', 'white'), colored('Register Worker (%s:%s)', 'red') % (ip,port)
        # print self.workers
        # print '!~~~~~'
        # print 'one worker registered, current worker number is ', len(self.workers)



 # Register new worker
    def register(self, ip, port):
        gevent.spawn(self.register_async, ip, port)

    # def changePartitionStatus(self, name):
    #     for worker in self.workers:
    #         for 
    #         if worker['current_task']['partition_name']

    
    def doPageRank(self, ranks):
        self.totalResult = None
        self.single_ResultList = []
        self.partitionFile(self.current_WorkingOn_FileName)

        self.setPartionsToWorker(self.partitions)
        # print 'self.partitions: %s'% self.partitions

        output = StringIO.StringIO()
        pickler = cloudpickle.CloudPickler(output)
        pickler.dump(ranks)
        rankSent = output.getvalue()


        for worker in self.workers:
            # print 'call next worker start work'
            
            try:
                c = zerorpc.Client(timeout = 9999999)
                c.connect('tcp://' + worker['ip'] + ':' + worker['port'])
                print colored('[Master:]', 'white'), colored('call worker: %s start working', 'red') % worker['port']
                c.doPageRank(rankSent, async=True)

            except TimeoutExpired:
                continue
            except LostRemote:
                continue
        while not self.totalResult:
            gevent.sleep(0.5)
        return self.totalResult


#  Called by worker
    def getSingleResult_FromWorker(self, r, port, p_name):
        # print '~~~~~~'
        # print self.getPartitions()

        if type(r) is int:
            result = r
        else:
            input = StringIO.StringIO(r)
            unpickler = pickle.Unpickler(input)
            result = unpickler.load()

        print type(result)
        for worker in self.workers:
            if worker['port'] == port:
                ready = True
                for ct in worker['current_task']:
                    if ct['partition_name'] == p_name:
                        ct['status'] = True
                    if not ct['status']:
                        ready = False
                if ready:
                    worker['status'] = StatusEnum.Status_Ready
                    print colored('[Master:]', 'white'), colored('worker:%s finish its works!', 'red') % worker['port']
        
            # print '---------------begin'
            # print worker
            # print '---------------end'

        self.single_ResultList.append(result)
        print 'partitions: ', len(self.partitions), ' result: ', len(self.single_ResultList)
        if len(self.single_ResultList) == len(self.partitions):
            for worker in self.workers:
                try:

                    c = zerorpc.Client()
                    c.connect('tcp://' + worker['ip'] + ':' + worker['port'])
                    c.stopWorking()
                except LostRemote:
                    continue
                except TimeoutExpired:
                    continue 

            return self.merge_toGetTotalResult(type(result))

# When meet merge condition, merge total result and append the result to totoal result list
    def merge_toGetTotalResult(self, type):
        self.totalResult = None
        if type is list:
            totalResult = []
            for r in self.single_ResultList:
                for k in r:
                    totalResult.append(k)
            print colored('[Master:]', 'white'), colored('count result is: %s', 'red') % totalResult
        elif type is int:
            totalResult = 0
            for r in self.single_ResultList:
                totalResult += r
            print colored('[Master:]', 'white'), colored('count result is: %s', 'red') % totalResult

        else:
            totalResult = {}
            for r in self.single_ResultList:
                

                    # print r
                for k in r:
                    # print 'k%s'% k
                    # print 'k%s'% v
                    if k in totalResult:
                        totalResult[k] = r[k] + totalResult[k]
                    else:
                        totalResult[k] = r[k]

        self.totalResult = totalResult
        
            # self.total_ResultList.append(totalResult)
        if self.filename:
            f = open(self.filename, 'w')
            json.dump(totalResult, f)
            f.close()


        self.rddLineageList = copy.deepcopy(self.persistedRDDlineageList)
        # self.initializeAll():

        print colored('===Finished! Type self.initializedAll if you want to start next work! ===','white')
        return totalResult
        # print colored(totalResult, 'red')

    def initializedAll(self):
        print colored('===========Start Initialize==========','white')
        self.current_WorkingOn_FileName = ''
        self.persistedRDDlineageList = []
        self.partitions = []
        self.current_WorkingOn_FileName = ''
        self.total_ResultList = []
        self.single_ResultList = []
        for worker in self.workers:
            worker['current_task'] = []
            worker['status'] = StatusEnum.Status_Ready
        print colored('===========Finished Initialize, You can start next work now==========','white')


if __name__ == '__main__':
    # print '~!~'
    port = sys.argv[1]
    c = Spark()
    # print 'test'
    s = zerorpc.Server(c)
    s.bind("tcp://0.0.0.0:" + port)
    s.run()
    # while c.rddLineageList:
    #   a=  self.textFile('myFile').flatMap(lambda line: line.split()).map(lambda word: (word.strip(',.\"\''), 1)).reduceByKey(lambda a, b: a + b).collect()
#  self.textFile('test_log').filter(lambda line:line.startsWith('ERROR')).persist()
  
    # d = c.textFile('myfile').flatMap(lambda line: line.split()).map(lambda word: (word, 1)).saveAsTextFile('result')
#  self.textFile('test_pagerank').flatMap(lambda line: {line.split()[0]: line.split()[1]}).groupByKey().collect()
  # self.textFile('test_pagerank').flatMap(lambda line: {line.split()[0] + ' ' + str(0.25): line.split()[1]}).groupByKey().saveAsTextFile('pageRankResult')  
