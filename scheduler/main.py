import socket
import threading
from _thread import *
import json
import dill as pickle
import codecs

def map(inputData):
    cards = inputData[1]
    suitDict = {}
    for card in cards:
        suit = card[0]
        if suit in suitDict.keys():
            suitDict[suit] += 1
        else:
            suitDict[suit] = 1
    suitList = [(x,y) for x,y in suitDict.items()]
    return suitList

def reduce(inputData):
    suitDict = {}
    for deck in inputData:
        for pair in deck:
            suit = pair[0]
            count = pair[1]
            print(suit)
            print(count)
            if suit in suitDict.keys():
                print("Exists")
                suitDict[suit] += int(count)
            else:
                suitDict[suit] = int(count)
    suitList = [(x,y) for x,y in suitDict.items()]
    return suitList

CONNECTION_PACKET = "HEADER:CONN||BODY:"
ACK_PACKET =  "HEADER:ACK||BODY:"
DATA_PACKET = "HEADER:DATA||BODY:"
DISCON_PACKET = "HEADER:STOP||BODY:"

headConn = None
tailConn = None

mapFunc = codecs.encode(pickle.dumps(map),"base64").decode()
reduceFunc = codecs.encode(pickle.dumps(reduce),"base64").decode()

jobs = []
answers = []

class connectionNode:
    def __init__(self,con,name):
        self.con = con
        self.name = name
        self.next = None

def buildJobs():
    global jobs
    jobs = [["deck1",[("heart",1),("diamond",1),("club",1),("spade",1),("heart",1),("diamond",1),("club",1),("spade",1)],mapFunc],["deck1",[("heart",1),("diamond",1),("club",1),("spade",1)],mapFunc]]

def sendJobs():
    global jobs
    global headConn
    global answers
    while True:
        toPop = []
        for ind,job in enumerate(jobs):
            #find an open node
            if(headConn != None):
                #assign job to headConn
                sendMessage(headConn.con,DATA_PACKET+json.dumps(job))
                toPop.append(ind)
                #remove headConn from pool
                removeFromPool(headConn.name)
                
        for i in toPop:
            jobs.pop(i)
        if(len(answers) > 1):
            answers.append(reduceFunc)
            jobs.append(answers)
            answers = []
        

def startServer():
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    host = socket.gethostname()
    port = 8070
    s.bind((host,port))
    return s

def padMessage(msg):
    return msg + (b' ' * (1024-len(msg)))

def sendMessage(c,msg):
    c.send(padMessage(msg.encode('UTF-8')))

def addToPool(c,name):
    global headConn
    global tailConn
    newNode = connectionNode(c,name)
    if(headConn):
        tailConn.next = newNode
        tailConn = newNode
    else:
        headConn = newNode
        tailConn = newNode

def removeFromPool(name):
    global headConn
    currentNode = headConn
    lastNode = None
    index = 0
    while(currentNode and currentNode.next):
        if(currentNode.name == name):
            break
        lastNode = currentNode
        currentNode = currentNode.next
        index+=1

    if(currentNode.name == name):
        #remove current node
        if(lastNode == None):
            headConn = currentNode.next
        else:
            lastNode.next = currentNode.next

def printPool():
    print("Printing Pool: ")
    global headConn
    currentNode = headConn
    while(currentNode and currentNode.next):
        print(currentNode.name)
        currentNode = currentNode.next
    if(currentNode != None):
        print(currentNode.name,"->")

def parsePacket(c,rec):
    global answers
    packetType = rec.split("||")[0].split(":")[1]
    bodyContents = rec.split("||")[1].split(":")[1][:-1].strip()
    if packetType == "CONN":
        #acknowledge connection
        sendMessage(c,ACK_PACKET)
        #add to pool
        addToPool(c,bodyContents)
        # removeFromPool(bodyContents)
        return 1
    elif packetType == "STOP":
        #remove from pool 
        removeFromPool(bodyContents)
        #send a disconnet packet
        sendMessage(c,DISCON_PACKET)
        printPool()
        #close the connection
        c.close()
        return 0
    elif packetType == "ACK":
        return 1
    elif packetType == "DATA":
        #do something with the data
        name = bodyContents.split("//")[0]
        answer = bodyContents.split("//")[1]
        answers.append(json.loads(answer))
        print("New answer:",answers)
        addToPool(c,name)
        # print(bodyContents)
        return 1
    
def listen(c):
    while True:
        rec = c.recv((1024))
        ret = parsePacket(c,str(rec))
        if ret == 0:
            break

def allowConnections(s):
    start_new_thread(sendJobs,())
    s.listen(5)
    
    while True:
            c, addr = s.accept()
            print("Got connection from",addr)
            # listen(c)
            start_new_thread(listen,(c,))

def main():
    buildJobs()
    s = startServer()
    try:
        allowConnections(s)
    except KeyboardInterrupt:
        print("Scheduler shut down")
        exit()

main()