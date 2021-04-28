import socket
import string
import random
import json
import dill as pickle
import codecs

CONNECTION_PACKET = "HEADER:CONN||BODY:"
ACK_PACKET =  "HEADER:ACK||BODY:"
DATA_PACKET = "HEADER:DATA||BODY:"
DISCON_PACKET = "HEADER:STOP||BODY:"

name = ""

def padMessage(msg):
    return msg + (b' ' * (1024-len(msg)))

def parsePacket(c,rec):
    packetType = rec.split("||")[0].split(":")[1]
    bodyContents = rec.split("||")[1].split(":")[1][:-1].strip()

    if packetType == "STOP":
        return 0
    elif packetType == "ACK":
        return 1
    elif packetType == "DATA":
        toSum = json.loads(bodyContents)
        func = toSum[-1]
        func = pickle.loads(codecs.decode(func.encode(), "base64"))
        answer = func(toSum[:-1])
        sendMessage(c,DATA_PACKET+name+"//"+str(answer))

        return 1

def sendMessage(s,message):
    s.send(padMessage(message.encode('UTF-8')))

def listen(s):
    while True:
        try:
            rec = s.recv(1024)
            res = parsePacket(s,rec.decode("UTF-8"))
            if res == 0 or not rec:
                print("Connection Closed")
                break
        except KeyboardInterrupt:
            s.send(padMessage((DISCON_PACKET+name).encode('UTF-8')))
            print("Sent disco packet")

def openConnection():
    global name
    s = socket.socket()
    host = socket.gethostname()
    port = 8070

    letters = string.digits
    name = ''.join(random.choice(letters) for i in range(10))
    s.connect((host, port))
    sendMessage(s,CONNECTION_PACKET+name)
    listen(s)
    s.close()

def main():
    openConnection()

main()
