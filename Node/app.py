import socket
import time
import threading
import json
import os
from random import uniform


IS_LEADER = False
IS_Follower = True
IS_Candidate = False

Sender = os.environ.get('Node_Name')
DB_Name = os.environ.get('DBname')


# connection_to_mongo(sender)

Term = 0
Voted_for = ""
logs = []
timeout = uniform(6.1,8.5)
heartbeat = 1
Votes = 0
Stop_Thread = False
Leader = ""
stop_listener = False
successfull_Commits = 0
ControllerIP = 0
counter = 1
random = 0



# Listener
def listener(skt):
    global IS_Follower, IS_Candidate, Votes, Voted_for, Term, IS_LEADER, Stop_Thread, Leader, stop_listener, successfull_Commits, ControllerIP, counter, random
    print(f"Listener started for:{Sender}")
    # if IS_Follower == "true":
    #     skt.settimeout(timeout)
    while True:
        if stop_listener == True:
            break
        skt.settimeout(timeout)
        try:
            msg, addr = skt.recvfrom(1024)
            decoded_msg = json.loads(msg.decode('utf-8'))
            # print(f"Request recieved: {decoded_msg['request']}, Term: {decoded_msg['term']}, Sender: {decoded_msg['sender_name']}")

            if decoded_msg["request"] == "APPEND_RPC" and Term > decoded_msg["term"]:
                print(f"Leader has smaller term {decoded_msg['term']}..Starting Election")
                if IS_LEADER == True:
                    IS_LEADER = False
                    Stop_Thread = True
                if IS_Follower == True:
                    print(f"Timed out..starting Election")
                    Votes += 1
                    Term += 1
                    IS_Follower = False
                    IS_Candidate = True
                    Voted_for = Sender

                    threading.Thread(target=Voting, args=[Sender]).start()


            if decoded_msg["request"] == "STORE":
                key = decoded_msg["key"]
                value = decoded_msg["value"]
                if IS_Follower == True:
                    message = {"sender_name": Sender, "request": "LEADER_INFO", "term": Term, "key": "LEADER", "value": Leader}
                    msg_bytes = json.dumps(message).encode('utf-8')
                    ControllerIP = addr[0]
                    skt.sendto(msg_bytes, (addr[0], 5555))
                elif IS_LEADER:

                    if logs == []:
                        msg = {"sender_name": Sender, "LeaderID": Sender, "request": "APPEND_REQUEST_RPC", "term": Term, "key": key, "value": value,
                               "logs": logs,
                               "prevLogIndex": len(logs)-1, "prevLogTerm": [], "Entry": None}
                    else:
                        msg = {"sender_name": Sender, "LeaderID": Sender, "request": "APPEND_REQUEST_RPC", "term": Term,
                               "logs": logs, "key": key, "value": value,
                               "prevLogIndex": len(logs)-1, "prevLogTerm": logs[-1]["value"], "Entry": None}

                    msg_bytes = json.dumps(msg).encode('utf-8')
                    ControllerIP = addr[0]
                    targetList = getTarget(Sender)

                    for i in targetList:
                        print(f"Sending Store request to {i}")
                        if i != None:
                            skt.sendto(msg_bytes, (i,5555))

            if decoded_msg["request"] == "APPEND_REQUEST_RPC":
                key = decoded_msg["key"]
                value = decoded_msg["value"]
                term = decoded_msg["term"]
                prevLogTerm = [] if logs == [] else logs[-1]["value"]

                print(f"Recieved Index: {decoded_msg['prevLogIndex']}")
                print(f"Node's Log index: {len(logs)-1}")

                print(f"Recieved Log Term: {decoded_msg['prevLogTerm']}")
                print(f"Node's Last Log Term: {prevLogTerm}")

                if decoded_msg["prevLogIndex"] == len(logs) - 1 and decoded_msg["prevLogTerm"] == prevLogTerm:

                    logs.append({"Term": term, "Key": key, "value": value})


                    message = {"sender_name": Sender, "request": "APPEND_REPLY", "term": Term, "key": key,
                               "value": value, "success": True}
                    msg_bytes = json.dumps(message).encode('utf-8')

                    print(f"Commited the Store Request on: {Sender}")
                    skt.sendto(msg_bytes, (addr[0], 5555))
                else:
                    message = {"sender_name": Sender, "request": "APPEND_REPLY", "term": Term, "key": None,
                               "value": None, "prevLogIndex": len(logs) - 1, "prevLogTerm": prevLogTerm, "success": False}
                    msg_bytes = json.dumps(message).encode('utf-8')

                    skt.sendto(msg_bytes, (addr[0], 5555))




            if decoded_msg["request"] == "APPEND_REPLY":
                success = decoded_msg["success"]
                if success == True:
                    successfull_Commits += 1
                    print(f"Followers who have commited: {successfull_Commits}")

                if success == False:
                    prevLogIndex = decoded_msg["prevLogIndex"]
                    prevLogTerm = decoded_msg["prevLogTerm"]

                    temp = []

                    for i in range(prevLogIndex+1, len(logs)-1):
                        temp.append(logs[i])

                    message =  {"sender_name": Sender, "request": "APPEND_REQUEST_REPLY", "term": Term, "key": None,
                               "value": None, "prevLogIndex": len(logs) - 1, "prevLogTerm": prevLogTerm, "Entry": temp}

                    msg_bytes = json.dumps(message).encode('utf-8')

                    skt.sendto(msg_bytes, (addr[0], 5555))


                if successfull_Commits >= 3:
                    successfull_Commits = 0
                    logs.append({"Term": Term, "Key": key, "value": value})

                    print("Leader Commited logs after Majority!!!")


            if decoded_msg["request"] == "APPEND_REQUEST_REPLY":
                entries = decoded_msg["Entry"]

                for i in entries:
                    logs.append(i)

                print(f"Added the Missing entries to the log, the last index is: {len(logs)-1}")


            if decoded_msg["request"] == "RETRIEVE":
                if IS_Follower == True:
                    message = {"sender_name": Sender, "request": "LEADER_INFO", "term": Term, "key": "LEADER",
                               "value": Leader}
                    msg_bytes = json.dumps(message).encode('utf-8')
                    skt.sendto(msg_bytes, (addr[0], 5555))

                if IS_LEADER == True:
                    message = {"sender_name": Sender, "request": "RETRIEVE", "Term": Term, "Key": "COMMITED_LOGS", "value": logs}
                    msg_bytes = json.dumps(message).encode('utf-8')
                    skt.sendto(msg_bytes,(addr[0], 5555))



            elif decoded_msg["request"] == "APPEND_RPC":
                if IS_LEADER == True:
                    IS_LEADER = False
                    IS_Follower = True
                    Stop_Thread = True
                if decoded_msg["term"] > Term:
                    Term = decoded_msg["term"]
                if IS_Candidate == True and decoded_msg["term"] >= Term:
                    IS_Candidate = False
                    IS_Follower = True
                if IS_LEADER == True:
                    Stop_Thread = True
                # print(f"HeartBeat Received from {decoded_msg['LeaderID']}")
                Leader = decoded_msg["LeaderID"]
                Voted_for = ""
                Votes = 0

            if decoded_msg["request"] == "CONVERT_FOLLOWER":
                if IS_Candidate == True:
                    print("Candidate going to be Follower!")
                    IS_Candidate = False
                    IS_Follower = True
                elif IS_LEADER == True:
                    print("Leader going to be Follower!")
                    Stop_Thread = True
                    IS_LEADER = False
                    IS_Follower = True
                elif IS_Follower == True:
                    print("Already a Follower!")
                    continue

            if decoded_msg["request"] == "LEADER_INFO":
                message = {"sender_name": Sender, "request": None, "term": None, "key": "LEADER", "value": Leader}
                msg_bytes = json.dumps(message).encode('utf-8')
                if random > 0:
                    time.sleep(22)
                random += 1
                skt.sendto(msg_bytes, (addr[0], 5555))

            if decoded_msg["request"] == "TIMEOUT":
                print(f"Timing out {Sender}")
                if IS_LEADER == True:
                    Stop_Thread = True
                    time.sleep(60)
                else:
                    raise socket.timeout


            if decoded_msg["request"] == "SHUTDOWN":
                print(f'Shutting down all threads on {Sender}')
                skt.close()
                Stop_Thread = True
                stop_listener = True

            if decoded_msg["request"] == "VOTE_REQUEST":
                print(f'{decoded_msg["sender_name"]} asking for votes...')
                if decoded_msg["term"] < Term:
                    continue

                elif decoded_msg["term"] >= Term and decoded_msg["prevLogIndex"] < len(logs) - 1:
                    continue

                elif decoded_msg["term"] > Term and decoded_msg["prevLogIndex"] >= len(logs) - 1 and Voted_for == "":
                    if IS_LEADER == True:
                        IS_LEADER = False
                        IS_Follower = True
                    Stop_Thread = True
                    message = {"sender_name": Sender, "request": "VOTE_ACK", "term": Term, "key": 0, "value": 0}
                    msg_bytes = json.dumps(message).encode('utf-8')
                    # print(f"Message Bytes: {msg_bytes}")
                    # print(f"Address: {addr[0]}")
                    skt.sendto(msg_bytes, (addr[0], 5555))
                    Voted_for = addr[0]


            if IS_Candidate == True:
                if decoded_msg["request"] == "VOTE_ACK" and decoded_msg["term"] <= Term:
                    print(f"{decoded_msg['sender_name']} sent a vote")
                    if Votes > 2 and IS_Candidate == True:
                        IS_Candidate = False
                        IS_LEADER = True
                        print(f"New Leader Elected {Sender}")
                        Votes = 0
                        Stop_Thread = False

                        threading.Thread(target=appendRPC, args=[UDP_Socket]).start()
                    else:
                        Votes += 1

        except socket.timeout:
            if IS_Follower==True:
                print(f"Timed out..starting Election")
                Votes += 1
                Term += 1
                IS_Follower = False
                IS_Candidate = True
                Voted_for = Sender

                threading.Thread(target=Voting, args=[Sender]).start()


def Voting(sender):
    sender1 = sender

    targetList = getTarget(sender1)

    Voting_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    sender_ip = socket.gethostbyname(Sender)

    Voting_Socket.bind((sender_ip, 1111))

    msg = {"sender_name": Sender, "request": "VOTE_REQUEST", "term": Term, "key": None, "value": None,
           "lastLogIndex": 0, "lastLogTerm": 0, "prevLogIndex": len(logs)-1}

    msg_bytes = json.dumps(msg).encode('utf-8')

    for i in targetList:
        if i!= None:
            Voting_Socket.sendto(msg_bytes,(i,5555))


    Voting_Socket.close()

def appendRPC(skt):
    global target2, target1, target3, target4, Voted_for, Stop_Thread, Leader, IS_LEADER, IS_Follower
    if IS_LEADER == True:
        Leader = Sender
    Voted_for = ""
    msg = {"sender_name": Sender, "LeaderID": Sender, "request": "APPEND_RPC", "term": Term, "logs": logs,
           "prevLogIndex": len(logs) - 1, "prevLogTerm": None, "Entry": None}

    msg["prevLogTerm"] = None if logs == [] else logs[-1]

    msg_bytes = json.dumps(msg).encode('utf-8')

    targetList = getTarget(Sender)

    while True:
        # if Stop_Thread == True:
        #     print(f'{Sender} no longer leader, no heartbeats')
        #     break
        if Stop_Thread == True:
            IS_LEADER = False
            IS_Follower = True
            print(f'{Sender} no longer leader, no heartbeats')
            break

        for i in targetList:
            if i != None:
                skt.sendto(msg_bytes,(i, 5555))
                # print(f'Sending to {i}')

        time.sleep(heartbeat)


def getTarget(sender):
    sender1 = sender

    if sender1 == "Node1":
        target1 = "Node2"
        target2 = "Node3"
        target3 = "Node4"
        target4 = "Node5"
    elif sender1 == "Node2":
        target1 = "Node1"
        target2 = "Node3"
        target3 = "Node4"
        target4 = "Node5"
    elif sender1 == "Node3":
        target1 = "Node1"
        target2 = "Node2"
        target3 = "Node4"
        target4 = "Node5"
    elif sender1 == "Node4":
        target1 = "Node1"
        target2 = "Node2"
        target3 = "Node3"
        target4 = "Node5"
    elif sender1 == "Node5":
        target1 = "Node1"
        target2 = "Node2"
        target3 = "Node3"
        target4 = "Node4"

    try:
        r1 = socket.gethostbyname(target1)
    except socket.gaierror:
        r1 = None
        print(f"Host Closed {target1}")

    try:
        r2 = socket.gethostbyname(target2)
    except socket.gaierror:
        r2 = None
        print(f"Host Closed {target2}")

    try:
        r3 = socket.gethostbyname(target3)
    except socket.gaierror:
        r3 = None
        print(f"Host Closed {target3}")

    try:
        r4 = socket.gethostbyname(target4)
    except socket.gaierror:
        r4 = None
        print(f"Host Closed {target4}")

    return [r1, r2, r3, r4]


if __name__ == "__main__":
    print(f"Starting {Sender}")


    # Creating Socket and binding it to the target container IP and port
    UDP_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    host_ip = socket.gethostbyname(Sender)

    # Bind the node to sender ip and port
    UDP_Socket.bind((host_ip, 5555))

    threading.Thread(target=listener, args=[UDP_Socket]).start()

    # time.sleep(100000)

