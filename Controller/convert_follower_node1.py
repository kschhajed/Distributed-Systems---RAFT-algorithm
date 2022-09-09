import json
import socket
import traceback
import time

# Wait following seconds below sending the controller request
time.sleep(10)

# Read Message Template
msg = json.load(open("Message.json"))

# Initialize
sender = "Controller"
target = "Node1"
port = 5555

# Request
msg['sender_name'] = sender
msg['request'] = "LEADER_INFO"


# Socket Creation and Binding
skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
skt.bind((sender, port))

# Send Message
try:
    # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")
j = 0
l = 0
while True:

        message, addr = skt.recvfrom(1024)
        decoded_msg = json.loads(message.decode('utf-8'))
        if decoded_msg["request"] == "LEADER_INFO":
            target3 = decoded_msg["value"]
            msg['request'] = "STORE"
            msg['key'] = "key2"
            msg['value'] = "value2"
            msg_bytes4 = json.dumps(msg).encode('utf-8')
            skt.sendto(msg_bytes4, (target3, 5555))
            time.sleep(5)
            msg['request'] = "RETRIEVE"
            print(f"New Request Created : {msg}")
            msg_bytes5 = json.dumps(msg).encode('utf-8')
            skt.sendto(msg_bytes5, (target3, 5555))

        if decoded_msg['request'] == 'RETRIEVE':
            logs = decoded_msg["value"]
            for i in logs:
                print(f"The Retrieved Key is: {i['Key']}, and Value is: {i['value']}")
        if j == 0:
            target1 = decoded_msg['value']
        msg['request'] = "STORE"
        print(f"New Request Created : {msg}")
        msg['key'] = "key1"
        msg['value'] = "value1"
        msg_bytes = json.dumps(msg).encode('utf-8')
        j += 1
        skt.sendto(msg_bytes, (target1, 5555))
        time.sleep(5)
        msg['request'] = "RETRIEVE"
        print(f"New Request Created : {msg}")
        msg_bytes1 = json.dumps(msg).encode('utf-8')
        skt.sendto(msg_bytes1,(target1, 5555))
        msg['request'] = "TIMEOUT"
        print(f"New Request Created : {msg}")
        msg_bytes2 = json.dumps(msg).encode('utf-8')
        skt.sendto(msg_bytes2, (target1, 5555))
        print("SLeeping for 15")
        time.sleep(20)
        msg['request'] = "LEADER_INFO"
        print(f"New Request Created : {msg}")
        msg_bytes3 = json.dumps(msg).encode('utf-8')
        skt.sendto(msg_bytes3, (target, 5555))




