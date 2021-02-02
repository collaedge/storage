from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
import time
import os
import random
import uuid
import sys
import string

from sawtooth_job.job_client import JobClient

pnconfig = PNConfiguration()

pnconfig.publish_key = 'pub-c-0f0864b8-4a7a-4059-89b9-1d083b503ca6'
pnconfig.subscribe_key = 'sub-c-73b0bad0-500e-11eb-a73a-1eec528e8f1f'
pnconfig.ssl = True

ID = "server_1"
candidates = {}
jobs = {}
BLOCL_SIZE = 8000 # bit

pubnub = PubNub(pnconfig)

def my_publish_callback(envelope, status):
    # Check whether request successfully completed or not
    if not status.is_error():
        pass
def send_files(pubnub, message, sent_file):
    # prepare file data, generate random strings, one block is 8K
    one_block = ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(BLOCL_SIZE)])
    count = 125
    i = 0
    # head contains one block, for response time testing
    cwd = os.getcwd()
    if not os.path.exists('storage'):
        os.makedirs('storage')
    store_path = cwd + "/storage"
    f_head = open(store_path + '/' + jobId + '_head.txt', 'w')
    f_head.write(one_block)
    f = open(store_path + '/' + jobId + '.txt', 'a+')
    while i < count:
        f.write(''.join([random.choice(string.ascii_letters + string.digits) for _ in range(BLOCL_SIZE)]) + '\n')
        i = i + 1
    
    f_name_head = store_path + '/' + jobId + '_head.txt'
    with open(f_name_head, 'rb') as fd:
        enve = pubnub.send_file().\
            channel("chan-message").\
            file_name(f_name_head).\
            message({"id": ID, "msg": sent_file}).\
            file_object(fd).sync()
        stat = getattr(enve, 'status')
        if getattr(stat, "status_code") == 204:
            result = getattr(enve, "result")
            sent_file["file_id"] = getattr(result, 'file_id')
            pubnub.publish().channel("chan-message").message({"id": ID, "msg": sent_file}).sync() 
    
    f_name = store_path + '/' + jobId + '.txt'
    with open(f_name_head, 'rb') as fd:
        enve = pubnub.send_file().\
            channel("chan-message").\
            file_name(f_name).\
            message({"id": ID, "msg": sent_file}).\
            file_object(fd).sync()
        stat = getattr(enve, 'status')
        # print("sent file: ", vars(stat))
        if getattr(stat, "status_code") == 204:
            result = getattr(enve, "result")
            sent_file["file_id"] = getattr(result, 'file_id')
            pubnub.publish().channel("chan-message").message({"id": ID, "msg": sent_file}).sync() 

class MySubscribeCallback(SubscribeCallback):
    def presence(self, pubnub, event):
        print(event.uuid)
        # pass
        # print presence
    def status(self, pubnub, status):
        pass
        # print status
    def message(self, pubnub, message):
        # servers, except publisher, respond the request
        if message.message["id"] != ID and message.message["msg"]["type"] == "pub":
            print("other servers receive publish: ",message.message["msg"])
            publisherId = message.message["msg"]["publisher"]
            jobId =  message.message["msg"]["jobId"]
            data_size = message.message["msg"]["data_size"]
            duration = message.message["msg"]["duration"]
            base_rewards = message.message["msg"]["base_rewards"]
            req_time = message.message["msg"]["req_time_stamp"]

            res = {
                "jobId": jobId,
                "candidate": ID,
                "type": "res",
                "des": publisherId,
                "guaranteed_rt": random.randint(15, 50),
                "data_size": data_size,
                "duration": duration,
                "base_rewards": base_rewards,
                "req_time": req_time
            }
            pubnub.publish().channel("chan-message").message({"id": ID,"msg":res}).pn_async(my_publish_callback)
            
        # publisher receive responses, other servers should not take this message
        elif message.message["msg"]["type"] == "res" and message.message["msg"]["des"] == ID: 
            # publisher start to choose receiver
            candidateId = message.message["msg"]["candidate"]
            guaranteed_rt = message.message["msg"]["guaranteed_rt"]
            candidates[candidateId] = guaranteed_rt
            receive_time = time.time()*1000
            print("candidates: ", candidates)
            print("receive time: ", receive_time - message.message["msg"]["req_time"])
            if len(candidates) >= 3:
                # initilize job_client instance
                job_client = JobClient(base_url='http://127.0.0.1:8008', keyfile=None)
                # choose receiver from candidates (id, guaranteed_rt)
                receiver = job_client.chooseReceiver(candidates)
                print("decide receiver: ",receiver)
                
                jobId = message.message["msg"]["jobId"]
                publisherId = ID
                receiverId = receiver[0]
                guaranteed_rt = receiver[1]
                data_size = message.message["msg"]["data_size"]
                start_time = time.time()*1000
                expire_time = start_time + float(message.message["msg"]["duration"])
                req_time = message.message["msg"]["req_time"]
                
                # send files to the receiver
                sent_file_prop = {
                    "type": "send_file",
                    "jobId": jobId,
                    "receiverId": receiverId,
                    "publisherId": publisherId,
                    "data_size": data_size,
                    "start_time": start_time,
                    "expire_time": expire_time,
                    "guaranteed_rt": guaranteed_rt,
                    "req_time": req_time
                }

                send_files(pubnub, message, sent_file_prop)

                dec = {
                    "type": "dec",
                    "jobId": jobId,
                    "receiverId": receiverId,
                    "publisherId": publisherId,
                    "data_size": data_size,
                    "start_time": start_time,
                    "expire_time": expire_time,
                    "guaranteed_rt": guaranteed_rt,
                }

                # ###### generate Tag for integrity validation

                # notify others to validate data and response time from receiver 
                pubnub.publish().channel("chan-message").message({"id": ID,"msg":dec}).pn_async(my_publish_callback)
        # other servers, except receiver, receive the publisher's decision
        elif message.message["msg"]["type"] == "dec" \
                and message.message["msg"]["publisherId"] != ID \
                and message.message["msg"]["receiverId"] != ID:
            
            print("notified: ",  message.message["msg"])
            # start to validate data
        elif message.message["msg"]["type"] == "send_file" and message.message["msg"]["receiverId"] == ID:
            print('file message: ', message.message["msg"])
            # receiver downloads file to store
            result = pubnub.list_files().channel("chan-message").sync()

            print("file: ", result)
            # for id, name, created in result.data:
            #     print(id + ',' + name + ',' + created)
            
            # download_envelope = pubnub.download_file().\
            #     channel("chan-message").\
            #     file_id(message.message["msg"]["jobId"]+'_head.txt').\
            #     file_name("knights_of_ni.jpg").sync()
            # receive one block, compute response time
            receive_head = time.time()*1000

pubnub.add_listener(MySubscribeCallback())
pubnub.subscribe().channels("chan-message").execute()

## publish a message
data_size, duration, base_rewards = input("Input a request info to publish separated by space <data_size duration base_rewards>: ").split()
jobId = str(uuid.uuid4().hex)
msg = {
    "publisher": ID,
    "type": "pub",
    "jobId": jobId,
    "data_size": data_size,
    "duration": duration,
    "base_rewards": base_rewards,
    "req_time_stamp": time.time()*1000
}
jobs[jobId] = msg
pubnub.publish().channel("chan-message").message({"id": ID,"msg":msg}).pn_async(my_publish_callback)