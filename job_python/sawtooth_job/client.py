from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
import time
import os
import random
import uuid
import sys

from sawtooth_job.job_client import JobClient

pnconfig = PNConfiguration()

pnconfig.publish_key = 'pub-c-0f0864b8-4a7a-4059-89b9-1d083b503ca6'
pnconfig.subscribe_key = 'sub-c-73b0bad0-500e-11eb-a73a-1eec528e8f1f'
pnconfig.ssl = True

ID = "server_1"
candidates = {}
jobs = {}

pubnub = PubNub(pnconfig)

def my_publish_callback(envelope, status):
    # Check whether request successfully completed or not
    if not status.is_error():
        pass

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
            publisherId = message.message["msg"]["publisher"]
            jobId =  message.message["msg"]["jobId"]
            data_size = message.message["msg"]["data_size"]
            duration = message.message["msg"]["duration"]
            base_rewards = message.message["msg"]["base_rewards"]
            res = {
                "jobId": jobId,
                "candidate": ID,
                "type": "res",
                "des": publisherId,
                "guaranteed_rt": random.randint(15, 50),
                "data_size": data_size,
                "duration": duration,
                "base_rewards": base_rewards
            }
            pubnub.publish().channel("chan-1").message({"id": ID,"msg":res}).pn_async(my_publish_callback)
            print(message.message["msg"])
        # publisher receive responses, other servers should not take this message
        elif message.message["msg"]["type"] == "res" and message.message["msg"]["des"] == ID: 
            # publisher start to choose receiver
            candidateId = message.message["msg"]["candidate"]
            guaranteed_rt = message.message["msg"]["guaranteed_rt"]
            candidates[candidateId] = guaranteed_rt
            print("candidates: ", candidates)
            if len(candidates) >= 3:
                # initilize job_client instance
                job_client = JobClient(base_url='http://127.0.0.1:8008', keyfile=None)
                # choose receiver from candidates
                receiver = job_client.chooseReceiver(candidates)
                print("decide receiver: ",receiver)
                # send files to the receiver
                #############
                ###############

                # notify others to validate data and response time from receiver
                jobId = message.message["msg"]["jobId"]
                publisherId = ID
                receiverId = receiver[0]
                guaranteed_rt = receiver[1]
                data_size = message.message["msg"]["data_size"]
                start_time = time.time()*1000
                expire_time = start_time + float(message.message["msg"]["duration"])

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
                # send 
                pubnub.publish().channel("chan-1").message({"id": ID,"msg":dec}).pn_async(my_publish_callback)
        # other servers, except receiver, receive the publisher's decision
        elif message.message["msg"]["type"] == "dec" \
                and message.message["msg"]["publisherId"] != ID \
                and message.message["msg"]["receiverId"] != ID:
            
            print("notified: ",  message.message["msg"])

pubnub.add_listener(MySubscribeCallback())
pubnub.subscribe().channels("chan-1").execute()

## publish a message
msg_type, data_size, duration, base_rewards = input("Input a request info to publish separated by space <type data_size duration base_rewards>: ").split()
jobId = str(uuid.uuid4().hex)
msg = {
    "publisher": ID,
    "type": msg_type,
    "jobId": jobId,
    "data_size": data_size,
    "duration": duration,
    "base_rewards": base_rewards
}
jobs[jobId] = msg
pubnub.publish().channel("chan-1").message({"id": ID,"msg":msg}).pn_async(my_publish_callback)