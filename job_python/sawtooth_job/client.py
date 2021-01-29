from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
import time
import os
import random

pnconfig = PNConfiguration()

pnconfig.publish_key = 'pub-c-0f0864b8-4a7a-4059-89b9-1d083b503ca6'
pnconfig.subscribe_key = 'sub-c-73b0bad0-500e-11eb-a73a-1eec528e8f1f'
pnconfig.ssl = True

ID = "edge_server_1"
candidates = {}

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
            print("publisher ID: ", publisherId)
            res = {
                "candidate": ID,
                "type": "res",
                "des": publisherId,
                "guaranteed_rt": random.randint(15, 50),
            }
            pubnub.publish().channel("chan-1").message({"id": ID,"msg":res}).pn_async(my_publish_callback)
            print(message.message["msg"])
        # publisher receive responses, other servers should not take this message
        elif message.message["msg"]["type"] == "res" and message.message["msg"]["des"] == ID: 
            #publisher start to choose receiver
            candidateId = message.message["msg"]["candidate"]
            guaranteed_rt = message.message["msg"]["guaranteed_rt"]
            candidates[candidateId] = guaranteed_rt
            print(candidates)

pubnub.add_listener(MySubscribeCallback())
pubnub.subscribe().channels("chan-1").execute()

## publish a message
while True:
    msg_type, data_size, duration, base_rewards = input("Input a request info to publish separated by space <type data_size duration base_rewards>: ").split()
    msg = {
        "publisher": ID,
        "type": msg_type,
        "data_size": data_size,
        "duration": duration,
        "base_rewards": base_rewards
    }
    if msg == 'exit': os._exit(1)
    pubnub.publish().channel("chan-1").message({"id": ID,"msg":msg}).pn_async(my_publish_callback)