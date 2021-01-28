from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
import time
import os

pnconfig = PNConfiguration()

pnconfig.publish_key = 'pub-c-0f0864b8-4a7a-4059-89b9-1d083b503ca6'
pnconfig.subscribe_key = 'sub-c-73b0bad0-500e-11eb-a73a-1eec528e8f1f'
pnconfig.ssl = True

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
    	if message.message["id"] != "client1":
        	print(message.message["text"])

pubnub.add_listener(MySubscribeCallback())
pubnub.subscribe().channels("chan-1").execute()

## publish a message
while True:
    msg = raw_input("Input a message to publish: ")
    if msg == 'exit': os._exit(1)
    pubnub.publish().channel("chan-1").message({"id": "client1","text":msg}).pn_async(my_publish_callback)