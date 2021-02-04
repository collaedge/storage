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
    one_block = ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(BLOCL_SIZE)])
    count = 125
    i = 0

    files = []
    files.append(one_block)
    # head contains one block, for response time testing
    sent_file['file'] = one_block
    sent_file['is_head'] = True
    pubnub.publish().channel("chan-message").message({"id": ID, "msg": sent_file}).sync() 

    content = ''
    while i < count:
        content = ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(BLOCL_SIZE)])
        files.append(content)
        sent_file['is_head'] = False
        sent_file['file'] = content
        pubnub.publish().channel("chan-message").message({"id": ID, "msg": sent_file}).sync()
        i = i + 1

    # ###### generate Tag for integrity validation

def save_file(message):
    is_head = message.message["msg"]["is_head"]
    file_content = message.message["msg"]["file"]
    jobId = message.message["msg"]["jobId"]
    cwd = os.getcwd()
    if not os.path.exists('storage'):
        os.makedirs('storage')
    store_path = cwd + "/storage"
    f = open(store_path + '/' + jobId + '.txt', 'a+')
    if is_head:
        f.write(file_content + '\n')
        receive_head = time.time()*1000
        req_time = message.message["msg"]["req_time"]
        save_rt(cwd, req_time, receive_head)
    elif not is_head:
        f.write(file_content + '\n')  
    f.close() 

def save_rt(cwd, req_time, receive_head):
    if not os.path.exists('response_times'):
        os.makedirs('response_times')
    store_path = cwd + "/response_times"
    f = open(store_path + '/upload_' + ID + '.txt', 'a+')
    f.write(str(receive_head - req_time) + '\n')
    f.close()

'''
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
            file_object(fd).\
            sync()
            # cipher_key("secret").
        stat = getattr(enve, 'status')
        if getattr(stat, "status_code") == 204:
            result = getattr(enve, "result")
            sent_file["file_id"] = getattr(result, 'file_id')
            sent_file["file_name"] = jobId + '_head.txt'
            pubnub.publish().channel("chan-message").message({"id": ID, "msg": sent_file}).sync() 
    
    f_name = store_path + '/' + jobId + '.txt'
    with open(f_name_head, 'rb') as fd:
        enve = pubnub.send_file().\
            channel("chan-message").\
            file_name(f_name).\
            message({"id": ID, "msg": sent_file}).\
            file_object(fd).\
            sync()
            # cipher_key("secret").
        stat = getattr(enve, 'status')
        # print("sent file: ", vars(stat))
        if getattr(stat, "status_code") == 204:
            result = getattr(enve, "result")
            sent_file["file_id"] = getattr(result, 'file_id')
            sent_file["file_name"] = jobId + '.txt'
            pubnub.publish().channel("chan-message").message({"id": ID, "msg": sent_file}).sync() 
'''

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
                duration = message.message["msg"]["duration"]
                req_time = message.message["msg"]["req_time"]
                
                # send files to the receiver
                sent_file_prop = {
                    "type": "send_file",
                    "jobId": jobId,
                    "receiverId": receiverId,
                    "publisherId": publisherId,
                    "data_size": data_size,
                    "start_time": start_time,
                    "duration": duration,
                    "guaranteed_rt": guaranteed_rt,
                    "req_time": req_time
                }

                send_files(pubnub, message, sent_file_prop)

                print('sent files')
                '''
                generate a group of random number to indicate each server pick a time to start validation process
                do it later
                '''
                dec = {
                    "type": "dec",
                    "jobId": jobId,
                    "receiverId": receiverId,
                    "publisherId": publisherId,
                    "data_size": data_size,
                    "start_time": start_time,
                    "duration": duration,
                    "guaranteed_rt": guaranteed_rt,
                }
               
                # notify others to validate data and response time from receiver 
                pubnub.publish().channel("chan-message").message({"id": ID,"msg":dec}).pn_async(my_publish_callback)
                print('publisher sent decision message')
        # other servers, except receiver, receive the publisher's decision
        elif message.message["msg"]["type"] == "dec" \
                and message.message["msg"]["receiverId"] != ID:
                #and message.message["msg"]["publisherId"] != ID \
            
            print("notified: ",  message.message["msg"])
            '''
            ////// here, should be a random waiting time. each server get a random time to start validation  /////
            do it later
            '''
            wait_time = random.randrange(5, 20)
            time.sleep(wait_time)
            guaranteed_rt = message.message["msg"]["guaranteed_rt"]
            jobId = message.message["msg"]["jobId"]
            receiverId = message.message["msg"]["receiverId"]
            start_time = message.message["msg"]["start_time"]

            start_validation_time = time.time()*1000

            val = {
                    "type": "val",
                    "jobId": jobId,
                    "receiverId": receiverId,
                    "start_time": start_time,
                    "guaranteed_rt": guaranteed_rt,
                    "start_validation_time": start_validation_time
                }

            # send validation message
            pubnub.publish().channel("chan-message").message({"id": ID,"msg":val}).pn_async(my_publish_callback)
            print('+++other servers sent validation messages++++')

        elif message.message["msg"]["type"] == "send_file" and message.message["msg"]["receiverId"] == ID:
            #print('file message: ', message.message["msg"])
            s = time.time()*1000
            save_file(message)
            e = time.time()*1000
            print('++++save file cost: ', e - s)
        # receiver get validation message
        elif message.message["msg"]["type"] == "val" and message.message["msg"]["receiverId"] == ID:
            print('===== get validation message')
            jobId = message.message["msg"]["jobId"]
            start_time = message.message["msg"]["start_time"]
            guaranteed_rt = message.message["msg"]["guaranteed_rt"]
            start_validation_time = message.message["msg"]["start_validation_time"]
            file_name =  jobId + '.txt'
            cwd = os.getcwd()
            if os.path.exists('storage'):
                print('===== start to return a block =====')
                store_path = cwd + "/storage"
                f = open(store_path + '/' + file_name, 'r')
                # return first block
                one_block = f.readline()
                f.close()
                re_val = {
                    "type": "re_val",
                    "jobId": jobId,
                    "receiverId": ID,
                    "start_time": start_time,
                    "one_block": one_block,
                    "guaranteed_rt": guaranteed_rt,
                    "start_validation_time": start_validation_time
                }
                pubnub.publish().channel("chan-message").message({"id": ID,"msg":re_val}).pn_async(my_publish_callback)
            
        # validators receive results
        elif message.message["msg"]["type"] == "re_val" and message.message["msg"]["receiverId"] != ID:
            print('validators receive results')
            start_time = message.message["msg"]["start_time"]
            start_validation_time = message.message["msg"]["start_validation_time"]
            receive_response_time = time.time()*1000
            guaranteed_rt = float(message.message["msg"]["guaranteed_rt"])
            jobId = message.message["msg"]["jobId"]
            receiverId = message.message["msg"]["receiverId"]

            test_rt = float(receive_response_time) - float(start_validation_time)

            if not os.path.exists('test_results'):
                os.makedirs('test_results')
            result_path = os.getcwd() + "/test_results"
            f = open(result_path + '/rt_results.txt', 'a+')
            if test_rt < guaranteed_rt:
                f.write(jobId + ',' + receiverId + ',' + str(1) + ',' + str(guaranteed_rt) + ',' + str(test_rt) + ',' + start_time + '\n')
            else:
                f.write(jobId + ',' + receiverId + ',' + str(0) + ',' + str(guaranteed_rt) + ',' + str(test_rt) + ',' + start_time + '\n')
            f.close()

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

'''
after duration time, propose a transaction
call job_client.create()
'''
time.sleep(float(duration)*2)
result_path = os.getcwd() + "/test_results"
# check weather has validated
if os.path.exists('test_results') and os.path.exists(result_path+'/rt_results.txt'):
    with open(result_path + '/rt_results.txt') as f:
        guaranteed_rt = ''
        test_rt = ''
        receiverId = ''
        start_time = ''
        is_integrity = ''
        while True:
            result = f.readline()
            l = result.split(',')
            if l[0] == msg["jobId"]:
                receiverId = l[1]
                guaranteed_rt = l[3]
                test_rt = l[4]
                start_time = l[5]
                is_integrity = l[2]
                break
            elif not l:
                break
    job_client = JobClient(base_url='http://127.0.0.1:8008', keyfile=None)   
    job_client.create(jobId, receiverId, msg["publisherId"], data_size, start_time, duration, guaranteed_rt, test_rt, base_rewards, is_integrity)



