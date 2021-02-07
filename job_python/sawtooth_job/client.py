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
import statistics

from sawtooth_job.job_client import JobClient
import integrity_validation

pnconfig = PNConfiguration()

pnconfig.publish_key = 'pub-c-0f0864b8-4a7a-4059-89b9-1d083b503ca6'
pnconfig.subscribe_key = 'sub-c-73b0bad0-500e-11eb-a73a-1eec528e8f1f'
pnconfig.ssl = True

ID = "server_1"
candidates = {}
jobs = {}
BLOCL_SIZE = 4000 # bit = 4K
DATASIZE = 0
HASHIS = {}

pubnub = PubNub(pnconfig)

def my_publish_callback(envelope, status):
    # Check whether request successfully completed or not
    if not status.is_error():
        pass

def get_folder_path(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    store_path = os.getcwd() + "/" + folder_name

def send_tags(pubnub, message, publisherId, receiverId):
    # generate tags for integrity validation
    tags = tagGen(ID, jobId)
    sent_tags = {
        "type": "tags",
        "jobId": jobId,
        "publisherId": publisherId,
        "receiverId": receiverId,
        "tags": ('|'.join(tags)).encode('utf-8')
    }
    # publisher store tag blocks locally
    store_path = get_folder_path('tagBlocks')
    with open(store_path + '/' + ID + '_' + jobId + '.txt', 'w+') as f:
        for tag in tags:
            f.write(tag+'\n')

    pubnub.publish().channel("chan-message").message({"id": ID, "msg": sent_tags}).pn_async(my_publish_callback)

'''
    send files to receiver
    send tags to all other servers (including receiver)
'''
def send_files(pubnub, message, sent_file_prop):
    jobId = sent_file_prop["jobId"]
    publisherId = sent_file_prop["publisherId"]
    receiverId = sent_file_prop["receiverId"]

    one_block = ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(BLOCL_SIZE)])
    count = 125
    i = 0
    # head contains one block, for response time testing
    sent_file_prop['file'] = one_block
    sent_file_prop['is_head'] = True

    store_path = get_folder_path('files')
    with open(store_path + '/' + ID + '_' + jobId + '.txt', 'a+') as f:
        f.write(one_block+'\n')
    pubnub.publish().channel("chan-message").message({"id": ID, "msg": sent_file_prop}).sync() 

    content = ''
    while i < count*int(DATASIZE):
        content = ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(BLOCL_SIZE)])
        sent_file_prop['is_head'] = False
        sent_file_prop['file'] = content
        store_path = get_folder_path('files')
        with open(store_path + '/' + ID + '_' + jobId + '.txt', 'a+') as f:
            f.write(content+'\n')
        pubnub.publish().channel("chan-message").message({"id": ID, "msg": sent_file_prop}).sync()
        i = i + 1
    
    # send tags to all other servers
    send_tags(pubnub, message, publisherId, receiverId)

def save_file(message):
    is_head = message.message["msg"]["is_head"]
    file_content = message.message["msg"]["file"]
    jobId = message.message["msg"]["jobId"]

    store_path = get_folder_path('storage')
    f = open(store_path + '/' + jobId + '.txt', 'a+')
    if is_head:
        f.write(file_content + '\n')
        receive_head = time.time()*1000
        req_time = message.message["msg"]["req_time"]
        save_rt(req_time, receive_head)
    elif not is_head:
        f.write(file_content + '\n')  
    f.close() 

'''
    save upload response time
'''
def save_rt(req_time, receive_head):
    store_path = get_folder_path('response_times')
    f = open(store_path + '/upload_' + ID + '.txt', 'a+')
    f.write(str(receive_head - req_time) + '\n')
    f.close()

def send_rt_validation(pubnub, message):
    guaranteed_rt = message.message["msg"]["guaranteed_rt"]
    jobId = message.message["msg"]["jobId"]
    receiverId = message.message["msg"]["receiverId"]
    start_time = message.message["msg"]["start_time"]

    validation_start_time = time.time()*1000

    val = {
            "type": "val",
            "jobId": jobId,
            "receiverId": receiverId,
            "start_time": start_time,
            "guaranteed_rt": guaranteed_rt,
            "validation_start_time": validation_start_time
        }

    # send validation message
    pubnub.publish().channel("chan-message").message({"id": ID,"msg":val}).pn_async(my_publish_callback)
    print('+++other servers sent validation messages++++')

'''
    send challenge to receiver
'''
def send_integrity_validation(pubnub, message):
    jobId = message.message["msg"]["jobId"]
    publisherId = message.message["msg"]["publisherId"]
    receiverId = message.message["msg"]["receiverId"]
    store_path = get_folder_path('tagBlocks')
    keys,hashis = genChallenge(store_path + '/' + publisherId + '_' + jobId+'.txt')

    HASHIS[jobId] = hashis

    in_val = {
        "type": "in_val",
        "jobId": jobId,
        "publisherId": publisherId,
        "receiverId": receiverId,
        "chal": ('|'.join(keys)).encode('utf-8')
    }
    # send chal message
    pubnub.publish().channel("chan-message").message({"id": ID,"msg":in_val}).pn_async(my_publish_callback)

def get_keyfile(username):
    home = os.path.expanduser("~")
    key_dir = os.path.join(home, ".sawtooth", "keys")
    return '{}/{}.priv'.format(key_dir, username)

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
            publisherId = message.message["msg"]["publisherId"]
            jobId =  message.message["msg"]["jobId"]
            data_size = message.message["msg"]["data_size"]
            duration = message.message["msg"]["duration"]
            base_rewards = message.message["msg"]["base_rewards"]
            pKey = message.message["msg"]["pKey"]
            sKey = message.message["msg"]["sKey"]
            req_time = message.message["msg"]["req_time_stamp"]

            store_path = get_folder_path('keys')
            with open (store_path + "/" + publisherId + "_private.pem", "w+") as prv_file:
                prv_file.write(sKey)
	
            with open (store_path + "/" + publisherId + "_public.pem", "w+") as pub_file:
                pub_file.write(pKey)
		        
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
                guaranteed_rt = candidates[receiverId]
                data_size = message.message["msg"]["data_size"]
                # the time decide receiver, as the start time of storage (roughly)
                start_time = time.time()*1000
                duration = message.message["msg"]["duration"]
                req_time = message.message["msg"]["req_time"]
                
                # reset candidates
                candidates.clear()
                
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
            duration = int(message.message["msg"]["duration"])
            '''
            ////// here, should be a random waiting time. each server get a random time to start validation  /////
            do it later
            '''
            # send files and receive files is sync, therefore, when other server receive notifications, all files are received
            wait_time = random.randrange(0, duration)
            print('------ wait ------', wait_time)
            time.sleep(wait_time)
            
            # asynchronous
            send_rt_validation(pubnub, message)

            # asynchronous
            send_integrity_validation(pubnub, message)
            
        # receiver save files
        elif message.message["msg"]["type"] == "send_file" and message.message["msg"]["receiverId"] == ID:
            #print('file message: ', message.message["msg"])
            s = time.time()*1000
            save_file(message)
            e = time.time()*1000
            print('++++save file cost: {} ms'.format(e - s))

        # receiver get validation message
        elif message.message["msg"]["type"] == "val" and message.message["msg"]["receiverId"] == ID:
            print('===== get validation message')
            jobId = message.message["msg"]["jobId"]
            start_time = message.message["msg"]["start_time"]
            guaranteed_rt = message.message["msg"]["guaranteed_rt"]
            validation_start_time = message.message["msg"]["validation_start_time"]
            file_name =  jobId + '.txt'

            if os.path.exists('storage'):
                print('===== start to return a block =====')
                store_path = os.getcwd() + "/storage"
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
                    "validation_start_time": validation_start_time
                }
                pubnub.publish().channel("chan-message").message({"id": ID,"msg":re_val}).pn_async(my_publish_callback)
            
        # validators receive results
        elif message.message["msg"]["type"] == "re_val" and message.message["msg"]["receiverId"] != ID:
            print('validators receive results')
            start_time = message.message["msg"]["start_time"]
            validation_start_time = message.message["msg"]["validation_start_time"]
            receive_response_time = time.time()*1000
            guaranteed_rt = float(message.message["msg"]["guaranteed_rt"])
            jobId = message.message["msg"]["jobId"]
            receiverId = message.message["msg"]["receiverId"]

            test_rt = float(receive_response_time) - float(validation_start_time)

            result_path = get_folder_path('test_results')
            f = open(result_path + '/rt_results.txt', 'a+')
            # if test_rt < guaranteed_rt:
            #     f.write(jobId + ',' + receiverId + ',' + str(1) + ',' + str(guaranteed_rt) + ',' + str(test_rt) + ',' + str(start_time) + '\n')
            # else:
            f.write(jobId + ',' + receiverId + ',' + str(guaranteed_rt) + ',' + str(test_rt) + ',' + str(start_time) + '\n')
            f.close()
        
        # receiver and validators receive tags and store
        elif message.message["msg"]["type"] == "tags" and message.message["msg"]["publisherId"] != ID:
            print('validators receive tags')
            jobId = message.message["msg"]["jobId"]
            publisherId = message.message["msg"]["publisherId"]
            tmp = message.message["msg"]["tags"]
            tags = (tmp.decode('utf-8')).split('|')
            store_path = get_folder_path('tagBlocks')
            with open(store_path + '/' + publisherId + '_' + jobId + '.txt') as f:
                for tag in tags:
                    f.write(tag + '\n')
        
        # receiver get chals
        elif message.message["msg"]["type"] == "in_val" and message.message["msg"]["receiverId"] == ID:
            print('receiver get chals')
            jobId = message.message["msg"]["jobId"]
            publisherId = message.message["msg"]["publisherId"]
            tmp = message.message["msg"]["chal"]
            chal = (tmp.decode('utf-8')).split('|')

            store_path = get_folder_path('storage')
            file_name = store_path + '/' + jobId + '.txt'
            proof = genProof(publisherId, file_name, chal)

            proof = {
                "type": "proof",
                "jobId": jobId,
                "publisherId": publisherId,
                "receiverId": ID,
                "proof": proof
            }
            # send proof to validators
            pubnub.publish().channel("chan-message").message({"id": ID,"msg":proof}).pn_async(my_publish_callback)

        # validators receive proof
        elif message.message["msg"]["type"] == "proof" and message.message["msg"]["receiverId"] != ID:
            print('validators get proof')
            publisherId = message.message["msg"]["publisherId"]
            receiverId = message.message["msg"]["receiverId"]
            jobId = message.message["msg"]["jobId"]
            proof = message.message["msg"]["jobId"]
            skey = loadPrvKey(publisherId)
            hashis = HASHIS[jobId]

            is_integrity = checkProof(proof, hashis, skey)
            result_path = get_folder_path('test_results')
            with open(result_path + '/integrity_results', 'a+') as f:
                f.write(jobId + ',' + receiverId + ',' + is_integrity)


pubnub.add_listener(MySubscribeCallback())
pubnub.subscribe().channels("chan-message").execute()

## publish a message
data_size, duration, base_rewards = input("Input a request info to publish separated by space <data_size(MB) duration(s) base_rewards>: ").split()
jobId = str(uuid.uuid4().hex)
DATASIZE = data_size
# generate public key and private key
pKey, sKey = keyGen(ID)

msg = {
    "publisherId": ID,
    "type": "pub",
    "jobId": jobId,
    "data_size": data_size,
    "duration": duration,
    "base_rewards": base_rewards,
    "pKey": pKey,
    "sKey": sKey,
    "req_time_stamp": time.time()*1000
}
jobs[jobId] = msg

pubnub.publish().channel("chan-message").message({"id": ID,"msg":msg}).pn_async(my_publish_callback)

'''
after duration time, propose a transaction
call job_client.create()
'''
print('--- wait for file expire ---')
time.sleep(float(duration)*10)
print('--- start to propose transaction ---')
result_path = get_folder_path('test_results')
# check weather has validated
if os.path.exists('test_results') and os.path.exists(result_path+'/rt_results.txt'):
    print('---- validation get results ----')
    with open(result_path + '/rt_results.txt') as f:
        print('--- file opened ---')
        guaranteed_rt = ''
        test_rts = []
        receiverId = ''
        start_time = ''
        is_integrity = ''
        while True:
            result = f.readline()
            l = result.split(',')
            if l[0] == msg["jobId"]:
                receiverId = l[1]
                guaranteed_rt = l[2]
                test_rts.append(float(l[3]))
                start_time = l[4]
                is_integrity = '1'
            elif not result:
                break
        # choose the median rt recorded transaction 
        test_rt = statistics.median(test_rts)
        print("test_rt: ", test_rt)
    keyfile = get_keyfile(ID)
    job_client = JobClient(base_url='http://127.0.0.1:8008', keyfile=keyfile)   
    job_client.create(jobId, receiverId, msg["publisherId"], data_size, start_time, duration, float(guaranteed_rt), float(test_rt), float(base_rewards), is_integrity)


