import signal, os  
import sys
import time
import Crypto
from Crypto.Signature import PKCS1_PSS
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto import Random
import numpy as np
import math
from random import randint, randrange
import random
from Crypto.Cipher import PKCS1_OAEP
from base64 import b64decode,b64encode

'''
    data publisher generates public key and private key
'''
def keyGen(id):
    RSAkeyPair = RSA.generate(2048,randfunc=None, e = 5)
    PubKey = RSAkeyPair.publickey()
    if not os.path.exists('keys'):
        os.makedirs('keys')
    store_path = os.getcwd() + "/keys"
    with open (store_path + "/" + id + "_private.pem", "w") as prv_file:
        print("{}".format(RSAkeyPair.exportKey().decode("utf-8")), file=prv_file)
	
    with open (store_path + "/" + id + "_public.pem", "w") as pub_file:
        print("{}".format(PubKey.exportKey().decode("utf-8")), file=pub_file)
    
    return PubKey, RSAkeyPair

def loadPrvKey(id):
    skey = ''
    store_path = os.getcwd() + "/keys"
    with open (store_path + "/" + id + "_private.pem", "r") as prv_file:
        key = prv_file.read()
        skey =RSA.importKey(key)
    return skey

def loadPubKey(id):
    pkey = ''
    store_path = os.getcwd() + "/keys"
    with open (store_path + "/" + id + "_public.pem", "r") as pub_file:
        key = pub_file.read()
        pkey =RSA.importKey(key)
    return pkey

'''
    get parts of data to be validated
'''
def getsample(data, num = None): 
    index = 0
    blocks = {}
    if num is None or num > len(data):
        for block in data:
            blocks[index] = block
            index = index + 1
	# else:
    #     result_List = random.sample(range(len(data)+1),num)
    #     for i in result_List:
    #         blocks[i] = data[i]

    return blocks

'''
    data publisher generates tags for each block
'''
def tagGen(serverId, jobId):
    data = []
    with open(os.getcwd() + "/files/" + serverId + '_' + jobId + '.txt','rb') as file:
        data = file.readlines()
		
    blocks = getsample(data)
    tags = []
    for key, value in blocks.items():
        hashi = SHA256.new(value).hexdigest()
        tags.append(str(key)+','+hashi)
    return tags

'''
    validators generate challenges
'''    
def genChallenge(file_name):
    with open(file_name, 'r') as f:
	    blocks = f.readlines()
    N = len(blocks)
    k = N//4
    left = int(math.log(N,2))
    right = left*10
    if left >= k:
        m = randrange(1,k+1)
    elif k > right:
        m = randrange(left,right)
    else:
        m = randrange(left,k+1)
    samples = random.sample(blocks,m)
	
    hashis = []
    keys = []
    for s in samples:
        key,value = s.split(',')
        keys.append(key)
        hashis.append(value)
    
    return keys, hashis

'''
    data receiver generate proof
'''
def genProof(id, file_name, tag_keys):
    pkey = loadPubKey(id)
    with open(file_name,'r') as file:
	    data = file.readlines()
	
    results = ""
    for key in tag_keys:
        block = data[int(key)]
        hashi = SHA256.new(block.encode('utf-8')).hexdigest()
        results += hashi
    
    H = SHA256.new(results.encode('utf-8')).hexdigest()
    cipher = PKCS1_OAEP.new(pkey)
    cipher_text = b64encode(cipher.encrypt(H.encode()))

    return cipher_text.decode('utf-8')

'''
    validators check proof
'''
def checkProof(proof, hashis, skey):
	#decrypt
	cipher = PKCS1_OAEP.new(skey)
	V = cipher.decrypt(b64decode(proof),None)
	
	#build checkmodel from hashis
	results=""
	for hashi in hashis:
		results += hashi.replace('\n','')
	#hash results
	p = SHA256.new(results.encode('utf-8')).hexdigest().encode('utf-8')
	
	#check validate
	if V == p:
		return "1"
	return "0"