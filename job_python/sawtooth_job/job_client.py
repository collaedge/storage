# Copyright 2016 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------
from __future__ import division

import uuid
import hashlib
import base64
from base64 import b64encode
import time
import random
import requests
import yaml
import math
import sys
import logging

from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory
from sawtooth_signing import ParseError
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey

from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader
from sawtooth_sdk.protobuf.transaction_pb2 import Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchList
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader
from sawtooth_sdk.protobuf.batch_pb2 import Batch
from sawtooth_sdk.processor.exceptions import InternalError

from sawtooth_job.job_exceptions import JobException


def _sha512(data):
    return hashlib.sha512(data).hexdigest()

# constant, to calculate extra rewards
P = 10

class JobClient:
    

    def __init__(self, base_url, keyfile=None):

        self._base_url = base_url

        if keyfile is None:
            self._signer = None
            return

        try:
            with open(keyfile) as fd:
                private_key_str = fd.read().strip()
        except OSError as err:
            raise JobException(
                'Failed to read private key {}: {}'.format(keyfile, str(err)))

        try:
            private_key = Secp256k1PrivateKey.from_hex(private_key_str)
        except ParseError as e:
            raise JobException(
                'Unable to load private key: {}'.format(str(e)))

        self._signer = CryptoFactory(create_context('secp256k1')) \
            .new_signer(private_key)

    # propose a job record
    def create(self, receiverId, publisherId, data_size, start_time, expire_time, guaranteed_rt, test_rt, base_rewards, is_integrity, wait=None):
        jobId = str(uuid.uuid4())
        if test_rt < guaranteed_rt:
            extra_rewards = round(P*(guaranteed_rt - test_rt) / guaranteed_rt, 1)
        elif not is_integrity:
            extra_rewards = 0
            base_rewards = 0
        else:
            extra_rewards = 0
            base_rewards = 0.5*base_rewards
        return self._send_transaction(
            jobId,
            receiverId,
            publisherId,
            str(data_size),
            str(start_time),
            str(expire_time),
            str(guaranteed_rt),
            str(test_rt),
            str(base_rewards),
            str(extra_rewards),
            is_integrity,
            "create",
            wait=wait,
            )
    """
    # get job response as input, choose a receiver
    def chooseReceiver(self, receiver1, receiver2, receiver3, receiver4, receiver5, receiver6, receiver7):
        # get receivers response, pick finish time as a param
        receivers_id = []
        # the time receivers finish the job
        guaranteed_rts = {}
        if receiver1 is not None:
            receiverId, publisherId, start_time, guaranteed_rt = receiver1.split(',')
            receivers_id.append(receiverId)
            guaranteed_rts[receiverId] = float(guaranteed_rt)

        if receiver2 is not None:
            receiverId, publisherId, start_time, guaranteed_rt = receiver2.split(',')
            receivers_id.append(receiverId)
            guaranteed_rts[receiverId] = float(guaranteed_rt)

        if receiver3 is not None:
            receiverId, publisherId, start_time, guaranteed_rt = receiver3.split(',')
            receivers_id.append(receiverId)
            guaranteed_rts[receiverId] = float(guaranteed_rt)

        if receiver4 is not None:
            receiverId, publisherId, start_time, guaranteed_rt = receiver4.split(',')
            receivers_id.append(receiverId)
            guaranteed_rts[receiverId] = float(guaranteed_rt)
        
        if receiver5 is not None:
            receiverId, publisherId, start_time, guaranteed_rt = receiver5.split(',')
            receivers_id.append(receiverId)
            guaranteed_rts[receiverId] = float(guaranteed_rt)

        if receiver6 is not None:
            receiverId, publisherId, start_time, guaranteed_rt = receiver6.split(',')
            receivers_id.append(receiverId)
            guaranteed_rts[receiverId] = float(guaranteed_rt)

        if receiver7 is not None:
            receiverId, publisherId, start_time, guaranteed_rt = receiver7.split(',')
            receivers_id.append(receiverId)
            guaranteed_rts[receiverId] = float(guaranteed_rt)

        # get reputation of receivers
        repus = self.computeReputation(receivers_id)
        print('++++ reputation +++++')
        print(repus)
        
        # print('++++ receiver_delays +++++')
        # print(receiver_delays)

        # print('++++ working_time +++++')
        # print(working_time)

        normalized_guaranteed_rt = self.normalization(guaranteed_rts)
        normalized_repus = self.normalization(repus)
        # print('++++ normalized_working_time +++++')
        # print(normalized_working_time)
        # print('++++ normalized_delay +++++')
        # print(normalized_delay)
        # print('++++ normalized_repus +++++')
        # print(normalized_repus)

        # compute scores for receivers, choose the best
        # call create function with parms
        return self.chooseOne(receivers_id, normalized_guaranteed_rt, normalized_repus)
    """

    # get job response as input, choose a receiver
    # candidates format:
    # [receiverId,publisherId,start_time,guaranteed_rt], an array
    def chooseReceiver(self, candidates):
        # get receivers response, pick start time as a param
        receivers_id = []
        # store all servers' guaranteed response time
        guaranteed_rts = {}
        
        for candidate in candidates:
            receiverId, publisherId, start_time, guaranteed_rt = candidate.split(',')
            receivers_id.append(receiverId)
            guaranteed_rts[receiverId] = float(guaranteed_rt)
            
        # get reputation of receivers
        repus = self.computeReputation(receivers_id)
        print('++++ reputation +++++')
        print(repus)
        
        # print('++++ receiver_delays +++++')
        # print(receiver_delays)

        # print('++++ working_time +++++')
        # print(working_time)

        normalized_guaranteed_rt = self.normalization(guaranteed_rts)
        normalized_repus = self.normalization(repus)
        # print('++++ normalized_working_time +++++')
        # print(normalized_working_time)
        # print('++++ normalized_delay +++++')
        # print(normalized_delay)
        # print('++++ normalized_repus +++++')
        # print(normalized_repus)

        # compute scores for receivers, choose the best
        # call create function with parms
        return self.chooseOne(receivers_id, normalized_guaranteed_rt, normalized_repus)

    def chooseOne(self, receivers, reputation, guaranteed_rt):
        guaranteed_rt_weight = 0.3
        reputation_weight = 0.7

        combine = {}
        for receiverId in receivers:
            combine[receiverId] = reputation_weight*reputation[receiverId] 
            + guaranteed_rt_weight*guaranteed_rt[receiverId]
        print('++++ choose one combine +++++')
        print(combine)
        s = sorted(combine.items(), key=lambda x: x[1],reverse = True)

        return s[0]


    def normalization(self, data):
        sorted_data = sorted(data.items(), key=lambda x: x[1])
        max = sorted_data[len(data)-1][1]
        min = sorted_data[0][1]
        normalized = {}
        for key in data.keys():
            if max == min:
                normalized[key] = 1
            else:
                normalized[key] = (data[key] - min) / (max - min)
        return normalized

    def computeReputation(self, receiverIds):
        # current time in millisecond
        # current_time = time.time()
        # current_time = 1593871200000

        # store each node's available rewards, used for validation phase
        print('++++++++create log file++++++++')
        logger = logging.getLogger()
        hdlr = logging.FileHandler('/home/ubuntu/reputation.log')
        logger.addHandler(hdlr) 
        logger.setLevel(logging.INFO)

        # get all job from chain
        job_list = [
            job.split(',')
            for jobs in self.list()
            for job in jobs.decode().split('|')
        ]

        # construct job record for reputation computation
        # required: start_time 
        #           extra_rewards 
        job_record = {}
        jobs = []
        if job_list is not None:
            for job_data in job_list:
                jobId, receiverId, publisherId, data_size, start_time, expire_time, guaranteed_rt, test_rt, base_rewards, extra_rewards, is_integrity = job_data
                # store jobs according to receiverId
                job_record.setdefault(receiverId, []).append({
                    'start_time': start_time,
                    'extra_rewards': extra_rewards
                })
                jobs.append({
                    'receiverId': receiverId,
                    'publisherId': publisherId,
                    'base_rewards': float(base_rewards),
                    'extra_rewards': float(extra_rewards)
                })
        else:
            raise JobException("Could not retrieve game listing.")

        print('++++++++job record in chain++++++++')
        print(job_record)
        
        # based on extra rewards, reflecting histroy performance
        # only compute for who has expressed interests
        reputation_receivers = self.computeBasedOnRewards(receiverIds, job_record)
        print('++++++++ reputation_receivers n++++++++')
        print(reputation_receivers)

        recvBaseRewards = {}
        recvExtraRewards = {}
        # initialize 
        for job in jobs:
            recvBaseRewards[job['receiverId']] = 0
            recvExtraRewards[job['receiverId']] = 0

        for job in jobs:
            recvBaseRewards[job['receiverId']] += job['base_rewards']
            recvExtraRewards[job['receiverId']] += job['extra_rewards']

        for receiverId in receiverIds:
            info = receiverId + ' - ' +str(recvBaseRewards[receiverId]) + ' - ' + str(recvExtraRewards[receiverId]) + ' - ' + str(round(reputation_receivers[receiverId], 3))
            print('++++++++write log++++++++')
            print(info)
            logger.info(info)

        return reputation_receivers

    def computeBasedOnRewards(self, receiverIds, job_record):
        B = 0.2
        reward_score = {}
        rewards = {}
        RECENT_JOB_NUM = 20
        print('+++++ unsorted print(job_record) ++++')
        print(job_record)
        # sort by start time
        for receiverId, records in job_record.items():
            job_record[receiverId] = sorted(records, key=lambda x: x['start_time'], reverse=True)
        print('+++++ sorted print(job_record) ++++')
        print(job_record)
        # extra rewards on each record of receivers on recent X number of jobs
        i = 0
        for receiverId, records in job_record.items():
            if receiverId in receiverIds:
                for record in records:
                    if i<RECENT_JOB_NUM:
                        rewards.setdefault(receiverId, []).append({
                            'start_time': float(record['start_time']),
                            'extra_rewards': float(record['extra_rewards'])
                        })
                    i = i+1
        
        print('+++++ sorted print(reward_score) ++++')
        print(rewards)
        
        for receiverId, records in rewards.items():
            score = 0
            for record in records:
                score = B*record['extra_rewards'] + (1-B)*score 
            reward_score[receiverId] = score

        return reward_score

    def getHihgestReceiver(self, receivers):
        print('====receivers: ')
        print(receivers)
        # get reputation of workers
        repus = self.computeReputation(receivers)
        print('++++ reputation +++++')
        print(repus)


        sorted_repu = sorted(repus.items(), key=lambda x: x[1],reverse = True)
        print('++++ sorted_repu +++++')
        print(sorted_repu[0])
        return sorted_repu[0]

    def getJob(self, jobId, space, wait=None):
        return self._send_transaction(
            jobId=jobId,
            action="get",
            wait=wait)

    def list(self):
        prefix = self._get_prefix()

        result = self._send_request(
            "state?address={}".format(prefix),
            )

        try:
            encoded_entries = yaml.safe_load(result)["data"]

            return [
                base64.b64decode(entry["data"]) for entry in encoded_entries
            ]

        except BaseException:
            return None

    def show(self, jobId):
        address = self._get_address(jobId)

        result = self._send_request(
            "state/{}".format(address),
            jobId=jobId,
            )
        try:
            return base64.b64decode(yaml.safe_load(result)["data"])

        except BaseException:
            return None

    def _get_status(self, batch_id, wait):
        try:
            result = self._send_request(
                'batch_statuses?id={}&wait={}'.format(batch_id, wait),
                )
            return yaml.safe_load(result)['data'][0]['status']
        except BaseException as err:
            raise JobException(err)

    def _get_prefix(self):
        return _sha512('job'.encode('utf-8'))[0:6]

    def _get_address(self, jobId):
        prefix = self._get_prefix()
        job_address = _sha512(jobId.encode('utf-8'))[0:64]
        return prefix + job_address

    def _send_request(self,
                      suffix,
                      data=None,
                      content_type=None,
                      jobId=None):
        if self._base_url.startswith("http://"):
            url = "{}/{}".format(self._base_url, suffix)
        else:
            url = "http://{}/{}".format(self._base_url, suffix)

        headers = {}

        if content_type is not None:
            headers['Content-Type'] = content_type

        try:
            if data is not None:
                result = requests.post(url, headers=headers, data=data)
            else:
                result = requests.get(url, headers=headers)

            if result.status_code == 404:
                raise JobException("No such job: {}".format(jobId))

            if not result.ok:
                raise JobException("Error {}: {}".format(
                    result.status_code, result.reason))

        except requests.ConnectionError as err:
            raise JobException(
                'Failed to connect to {}: {}'.format(url, str(err)))

        except BaseException as err:
            raise JobException(err)

        return result.text

    def _send_transaction(self,
                    jobId,
                    receiverId,
                    publisherId,
                    data_size,
                    start_time,
                    expire_time,
                    guaranteed_rt,
                    test_rt,
                    base_rewards,
                    extra_rewards,
                    is_integrity,
                    action,
                    wait=None):
        # Serialization is just a delimited utf-8 encoded string
        payload = ",".join([jobId, receiverId, publisherId, data_size, start_time, expire_time,
                            guaranteed_rt, test_rt, base_rewards, extra_rewards, is_integrity, action]).encode()

        print('client payload: ')
        print(payload)

        # Construct the address
        address = self._get_address(jobId)

        header = TransactionHeader(
            signer_public_key=self._signer.get_public_key().as_hex(),
            family_name="job",
            family_version="1.0",
            inputs=[address],
            outputs=[address],
            dependencies=[],
            payload_sha512=_sha512(payload),
            batcher_public_key=self._signer.get_public_key().as_hex(),
            nonce=hex(random.randint(0, 2**64))
        ).SerializeToString()

        signature = self._signer.sign(header)

        transaction = Transaction(
            header=header,
            payload=payload,
            header_signature=signature
        )

        batch_list = self._create_batch_list([transaction])
        batch_id = batch_list.batches[0].header_signature

        if wait and wait > 0:
            wait_time = 0
            begin_time = time.time()
            response = self._send_request(
                "batches", batch_list.SerializeToString(),
                'application/octet-stream'
                )
            while wait_time < wait:
                status = self._get_status(
                    batch_id,
                    wait - int(wait_time)
                    )
                wait_time = time.time() - begin_time

                if status != 'PENDING':
                    return response

            return response

        return self._send_request(
            "batches", batch_list.SerializeToString(),
            'application/octet-stream',
            )

    def _create_batch_list(self, transactions):
        transaction_signatures = [t.header_signature for t in transactions]

        header = BatchHeader(
            signer_public_key=self._signer.get_public_key().as_hex(),
            transaction_ids=transaction_signatures
        ).SerializeToString()

        signature = self._signer.sign(header)

        batch = Batch(
            header=header,
            transactions=transactions,
            header_signature=signature)
        return BatchList(batches=[batch])
