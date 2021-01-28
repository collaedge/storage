# 
# -----------------------------------------------------------------------------

from sawtooth_sdk.processor.exceptions import InvalidTransaction


class JobPayload:

    def __init__(self, payload):
        print('+++payload: ')
        print(payload)
        try:
            # load payload
            jobId, receiverId, publisherId, data_size, start_time, expire_time, guaranteed_rt, test_rt, base_rewards, extra_rewards, is_integrity, action = payload.decode().split(",")
        except ValueError:
            raise InvalidTransaction("Invalid payload serialization")
        if not jobId:
            raise InvalidTransaction('jobId is required')

        if not receiverId:
            raise InvalidTransaction('receiverId is required')

        if not publisherId:
            raise InvalidTransaction('publisherId is required')
        
        if not data_size:
            raise InvalidTransaction('data_size is required')

        if not start_time:
            raise InvalidTransaction('start_time is required')

        if not expire_time:
            raise InvalidTransaction('expire_time is required')

        if not guaranteed_rt:
            raise InvalidTransaction('guaranteed_rt is required')

        if not test_rt:
            raise InvalidTransaction('test_rt is required')

        if not base_rewards:
            raise InvalidTransaction('base_rewards is required')

        if not extra_rewards:
            raise InvalidTransaction('extra_rewards is required')

        if not is_integrity:
            raise InvalidTransaction('is_integrity is required')

        if not action:
            raise InvalidTransaction('Action is required')

        if action not in ('create', 'ggetByIdet', 'getByWorker'):
            raise InvalidTransaction('Invalid action: {}'.format(action))

        self._jobId = jobId
        self._receiverId = receiverId
        self._publisherId = publisherId
        self._data_size = data_size
        self._start_time = start_time
        self._expire_time = expire_time
        self._guaranteed_rt = guaranteed_rt
        self._test_rt = test_rt
        self._base_rewards = base_rewards
        self._extra_rewards = extra_rewards
        self._is_integrity = is_integrity
        self._action = action

    @staticmethod
    def load_job(payload):
        return JobPayload(payload=payload)

    @property
    def jobId(self):
        return self._jobId

    @property
    def receiverId(self):
        return self._receiverId

    @property
    def publisherId(self):
        return self._publisherId

    @property
    def data_size(self):
        return self._data_size

    @property
    def start_time(self):
        return self._start_time

    @property
    def _expire_time(self):
        return self._expire_time
    
    @property
    def guaranteed_rt(self):
        return self._guaranteed_rt

    @property
    def test_rt(self):
        return self._test_rt

    @property
    def base_rewards(self):
        return self._base_rewards

    @property
    def extra_rewards(self):
        return self._extra_rewards

    @property
    def is_integrity(self):
        return self._is_integrity

    @property
    def action(self):
        return self._action

    
