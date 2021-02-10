from sawtooth_job.client import publish_job, listen_and_sub

def run():
    listen_and_sub()
    print('start to publish')
    publish_job(1, 10, 20)

run()