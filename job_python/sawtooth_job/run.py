from sawtooth_job.client import publish_job

def run():
    print('publish')
    publish_job(1, 10, 20)

print('publish')
run()