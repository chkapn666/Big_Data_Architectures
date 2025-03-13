from celery import Celery
from redis import Redis

app = Celery('my_app2', broker='amqp://localhost', backend='redis://localhost')

@app.task
def store(L):
    r = Redis(host='localhost', port=6379, db=0)
    for (k, v) in L:
        r.set(k, v)
    r.close()

    return f'Processed {len(L)} pairs.'