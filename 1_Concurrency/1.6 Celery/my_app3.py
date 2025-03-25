from celery import Celery

app = Celery('my_app3', broker='amqp://localhost', backend='redis://localhost')

@app.task
def rsum(rng):
    sum = 0 
    for n in range(rng[0], rng[1]+1):
        sum += n 
    return sum

    