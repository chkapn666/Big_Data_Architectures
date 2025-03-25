from celery import Celery
from datetime import datetime

app = Celery('my_app1', broker='amqp://localhost', backend='redis://localhost')

@app.task
def ts(msg):
    now = datetime.now()
    return msg + ' [ts: ' + now.strftime('%d%m%Y %H:%M:%S') + ']'

