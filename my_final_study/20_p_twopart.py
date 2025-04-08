from kafka import KafkaProducer
from time import sleep
from json import dumps 

szer = lambda x: dumps(x).encode('utf-8')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=szer)

for e in range(100):
    msg = {'Num': e}
    producer.send('twopart', value=msg)
    print(f'Produced Value: {msg}')
    sleep(5)