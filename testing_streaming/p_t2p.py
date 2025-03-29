from kafka import KafkaProducer
import time
import json

szer = lambda x: json.dumps(x).encode('utf-8')

producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=szer
        )

for e in range(100):
    msg = {'Num': e}
    producer.send('t2p', value=msg)
    print(f'Produced value: {msg}')
    time.sleep(1)
