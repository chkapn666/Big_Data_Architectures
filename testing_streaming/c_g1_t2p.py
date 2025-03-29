from kafka import KafkaConsumer
import json

szer = lambda x: json.loads(x.decode('utf-8'))

consumer = KafkaConsumer(
        't2p',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='g1',
        value_deserializer=szer
    )

for doc in consumer:
    docval = doc.value
    print(f'Received Value {docval}')
    
