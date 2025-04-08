from kafka import KafkaConsumer 
from json import loads

szer = lambda x: loads(x.decode('utf-8'))
consumer = KafkaConsumer('twopart', bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',group_id='gr1',value_deserializer=szer)

for doc in consumer:
    docval = doc.value
    print(f'Received {docval}')