from kafka import KafkaConsumer
from json import loads

szer = lambda x: loads(x.decode('utf-8'))

# Change the topic name to match the producer
consumer = KafkaConsumer('numTest', bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', enable_auto_commit=True,
    group_id='group1', value_deserializer=szer)

suma = 0

for msg in consumer:
    msgval = msg.value
    if isinstance(msgval, dict):  # Ensure msgval is a dictionary
        suma += msgval['Num']
        print(f'Running sum: {suma}')
    else:
        print(f"Unexpected type: {type(msgval)}")
