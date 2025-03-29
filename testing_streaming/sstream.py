import json
from kafka import KafkaConsumer

consumer = KafkaConsumer('tweeter', 
                            auto_offset_reset='earliest',
                            bootstrap_servers=['localhost:9092'])

for msg in consumer:
    tw = json.loads(msg.value)
    print(tw)
