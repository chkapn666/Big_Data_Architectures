import json
from kafka import KafkaConsumer

consumer = KafkaConsumer('tweeter', 
                            auto_offset_reset='earliest',
                            bootstrap_servers=['localhost:9092'])

# !! We need to make sure that the json file i 'cat' into the producer is a NEWLINE-DELIMITED JSON FILE !!
for msg in consumer:
    tw = json.loads(msg.value)
    print(tw)
