# brew services start zookeeper
# brew services start kafka
# Create a topic:
# kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic testT
# List available topics:
# kafka-topics --list --bootstrap-server localhost:9092
# Command line message production/consumption
# kafka-console-producer --broker-list localhost:9092 --topic testT
# kafka-console-consumer --bootstrap-server localhost:9092 --topic testT --from-beginning
# Deleting a topic
# 


from kafka import KafkaProducer
import time
import json

szer = lambda x: json.dumps(x).encode('utf-8')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=szer)

for e in range(100):
    data = {'Num': e}
    # Change the topic name to match the consumer
    producer.send('numTest', value=data)
    print(f'Sent message {data}')
    time.sleep(5)
