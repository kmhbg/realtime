from kafka import KafkaProducer
import json
from time import sleep

class Producer:
    def __init__(self,host="localhost",port=9092,topic="new_topic"):
        self.host = host
        self.port = port 
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=f"{self.host}:{self.port}",
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    
    def boadcast(self):
        # Send messages with values starting from 0
        for i in range(50):
            value = {"value": i}
            self.producer.send(self.topic, value=value)
            sleep(3)


       
# Set the Kafka topic and bootstrap servers


# Create the Kafka producer

producer = Producer(topic="count")
producer.boadcast()
