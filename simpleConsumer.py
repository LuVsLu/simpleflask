from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

client = KafkaClient("localhost:9092")
consumer = SimpleConsumer(client, "test-group", "test2")

for message in consumer:
    print(message)

