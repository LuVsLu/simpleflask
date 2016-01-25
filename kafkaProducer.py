import logging

from kafka.producer import SimpleProducer
from kafka.client import KafkaClient

# To send messages synchronously
client = KafkaClient('localhost:9092')
producer = SimpleProducer(client, async=False)

# Note that the application is responsible for encoding messages to type bytes
producer.send_messages('test', b'some message')
producer.send_messages('test', b'this method', b'is variadic')

# Send unicode message
producer.send_messages('test', b'how are you?')

# To wait for acknowledgements
# ACK_AFTER_LOCAL_WRITE : server will wait till the data is written to
#                         a local log before sending response
# ACK_AFTER_CLUSTER_COMMIT : server will block until the message is committed
#                            by all in sync replicas before sending a response
producer = SimpleProducer(client,
                          async=False,
                          req_acks=SimpleProducer.ACK_AFTER_LOCAL_WRITE,
                          ack_timeout=2000,
                          sync_fail_on_error=False)

responses = producer.send_messages('test', b'another message')
for r in responses:
    logging.info(r.offset)
