from kafka import KafkaProducer
import time
TOPIC = 'ElonMusk'

KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

# a = {'jfkdsa':'fdsa'}
num = bytes('1', encoding='utf-8')
producer.send(TOPIC,  key=b'foo', value = num)

producer.flush()
# time.sleep(5)