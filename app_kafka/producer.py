#!/usr/bin/env python3

from confluent_kafka import Producer
from random import randint
from datetime import datetime, timedelta
import json
import time

def generate_data():
    current_time = datetime.now()
    random_time = current_time - timedelta(days=randint(0, 30),
                                            hours=randint(0, 23),
                                            minutes=randint(0, 59),
                                            seconds=randint(0, 59))
    timestamp = random_time.strftime('%Y-%m-%dT%H:%M:%S')
    achat = round(randint(48000, 51000) + randint(0, 99) * 0.01, 2)
    vente = round(achat + randint(0, 100) * 0.01, 2)
    volume = randint(10, 40)
        
    return  {"timestamp": timestamp, "achat": achat, "vente": vente, "volume": volume}

# Kafka broker address
bootstrap_servers = 'kafka:9092'

# Kafka topic you want to produce messages to
topic = 'topic1'

# Create a Kafka producer instance
producer = Producer({'bootstrap.servers': bootstrap_servers})

def delivery_report(err, msg):
    """Delivery report callback called on producing message"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Produce a message
def produce_message(key, value):
    producer.produce(topic, key=key, value=value, callback=delivery_report)
    producer.poll(0)  # Poll for events

# Wait for all messages to be delivered
producer.flush()

# Example usage
while(True):
    json_data = json.dumps(generate_data())
    print("sending :", json_data)
    produce_message(None, json_data)
    producer.flush()
    time.sleep (5)