from google.cloud import pubsub_v1

import random
import json
import datetime
import time
import numpy as np
from faker import Faker

# Project ID + Pub/Sub topic
PROJECT_ID = 'YOUR_PROJECT_ID'
TOPIC = 'YOUR_TOPIC'

# Instansiate publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)

# Web page urls + probabilities for np.random.choice
urls = ['/', '/books', '/books/001', '/books/002', '/gadget', '/gadget/004', '/special', '/special/thankyou']
even_days = [0.5, 0.05, 0.025, 0.025, 0.05, 0.05, 0.2, 0.1]
odd_days = [0.47, 0.05, 0.025, 0.025, 0.05, 0.05, 0.2, 0.13]

# faker instance
fake = Faker()

# Publish the data
def publish(publisher, topic_path, message):
    data = message.encode('utf-8')

    return publisher.publish(topic_path, data=data)

# Generate user log data for this pipeline simulation
# The data will be user_id, display size, user's visiting log, and timestamp
def generate_data():
    data = {}
    data['user_id'] = random.randrange(1, 101, 1)
    data['device'] = np.random.choice(['1024', '768', '480'], 1, p=[0.15, 0.15, 0.7])[0]
    time_temp = fake.date_time_between('-42d', 'now')
    
    if time_temp.date().day % 2 == 0:
        data['page'] = np.random.choice(urls, 1, p=even_days)[0]
    else:
        data['page'] = np.random.choice(urls, 1, p=odd_days)[0]

    data['timestamp'] = time_temp.strftime('%d/%b/%Y:%H:%M:%S')

    return json.dumps(data)

if __name__ == '__main__':
    print("CTRL-C to stop")
    while True:
        publish(publisher, topic_path, generate_data())
        time.sleep(0.5)