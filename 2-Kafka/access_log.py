import time
import random
from random import randint
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
import json

bootstrap_servers = ['localhost:9092']  # Kafka broker'ın adresi ve portu
topic_name = 'ornek'  # Kafka topic adı
print(topic_name)

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def generate_random_ip():
    return '.'.join(
        str(randint(0, 255)) for _ in range(4))


log = pd.read_csv('log.csv')
old_ip = '10.131.2.1'

for i in range(100):
    n = random.uniform(1.2, 6.9)
    interval = float(round(n, 3))
    date_time = datetime.now()
    date_time_ = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    new_ip = str(generate_random_ip())
    ip = str(random.choice([old_ip, new_ip, new_ip]))
    old_ip = ip
    url = str(random.choice(log['URL']))
    status = int(random.choice(log['Staus']))
    size = int(random.randrange(1000, 9999, 4))
    duration = float(round(random.uniform(4.2, 14.6), 1))

    # print(f"{interval}", date_time_, ip, url, status, size, duration)
    data = {
        "interval": interval,
        "date_time_": date_time_,
        "ip": ip,
        "url": url,
        "status": status,
        "size": size,
        "duration": duration
    }

    producer.send(topic_name, value=data)
    producer.flush()
    print("data")
    time.sleep(1)

