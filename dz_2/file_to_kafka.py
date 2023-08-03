#!/usr/bin/python3
import pandas as pd
from kafka import KafkaProducer
import json
import subprocess

# Скачать файл из HDFS
subprocess.call(['hdfs', 'dfs', '-get', '/mysha/dz2/iris_fifty_lines.csv', 'iris_fifty_lines.csv'])

# Прочитать CSV-файл в объект Pandas DataFrame
df = pd.read_csv('iris_fifty_lines.csv')

# Преобразовать DataFrame в список словарей
data = df.to_dict(orient='records')

# Отправить данные в топик Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

for record in data:
    producer.send('iris', record)

producer.close()

