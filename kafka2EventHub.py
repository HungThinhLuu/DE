import pandas as pd
import json
df = pd.read_csv('VNI-test.csv')

list_price = ["Price", "Open", "High", "Low"]
for i in list_price:
    df[i] = pd.to_numeric(df[i].apply(lambda x: x.replace(",", "")))

BOOTSTRAP_SERVER = 'friday.servicebus.windows.net:9093'
TOPIC = 'stock'

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER], 
                        security_protocol = 'SASL_SSL',
                        sasl_mechanism = 'PLAIN',
                        sasl_plain_username = '$ConnectionString',
                        sasl_plain_password = 'Endpoint=sb://friday.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=cX20chc/DHcQwSatk+oInoN6+7z20KnyI+AEhDB0034=')

col = []
for i in range(0, 7):
    col.append(df.columns[i])

def send(tmp):
    '''s = ""
    for i in range(0,6):
        s = s + col[i] + ": " + str(tmp[i]) + "-"'''
    s = {}
    for i in range(0, 7):
        s[col[i]] = tmp[i]
    print("Message: ", s)
    producer.send(TOPIC, value = json.dumps(s).encode('utf-8'))

import time
timing = 5
for i in range(df.shape[0]-1, -1, -1):
    tmp = list(df.loc[i])
    send(tmp)
    time.sleep(timing)