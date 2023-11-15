import json
import time

import urllib.request
from urllib.request import Request, urlopen

from confluent_kafka import Producer

API_KEY = "7622b4ba-557b-4692-b92e-63dd812a7002"

url = "https://airlabs.co/api/v9/flights?api_key={}".format(API_KEY)

conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your actual Kafka broker address
    # Add other configuration properties as needed
}

producer = Producer(conf)

while True:

 response = urllib.request.Request(url,headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0'})
 flights = json.loads(urlopen(response).read().decode())

 for flight in flights["response"]:

   #print(flight)

   producer.produce("flight-realtime", json.dumps(flight).encode())

 print("{} Produced {} station records".format(time.time(), len(flights)))

 time.sleep(1)