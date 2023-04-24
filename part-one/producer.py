#!/usr/bin/env python

import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from requests import get
import json

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Get the url of the data we're going to be accessing.
    url = "http://www.psudataeng.com:8000/getBreadCrumbData"

    # Open the file from the site, read it, and write the data to "data.json".
    with open("data.json", "wb") as data:
        response = get(url)
        data.write(response.content)

    # Close the file.
    data.close()

    # Re-open the file, and parse the JSON data recieved.
    with open("data.json", "r") as raw:
        parsedData = json.load(raw)

    # Close the file.
    raw.close()

    # Produce data by selecting random values from these lists.
    topic = "sensor-data"

    # Loop through the list of dictionaries and produce the information to be sent to the consumer.
    for i in range(len(parsedData)):

        # Get the data from the dictionary.
        # Convert the data of this dictionary into bytes.
        trip = json.dumps(parsedData[i]).encode('utf-8')

        # Produce the data.
        producer.produce(topic, trip)

        # Increment the counter.
        ++i

        # Call flush???
        if i % 10000 == 0:
            producer.flush()

    # Block until the messages are sent.
    producer.poll(100)
    producer.flush()

    # Print the # of sensor readings.
    print(i)
