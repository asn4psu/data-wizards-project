#!/usr/bin/env python

import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from requests import get
import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import re

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
    url = "http://www.psudataeng.com:8000/getStopEvents"
    response = requests.get(url)
    htmlContent = response.content

    # Parse the recieved HTML using BeautifulSoup.
    soup = BeautifulSoup(htmlContent, 'html.parser')

    # Get the folder name.
    folderName = soup.find('h1').text
    date = re.search(r'\d{4}-\d{2}-\d{2}', folderName).group()
    newName = "stop-" + date

    # Get all of the stop events from the data.
    tables = soup.find_all('table')
    stopEvents = []
    for table in tables:
        event = table.find_previous('h2').text
        eventNumber = re.search(r'-?\d+', event).group()
        stopEvents.append(eventNumber)

    # Read the HTML table into a list of DataFrames.
    df_temp = pd.read_html('http://www.psudataeng.com:8000/getStopEvents')
    for i in range(len(df_temp)):
        df_temp[i].insert(0, 'trip_id', stopEvents[i])

    # Combine all the dataframes into one.
    df_temp = pd.concat(df_temp, ignore_index=True)
    df = df_temp[['trip_id', 'route_number', 'vehicle_number', 'service_key', 'direction']]
    # Convert the dataFrame to a dictionary.
    parsedData = df.to_dict()
    # Produce data by selecting random values from these lists.
    topic = "stop-events"
    
    # Produce the information to be sent to the consumer.
    """
    stop = df
    stopParsed = stop.to_dict()
    stopParsedJson = json.dumps(stopParsed).encode('utf-8')
    
    # Produce the data.
    producer.produce(topic, stopParsedJson)
    # Call flush???
    

    
    if i % 10000 == 0:
        producer.flush()
    """  
    # Produce the information to be sent to the consumer.
    # Iterate over the rows of the DataFrame
    for i, row in df.iterrows():
        # Convert the row to a dictionary
        row_dict = row.to_dict()

        # Convert the row dictionary to a JSON string
        if isinstance(row_dict, dict):

            row_json = json.dumps(row_dict).encode('utf-8')

            # Produce the message with the JSON string payload
            producer.produce(topic, row_json)

            # Call flush after producing every row (if needed)
            if i % 10000 == 0:
                producer.flush()

    # Block until the messages are sent.
    producer.poll(100)
    producer.flush()

    # Print the # of sensor readings.
    print(i)
    #print(stop)
