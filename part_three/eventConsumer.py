#!/usr/bin/env python

import numpy as np
import sys
import json
import pandas as pd
from datetime import date, datetime, timedelta
import psycopg2
from psycopg2 import Error
import sqlite3
from sqlalchemy import create_engine, inspect, text
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING



if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)
    

    def remove_numeric_keys(data):
        if isinstance(data, dict):
            return {k: remove_numeric_keys(v) for k, v in data.items() if not k.isdigit()}
        if isinstance(data, list):
            return [remove_numeric_keys(item) for item in data]
        return data


    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    #topic = "stop-events"
    topic = "stop-events"
    consumer.subscribe([topic], on_assign=reset_offset)

    # Make a counter for the # of messages consumed.
    count = 0

    # Create a way to store the decoded data.
    decodedData = []
    # Poll for new messages from Kafka and add them to the database.
    try:
        print("Attempting consumption...")
        while True:
            msg = consumer.poll(30.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Consumption complete! Waiting for new data.")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Parse the data, storing the resulting dictionary into a variable.
                
                received = msg.value().decode('utf-8')
                receivedDict = json.loads(received)
                decodedData.append(receivedDict)

                
                # Increment the count.
                ++count

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

        # Make sure we consumed. 
        if decodedData: 
            # Read in the data as a Pandas DataFrame
           # df = pd.DataFrame.from_dict(decodedData)

           # print(df)
        # Remove numeric keys and keep values
            """
            clean_data = []
            for d in decodedData:
                if isinstance(d, dict):
                     cleaned_dict = {key: list(value.values()) for key, value in d.items() if isinstance(value, dict)}
                     clean_data.append(cleaned_dict)
                else:
                    print("Encountered non-dictionary data:", d)
            
            clean_data = [
                {key: list(value.values()) for key, value in d.items()}
                for d in decodedData
            
            ]
            
            print(clean_data)
            """
            df = pd.DataFrame.from_dict(decodedData)
            df_renamed = df.rename(columns={'route_number': 'route_id',
                        'vehicle_number': 'vehicle_id'})
            df_renamed = df_renamed.drop_duplicates()

            engine = create_engine('postgresql://postgres:2023@localhost/postgres')
            connection = engine.connect()
            

            try:
                # Create an inspector
                inspector = inspect(engine)

                # Check if the "BreadCrumb" table exists
                if 'trip' in inspector.get_table_names():

                    # Create a temporary table from the df_renamed DataFrame
                    df_renamed.to_sql('temp_table', engine, if_exists='replace', index=False)

                    # Join the temp_table with the trip table on trip_id column
                    query = """
                    BEGIN;
                    UPDATE trip
SET
  route_id = t.route_id::integer,
  vehicle_id = t.vehicle_id::integer,
  service_key = CASE
    WHEN t.service_key = 'W' THEN 'Weekday'
    WHEN t.service_key = 'Saturday' THEN 'Saturday'
    WHEN t.service_key = 'Sunday' THEN 'Sunday'
    ELSE trip.service_key
  END,
  direction = CASE
    WHEN t.direction = 0 THEN 'Out'
    WHEN t.direction = 1 THEN 'Back'
    ELSE trip.direction
  END
FROM temp_table t
WHERE trip.trip_id = t.trip_id::integer;

                    COMMIT;
                    """
                    query = query.strip()
                    # Wrap the query with sqlalchemy.text()
                    executable_query = text(query)

                    # Execute the SQL query
                    connection.execute(executable_query)
                    print("Row inserted successfully.")


                else:
                    print("The 'trip' table does not exist in the database.")

            except Exception as e:
                print("An error occurred:", str(e))

            finally:
                # Close the connection
                connection.close()

            """
            # Explode the DataFrame
            df_exploded = df.apply(pd.Series.explode)

            # Reset the index
            df_exploded = df_exploded.reset_index(drop=True)

            print(df_exploded)
            """
        else:
            print("does not exist")
""" 

        # Data Validation. Note that there's less here since we want to ensure the visualization works.
            # All tables have a trip_id. 
            valid_trip_id = True
            for i in range(len(df)):
                if df['trip_id'].isnull().any():
                    all_valid = False
            
            if valid_trip_id:
                print("All tables have trip_id.")
            else:
                print("Some tables have null values for trip_id.")
    	# Data Transformation
"""
