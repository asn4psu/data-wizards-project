#!/usr/bin/env python

import numpy as np
import sys
import json
import pandas as pd
from datetime import date, datetime, timedelta
import psycopg2
from psycopg2 import Error
import sqlite3
from sqlalchemy import create_engine, inspect
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

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = "sensor-data"
    consumer.subscribe([topic], on_assign=reset_offset)

    # Make a counter for the # of messages consumed.
    count = 0

    # Make a list to store the parsed data dictionaries.
    dataList = []

    # Poll for new messages from Kafka and add them to the database.
    try:
        print("Attempting consumption...")
        while True:
            msg = consumer.poll(10.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Consumption complete! Waiting for new data.")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Parse the data, storing the resulting dictionary into a variable.
                parsedData = msg.value().decode('utf-8')
                parsedDataDict = json.loads(parsedData)

                # Append the dicitonary to the list we have.
                dataList.append(parsedDataDict)

                # Increment the count.
                ++count

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

        # Convert the list of dictionaries to JSON.
        jsonData = json.dumps(dataList, indent = 2)

        # Get the name of the file.
        name = str(date.today())

        # Write this data to a file, if there is new data.
        if dataList != []:
            with open(name, 'a') as f:
                f.write(jsonData)
            f.close()

        # Read in the breadcrumb data as a Pandas DataFrame
        df = pd.read_json(name)

        # Does each trip have a trip ID?
        if df['EVENT_NO_TRIP'].isnull().any():
            print("EVENT_NO_TRIP has null values")
        else:
            print("EVENT_NO_TRIP has no Null values")

        # Does each trip have a stop ID?
        if df['EVENT_NO_STOP'].isnull().any():
            print("EVENT_NO_STOP has null values")
        else:
            print("EVENT_NO_STOP has no Null values")

        # Does each trip have an operation date?
        if df['OPD_DATE'].isnull().any():
            print("OPD_DATE has null values")
        else:
            print("OPD_DATE has no Null values")

        # Is each trip connected with a vehicle ID?
        if df['VEHICLE_ID'].isnull().any():
            print("VEHICLE_ID has null values")
        else:
            print("VEHICLE_ID has no Null values")

        # Was there distance noted on every trip?
        if df['METERS'].isnull().any():
            print("METERS has null values")
        else:
            print("METERS has no Null values")

        # Did all the data have an acting time?
        if df['ACT_TIME'].isnull().any():
            print("ACT_TIME has null values")
        else:
            print("ACT_TIME has no Null values")

	# Check that when GPS_LONGITUDE = NaN, then GPS_LATITUDE = NaN
    	# This is true for all values of GPS_LONGITUDE that are null
        GPS_LONGITUDE_C = df['GPS_LONGITUDE'].isnull()
    	# This is False for all values of GPS_LONGITUDE that are null
        GPS_LATITUDE_C = ~(df['GPS_LATITUDE'].isnull())
    	# NaN & NaN = False, but Nan & ~NaN = True
        Check_NaN = GPS_LONGITUDE_C & GPS_LATITUDE_C
    	# The only case where there exist a GPS_LATITUDE when GPS_LONGITUDE = NaN is when (T & !F)
        if Check_NaN.all():
            print("There exist a GPS_LATITUDE when GPS_LONGITUDE is null")
        else:
            print("When GPS_LONGITUDE = NaN, then GPS_LATITUDE = NaN")

    	# Convert the the format of OPD_DATE to datetime
        Check_time = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S')

    	# Check if all rows have their hours, minutes, and seconds equal to zero
        hourzero = (Check_time.dt.hour == 0).all()
        minutezero = (Check_time.dt.minute == 0).all()
        secondzero = (Check_time.dt.second == 0).all()
        if hourzero and minutezero and secondzero:
            print("All hours, minutes, and seconds in OPD_DATE are equal to zero.")
        else:
            print("Some values in OPD_DATE have a hours, minutes, and seconds != 0.")

	# Check if all values in the gps_satellites are valid
    	# Count for the NaN values
        nan_gps_satellites = np.isnan(df['GPS_SATELLITES'])
    	# Check if all are in range of 0 - 32
        range_gps_satellites = df['GPS_SATELLITES'].between(0, 32)
    	# Values are either NaN or in range
        valid_gps_satellites = nan_gps_satellites | range_gps_satellites
    	# Check if all values meet the requirement
        all_valid_gps_satellites = valid_gps_satellites.all()

        if all_valid_gps_satellites:
            print("All values in GPS_SATELLITES are valid.")
        else:
            print("Some values in GPS_SATELLITES are not valid.")

	    # Check if all values in the GPS_LATITUDE are valid
        nan_GPS_LATITUDE = np.isnan(df['GPS_LATITUDE'])

	    # Check if all are in range of [-90,90]
        range_GPS_LATITUDE = df['GPS_LATITUDE'].between(-90, 90)
        valid_GPS_LATITUDE = nan_GPS_LATITUDE | range_GPS_LATITUDE
        all_valid_GPS_LATITUDE = valid_GPS_LATITUDE.all()

	    # Check if all values meet the requirement
        if all_valid_GPS_LATITUDE:
            print("All values in GPS_LATITUDE are valid.")
        else:
            print("Some values in GPS_LATITUDE are not valid.")

	# Data Transformation
        df_new = df.loc[:, ['METERS', 'ACT_TIME']]

        # Calculate the speed column
        df_new['SPEED'] = (df_new['METERS'] - df_new['METERS'].shift(1)) / (df_new['ACT_TIME'] - df_new['ACT_TIME'].shift(1))

        # Fill the first row's speed value with the second row's speed value
        df_new.loc[0, 'SPEED'] = df_new.loc[1, 'SPEED']

        df1 = df.loc[:, ['OPD_DATE', 'ACT_TIME']]

        def create_timestamp(row):
            opd_date = row['OPD_DATE']
            act_time = row['ACT_TIME']
            date_string = opd_date[:9]
            date_format = "%d%b%Y"
            datetime_obj = datetime.strptime(date_string, date_format)
            time = timedelta(seconds=act_time)
            combined_timestamp = datetime_obj + time
            return combined_timestamp

        # Apply the create_timestamp function to each row of the DataFrame to create a new TIMESTAMP column
        df1['TIMESTAMP'] = df1.apply(create_timestamp, axis=1)

        # Print the resulting DataFrame
        #print(df1)

        df_temp = pd.concat([df1, df_new, df], axis=1)
        df_final = df_temp.loc[:, ['EVENT_NO_TRIP', 'TIMESTAMP','VEHICLE_ID', 'SPEED', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP']]

        df_breadcrumb = df_final.loc[:, ['TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']]

        df_breadcrumb_renamed = df_final.loc[:, ['TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']].rename(columns={
            'TIMESTAMP': 'tstamp',
            'GPS_LATITUDE': 'latitude',
            'GPS_LONGITUDE': 'longitude',
            'SPEED': 'speed',
            'EVENT_NO_TRIP': 'trip_id'
        })
        print(df_breadcrumb_renamed);
        """
        df_trip = pd.DataFrame()
        df_trip['route_id', 'service_key', 'direction'] = None;
        df_trip['trip_id'] = df_breadcrumb_renamed['trip_id']
        df_trip['vehicle_id'] = df_final['VEHICLE_ID']
        df_trip = df_trip[['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction']]
        """
        df_trip = pd.DataFrame(columns=['route_id', 'service_key', 'direction'])
        df_trip['trip_id'] = df_breadcrumb_renamed['trip_id']
        df_trip['vehicle_id'] = df_final['VEHICLE_ID']

        df_trip = df_trip[['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction']]
        df_trip = df_trip.drop_duplicates()
        print(df_trip);
                
        engine = create_engine('postgresql://postgres:2023@localhost/postgres')
        connection = engine.connect()


        try:
            # Create an inspector
            inspector = inspect(engine)

            if 'trip' in inspector.get_table_names():
                df_trip.to_sql('trip', connection, if_exists='append', index=False)
            else:
                print("The 'trip' table does not exist in the database.")
            # Check if the "BreadCrumb" table exists
            if 'breadcrumb' in inspector.get_table_names():
                # Insert the data into the "BreadCrumb" table
                df_breadcrumb_renamed.to_sql('breadcrumb', connection, if_exists='append', index=False)
                print("Row inserted successfully.")
            else:
                print("The 'breadcrumb' table does not exist in the database.")

        except Exception as e:
            print("An error occurred:", str(e))

        finally:
            # Close the connection
            connection.close()
            

