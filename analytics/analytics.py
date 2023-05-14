import json
from os import environ
from time import sleep
from sqlalchemy import create_engine, Column, String, Table, func
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
from geopy import distance

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here

Base = declarative_base()


# Extract and Auto loads devices table in Device Object
class Device(Base):
    __table__ = Table("devices", Base.metadata, autoload_with=psql_engine)


# Function to calculate Distance between Two location
def calculate_distance(loc1, loc2):
    point1 = json.loads(loc1)  # Json loads first location
    point2 = json.loads(loc2)  # Json loads second location

    # return distance between two data points
    return distance.distance((point1[0]['latitude'], point1[0]['longitude']),
                             (point2[0]['latitude'], point2[0]['longitude'])).km


# PostgreSQL Database connection
Session = sessionmaker(bind=psql_engine)
psql_session = Session()

# ORM Script to Aggregate maximum temperatures measured for every device per hours
max_temp_query = psql_session.query(Device.device_id, Device.time.hour.label('Hour'), func.max(Device.temperature)) \
    .group_by(Device.device_id, Device.time.hour).all()

# Convert into dataframe
max_temp_df = pd.read_sql_query(max_temp_query.statement, con=psql_engine)

# ORM Script to Aggregate data points aggregated for every device per hours.
data_point_query = psql_session.query(Device.device_id, Device.time.hour.label('Hour'), func.count(1)) \
    .group_by(Device.device_id, Device.time.hour).all()

# Convert into dataframe
data_point_df = pd.read_sql_query(data_point_query.statement, con=psql_engine)

# ORM Script to Partition the dataset based on the Device ID and Time Hour.
device_distance_query = psql_session.query(Device.device_id,
                                           Device.time.hour.label('Hour'),
                                           Device.location,
                                           func.row_number().over(
                                               partition_by=(Device.device_id, Device.time.hour),
                                               order_by=Device.time.asc()
                                           ).label('row_number')
                                           ).all()

# Convert into dataframe
device_distance_df = pd.read_sql_query(device_distance_query.statement, con=psql_engine)

# Add new Column with default value
device_distance_df = device_distance_df.assign(location2='NULL')

# Assign value of New Column with the previous location of same device to calculate distance.
device_distance_df.loc[0, 'location2'] = device_distance_df.loc[0, 'location']

for i in range(1, len(device_distance_df)):
    device_distance_df.loc[i, 'C'] = device_distance_df.loc[i - 1, 'C']


# Calculate Distance Between Two Locations
device_distance_df['distance'] = device_distance_df.apply(
    lambda x: calculate_distance(x['location'], x['location2']) if x['row_number'] > 1 else 0, axis=1)


# Calculate Total distance of device movement for every device per hours
device_distance_df = device_distance_df.groupby(['device_id','Hour'])['distance'].sum()


# Connection to the MySQL Database
while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)

print('Connection to MySQL successful.')

# MySQL Database Session
Session = sessionmaker(bind=mysql_engine)
psql_session = Session()

# Store Aggregated temperature of Device per Hour into MySQL Table
max_temp_df.to_sql('device_Hourly_temperature', mysql_engine)

# Store Aggregated datapoints of Device per Hour into MySQL Table
data_point_df.to_sql('device_hourly_datapoints', mysql_engine)

# Store Aggregated distance of Device per Hour into MySQL Table
device_distance_df.to_sql('device_hourly_distance', mysql_engine)
