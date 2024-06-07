import os
import pandas as pd
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset
from geopy.geocoders import Nominatim, ArcGIS
import geopy
import time
import ssl
import certifi
import re
from typing import Tuple
import sqlite3

incoming_data_dir = "data/incoming"
start_date = "2024-01-01+0000"
end_date = "2024-01-02+0000"

partitions_def = DailyPartitionsDefinition(start_date=start_date, end_date=end_date)

def get_location_for_address(address: str) -> Tuple[float, float]:
  try:
    # Connect to SQLite database
    conn = sqlite3.connect('data/calls_for_service_enriched.db')
    cursor = conn.cursor()

    # Query to find latitude and longitude for the given address
    cursor.execute('''
      SELECT Latitude, Longitude FROM EnrichedCallsForService
      WHERE Block_Address = ?
    ''', (address,))

    # Fetch the result
    result = cursor.fetchone()

    # Close the connection
    conn.close()

    if result:
      # Return the latitude and longitude if found
      return (result[0], result[1])
    else:
      # Return None if no matching address is found
      return (None, None)
  except Exception as e:
    print(f"Error retrieving location for address {address}: {e}")
    return (None, None)


@asset(partitions_def=partitions_def)
def raw_calls_for_service_data(context: AssetExecutionContext) -> pd.DataFrame:
  partition_date_str = context.partition_key
  files = os.listdir(incoming_data_dir)
  daily_data = pd.DataFrame()

  print("Processing",  partition_date_str)

  for file in files:
    if file.endswith('.csv'):
      data_path = os.path.join(incoming_data_dir, file)
      data = pd.read_csv(data_path)
      # Convert 'CreateDatetime' to datetime and filter data to match the exact hour in partition_date_str
      data['CreateDatetime'] = pd.to_datetime(data['CreateDatetime'])
      filtered_data = data[data['CreateDatetime'].dt.strftime('%Y-%m-%d') == partition_date_str]
      daily_data = pd.concat([daily_data, filtered_data], ignore_index=True)
    
  print(daily_data)
  return daily_data

@asset(partitions_def=partitions_def)
def geocoded_calls_for_service_data(raw_calls_for_service_data: pd.DataFrame) -> pd.DataFrame:
  ctx = ssl.create_default_context(cafile=certifi.where())
  geopy.geocoders.options.default_ssl_context = ctx
  
  if raw_calls_for_service_data.empty:
    return pd.DataFrame(columns=['Block_Address', 'Coordinates', 'Latitude', 'Longitude'])

  df = raw_calls_for_service_data
  geolocator = ArcGIS()
  # geolocator = Nominatim(user_agent="berk-crime-map")
  
  print(df)

  def get_lat_long(address):
    address = re.sub(r'\s+', ' ', address).strip()
    try:
      coordinates = get_location_for_address(address)
      if coordinates is None:
        # location = geolocator.geocode({'street': address, 'city': 'Berkeley', 'state': 'CA', 'country': 'USA'})
        coordinates = geolocator.geocode(f"{address}, Berkeley, CA, USA")
        print(coordinates)
      else:
        print(f"Using cached coordinates for {address}: {coordinates}")
      return coordinates
    except Exception as e:
      print(f"Error geocoding block address {address}: {e}")
      return (None, None)
  
  df['Coordinates'] = df['Block_Address'].apply(get_lat_long)
  df[['Latitude', 'Longitude']] = pd.DataFrame(df['Coordinates'].tolist(), index=df.index)
  df.drop(columns=['Coordinates'], inplace=True)
  return df

@asset(partitions_def=partitions_def)
def enriched_calls_for_service_data(geocoded_calls_for_service_data: pd.DataFrame) -> pd.DataFrame:
  # Connect to SQLite database
  conn = sqlite3.connect('data/calls_for_service_enriched.db')
  cursor = conn.cursor()

  # Create table if it does not exist
  cursor.execute('''
    CREATE TABLE IF NOT EXISTS EnrichedCallsForService (
      Incident_Number TEXT,
      CreateDatetime TEXT,
      Call_Type TEXT,
      Source TEXT,
      Progress TEXT,
      Priority INTEGER,
      Dispositions TEXT,
      Block_Address TEXT,
      City TEXT,
      ZIP_Code TEXT,
      NonBerkeley_Address TEXT,
      ObjectId INTEGER,
      Latitude REAL,
      Longitude REAL
    )
  ''')

  # Insert data into the table
  geocoded_calls_for_service_data.to_sql('EnrichedCallsForService', conn, if_exists='replace', index=False)

  # Commit changes and close the connection
  conn.commit();
  conn.close();

  return geocoded_calls_for_service_data;

