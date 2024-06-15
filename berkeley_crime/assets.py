import os
import ssl

import certifi
import geopy
import pandas as pd
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.sqlite import insert

from . import resources

incoming_data_dir = "data/incoming"
start_date = "2015-01-01+0000"

partitions_def = DailyPartitionsDefinition(start_date=start_date)

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
      data['CreateDatetime'] = data['CreateDatetime'].fillna(method='ffill')
      data['CreateDatetime'] = pd.to_datetime(data['CreateDatetime'], utc=True)
      filtered_data = data[data['CreateDatetime'].dt.strftime('%Y-%m-%d') == partition_date_str]
      daily_data = pd.concat([daily_data, filtered_data], ignore_index=True)
    
  print(daily_data)
  return daily_data

@asset(partitions_def=partitions_def)
def geocoded_calls_for_service_data(raw_calls_for_service_data: pd.DataFrame, geopy_client: resources.GeopyClient) -> pd.DataFrame:
  ctx = ssl.create_default_context(cafile=certifi.where())
  geopy.geocoders.options.default_ssl_context = ctx
  
  if raw_calls_for_service_data.empty:
    return pd.DataFrame(columns=['Block_Address', 'Coordinates', 'Latitude', 'Longitude'])

  df = raw_calls_for_service_data
  df['Coordinates'] = df['Block_Address'].apply(geopy_client.get_coords)
  df[['Latitude', 'Longitude']] = pd.DataFrame(df['Coordinates'].tolist(), index=df.index)
  df.drop(columns=['Coordinates'], inplace=True)
  return df


@asset(partitions_def=partitions_def)
def enriched_calls_for_service_data(geocoded_calls_for_service_data: pd.DataFrame) -> pd.DataFrame:
  # Connect to SQLite database
  engine = create_engine('sqlite:///data/calls_for_service_enriched.db')
  conn = engine.connect()

  if 'CreateDatetime' in geocoded_calls_for_service_data.columns:
    geocoded_calls_for_service_data['CreateDatetime'] = geocoded_calls_for_service_data['CreateDatetime'].astype(str)

  # Create table if it does not exist
  conn.execute(text('''
    CREATE TABLE IF NOT EXISTS EnrichedCallsForService (
      Incident_Number TEXT PRIMARY KEY,
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
  '''))

  # Insert data into the table
  metadata = MetaData()
  metadata.bind = engine
  table = Table('EnrichedCallsForService', metadata, autoload_with=engine)

  stmt = insert(table).values(geocoded_calls_for_service_data.to_dict(orient='records'))
  do_update_stmt = stmt.on_conflict_do_update(
    index_elements=['Incident_Number'],
    set_={c.name: c for c in stmt.excluded if c.name != 'Incident_Number'}
  )
  num_records_updated = conn.execute(do_update_stmt).rowcount
  print(f"Number of records updated: {num_records_updated}")

  # Commit changes and close the connection
  conn.commit()
  conn.close()
  return geocoded_calls_for_service_data;
