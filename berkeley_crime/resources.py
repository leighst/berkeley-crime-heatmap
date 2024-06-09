from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import sqlite3
from dagster._core.execution.context.init import InitResourceContext

import requests
from dagster import ConfigurableResource
from dagster._utils.cached_method import cached_method

from geopy.geocoders import Nominatim, ArcGIS
from geopy.exc import GeopyError

from typing import Tuple
import tenacity
import re

# Custom retry condition to check for specific HTTP status codes
def retry_on_http_status_code(exception):
  if isinstance(exception, GeopyHTTPError) and exception.status_code in {503, 429}:
    return True
  return False

# Retry strategy
retry_strategy = (
    tenacity.retry_if_exception_type(TimeoutError) |
    tenacity.retry_if_exception(retry_on_http_status_code)
)

# todo: use cached_method
class GeopyClient(ConfigurableResource, ABC):
    @abstractmethod
    def get_coords(self, address: str) -> Optional[Any]:
        pass

geolocator = ArcGIS()

class GeopyAPIClient(GeopyClient):
    @cached_method
    def get_coords(self, block_address: str) -> Optional[Any]:
      address = re.sub(r'\s+', ' ', block_address).strip()
      address = f"{address}, Berkeley, CA, USA"
      try:
        coordinates = get_cached_coords(block_address)
        if coordinates == (None, None):
          location = self.geocode_with_retry(address)

          if location is None:
            print(f"No location found for address {address}")
            return (None, None)

          coordinates = (location.latitude, location.longitude)
          print(f"Using new coordinates for {address}: {coordinates}")
        else:
          print(f"Using cached coordinates for {address}: {coordinates}")
          return coordinates
      except Exception as e:
        print(f"Error geocoding block address {address}: {e}")
        raise e
      
    # Decorator for retrying the function
    @tenacity.retry(retry=retry_strategy, wait=tenacity.wait_exponential(multiplier=3), stop=tenacity.stop_after_attempt(5))
    def geocode_with_retry(self, address):
      try:
        location = geolocator.geocode(address, timeout=10)
        print(f"Geocoded address {address} to {location}")
        return location
      except requests.exceptions.RequestException as e:
        print(f"Error geocoding address {address}: {e}")
        if hasattr(e, 'response') and e.response is not None:
          status_code = e.response.status_code
          if status_code in {503, 429}:
            raise GeopyHTTPError(f"HTTP Error: {status_code}", status_code)
        raise

# Define a custom exception for specific HTTP status codes
class GeopyHTTPError(GeopyError):
    def __init__(self, message, status_code):
        super().__init__(message)
        self.status_code = status_code

def get_cached_coords(address: str) -> Tuple[float, float]:
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