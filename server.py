import time
import requests
from typing import List
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import config

# Define the database models
Base = declarative_base()

class Flight(Base):
    __tablename__ = "flights"
    icao24 = Column(String, primary_key=True)
    callsign = Column(String)
    origin_country = Column(String)
    longitude = Column(Float)
    latitude = Column(Float)
    baro_altitude = Column(Float)
    velocity = Column(Float)
    vertical_rate = Column(Float)

    def to_df(self):
        return pd.DataFrame(self.__dict__, index=[0])

# Define the data access layer class
class FlightDataAccessLayer:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def get_all_flights(self) -> List[Flight]:
        with self.SessionLocal() as session:
            return session.query(Flight).all()

    def update_flight(self, flight: Flight):
        with self.SessionLocal() as session:
            session.merge(flight)
            session.commit()
    
    def add_flights(self, flights: List[Flight]):
        with self.SessionLocal() as session:
            session.add_all(flights)
            session.commit()

import requests
import time

class OpenSky:
    def __init__(self):
        self.url = "https://opensky-network.org/api"
        self.auth = (config.OPENSKY_USERNAME, config.OPENSKY_PASSWORD)

    def request(self, endpoint, params):
        # Set the number of times to retry the request
        max_retries = 5

        # Make the request and retry up to max_retries times if the request fails with a 429 error
        for i in range(max_retries):
            response = requests.get(f"{self.url}/{endpoint}", params=params, auth=self.auth)
            if response.status_code == 429:
                wait_time = 20
                print(f"Too many requests. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                return response.json()

        # If the request fails after max_retries, raise an exception
        raise Exception("Failed to retrieve data from OpenSky API")

# Create the database tables
engine = create_engine("sqlite:///flights.db")
Base.metadata.create_all(engine)

from haversine import haversine, Unit

def extract_data():
    # Define the location to search around
    location = (51.513348, -0.792391)

    # Make the API request
    #url = "https://opensky-network.org/api/states/all"
    params = {"lamin": location[0] - 1, "lomin": location[1] - 1, "lamax": location[0] + 1, "lomax": location[1] + 1}
    #response = requests.get(url, params=params)
    response = OpenSky().request("states/all", params)
    data = response["states"]

    # Calculate the distances between each flight and the location
    distances = []
    for row in data:
        if row[5] is not None and row[6] is not None:
            distance = haversine(location, (row[6], row[5]), unit=Unit.MILES)
            distances.append(distance)
        else:
            distances.append(float("inf"))

    # Add the distances to the data and sort by distance
    df = pd.DataFrame(data)
    df["distance"] = distances
    df = df.sort_values(by="distance").head(10)
    
    #filer out the flights that are too far away
    close_flights = []
    
    for index, row in df.iterrows():
        #find flight in data
        for flight in data:
            if flight[0] == row[0]:
                close_flights.append(flight)
                break
        

    return close_flights


def transform_data(data):
    columns = ["icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude", "latitude", "baro_altitude", "velocity", "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk", "spi", "position_source", 'n']
    df = pd.DataFrame(data, columns=columns)
    df = df[df["latitude"].notnull() & df["longitude"].notnull()]
    flights = []
    for index, row in df.iterrows():
        flight = Flight(
            icao24=row["icao24"],
            callsign=row["callsign"],
            origin_country=row["origin_country"],
            longitude=row["longitude"],
            latitude=row["latitude"],
            baro_altitude=row["baro_altitude"],
            velocity=row["velocity"],
            vertical_rate=row["vertical_rate"]
        )
        flights.append(flight)
    return flights

def load_data(flights):
    dal = FlightDataAccessLayer("sqlite:///flights.db")
    dal.add_flights(flights)

def update_flight_positions():
    # Initialize the FlightDataAccessLayer
    dal = FlightDataAccessLayer("sqlite:///flights.db")

    # Retrieve the flights from the database
    flights = dal.get_all_flights()

    # Loop through each flight and update its position
    for flight in flights:
        url = f"https://opensky-network.org/api/states/all?icao24={flight.icao24}"
        response = requests.get(url)
        data = response.json()["states"]
        if len(data) > 0:
            row = data[0]
            flight.longitude = row[5]
            flight.latitude = row[6]
            flight.baro_altitude = row[7]
            flight.velocity = row[9]
            flight.vertical_rate = row[11]
            dal.update_flight(flight)

def run_pipeline():
    data = extract_data()
    flights = transform_data(data)
    load_data(flights)
    print("Pipeline run successfully at", time.ctime())
 
#if main
if __name__ == "__main__":     
    run_pipeline()
#update_flight_positions()
''' # Schedule the pipeline to run periodically
import schedule
import time

schedule.every(1).minutes.do(update_flight_positions)

while True:
    schedule.run_pending()
    time.sleep(1) '''
