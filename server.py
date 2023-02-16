import time
import requests
import datetime
from typing import List
from sqlalchemy import ForeignKey, create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, relationship
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
    last_updated = Column(DateTime, default=datetime.datetime.utcnow)
    positions = relationship("FlightPosition", back_populates="flight")

    def to_df(self):
        return pd.DataFrame(self.__dict__, index=[0])
    
class FlightPosition(Base):
    __tablename__ = "flight_positions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    icao24 = Column(String, ForeignKey("flights.icao24"))
    latitude = Column(Float)
    longitude = Column(Float)
    altitude = Column(Float)
    velocity = Column(Float)
    vertical_rate = Column(Float)
    time_position = Column(DateTime, default=datetime.datetime.utcnow)
    flight = relationship("Flight", back_populates="positions")

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
            
    def add_flight_positions(self, flight_positions: List[FlightPosition]):
        with self.SessionLocal() as session:
            session.add_all(flight_positions)
            session.commit()
            
    def get_latest_positions(self, icao24: str, limit: int = 100) -> List[FlightPosition]:
        with self.SessionLocal() as session:
            return session.query(FlightPosition).filter(FlightPosition.icao24 == icao24).order_by(FlightPosition.time_position.desc()).limit(limit).all()


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
            vertical_rate=row["vertical_rate"],
            last_updated=datetime.datetime.now(datetime.timezone.utc),
        )
        flights.append(flight)
    return flights

def load_data(flights):
    dal = FlightDataAccessLayer("sqlite:///flights.db")
    dal.add_flights(flights)

def update_flight_positions():
    dal = FlightDataAccessLayer("sqlite:///flights.db")
    flights = dal.get_all_flights()
    for flight in flights:
        #url = f"https://opensky-network.org/api/states/all?icao24={flight.icao24}"
        response = OpenSky().request("states/all", {"icao24": flight.icao24})
        if "states" in response:
            states = response["states"]
            if states:
                lat, lon, alt, vel, vr = states[0][6], states[0][5], states[0][7], states[0][9], states[0][11]
                position = FlightPosition(
                    icao24=flight.icao24,
                    latitude=lat,
                    longitude=lon,
                    altitude=alt,
                    velocity=vel,
                    vertical_rate=vr
                )
                #flight.positions.append(position)
                dal.add_flight_positions([position])
                
    
    print("Flight positions updated at", time.ctime())

def run_pipeline():
    data = extract_data()
    flights = transform_data(data)
    load_data(flights)
    print("Pipeline run successfully at", time.ctime())
 
#if main
if __name__ == "__main__":     
    #run_pipeline()
    update_flight_positions()
''' # Schedule the pipeline to run periodically
import schedule
import time

schedule.every(1).minutes.do(update_flight_positions)

while True:
    schedule.run_pending()
    time.sleep(1) '''