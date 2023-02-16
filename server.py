import time
import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from typing import List

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
    
    #add to df method
    def too_df(self):
        return pd.DataFrame(self.__dict__, index=[0])


# Define the data access layer class
class FlightDataAccessLayer:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def get_all_flights(self) -> List[Flight]:
        with self.SessionLocal() as session:
            return session.query(Flight).all()

    def add_flights(self, flights: List[Flight]):
        with self.SessionLocal() as session:
            session.add_all(flights)
            session.commit()

# Create the database tables
engine = create_engine("sqlite:///flights.db")
Base.metadata.create_all(engine)

# Define the pipeline functions
def extract_data():
    url = "https://opensky-network.org/api/states/all"
    params = {"lamin": 51.25, "lomin": -0.5, "lamax": 52.5, "lomax": 0.5}
    response = requests.get(url, params=params)
    data = response.json()["states"]
    return data

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

def run_pipeline():
    data = extract_data()
    flights = transform_data(data)
    load_data(flights)
    print("Pipeline run successfully at", time.ctime())

''' # Schedule the pipeline to run periodically
import schedule
import time

schedule.every(1).minutes.do(run_pipeline)

while True:
    schedule.run_pending()
    time.sleep(1)
 '''

run_pipeline()
 