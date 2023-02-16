# Flight Tracker
This is a web app that tracks the positions of flights around a given location and displays their movements on a map in real-time. It uses data from the OpenSky API and stores it in a SQLite database.

# Getting Started
Clone this repository: git clone https://github.com/your-username/flight-tracker.git
Install the required packages: pip install -r requirements.txt
Run the ETL process to populate the flights database: python etl.py
Start the Dash app to display the flight map: python app.py
# Usage
Open your web browser and go to http://localhost:8050 to view the flight map.
The map will display the 10 closest flights to the default location, which is London. The flights will be represented by red plane icons on the map.
The map will update every 60 seconds to show the latest position of each flight.