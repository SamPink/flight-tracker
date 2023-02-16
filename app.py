import dash
import dash_core_components as dcc
import dash_html_components as html
import folium
from folium.plugins import MarkerCluster
from sqlalchemy import create_engine
from server import FlightDataAccessLayer, FlightPosition

# Initialize the FlightDataAccessLayer
dal = FlightDataAccessLayer("sqlite:///flights.db")

# Initialize the Dash app
app = dash.Dash(__name__)

# Define the app layout
app.layout = html.Div(children=[
    html.H1(children='London Flights Map'),

    html.Div(children=[
        html.Iframe(id='map', srcDoc='', width='100%', height='600')
    ]),
    dcc.Interval(
        id='interval-component',
        interval=60*1000, # Update every 5 seconds
        n_intervals=0
    )
])

# Define the callback function to update the flight map
@app.callback(
    dash.dependencies.Output('map', 'srcDoc'),
    [dash.dependencies.Input('interval-component', 'n_intervals')]
)
def update_flight_map(n):
    flights = dal.get_all_flights()
    flight_map = folium.Map(
        location=[51.5074, -0.1278],
        zoom_start=8
    )
    marker_cluster = MarkerCluster().add_to(flight_map)
    for flight in flights:
        positions = dal.get_latest_positions(flight.icao24, limit=5000)
        lat_long = [[position.latitude, position.longitude] for position in positions]
        if lat_long:
            popup_text = f"Flight: {flight.callsign or 'Unknown'}<br>Altitude: {flight.baro_altitude or 'Unknown'} meters"
            folium.Marker(
                location=[positions[-1].latitude, positions[-1].longitude],
                popup=popup_text,
                icon=folium.Icon(color='red', icon='plane', prefix='fa')
            ).add_to(marker_cluster)
            folium.PolyLine(locations=lat_long, color='blue', weight=2.5, opacity=1).add_to(flight_map)
    return flight_map._repr_html_()

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
