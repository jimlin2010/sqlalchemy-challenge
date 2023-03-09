from flask import Flask, jsonify

import numpy as np
import pandas as pd
import datetime as dt

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

########################################################
# create engine to hawaii.sqlite
engine = create_engine('sqlite:///Resources/hawaii.sqlite')
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# View all of the classes that automap found
Base.classes.keys()
# Save references to each table
measurement_ref = Base.classes.measurement
station_ref = Base.classes.station
# Create our session (link) from Python to the DB
session = Session(engine)
########################################################

app = Flask('weather')

api_v1_root = '/api/v1.0/'


# Question 1
@app.route('/')
def homepage():
    return jsonify('/api/v1.0/precipitation' , '/api/v1.0/stations', '/api/v1.0/tobs', '/api/v1.0/<start>', '/api/v1.0/<start>/<end>')



# Question 2
@app.route(api_v1_root + 'precipitation')
def precipitation():
    # Find the most recent date in the data set.
    most_recent_date = session.query(measurement_ref.date).order_by(measurement_ref.date.desc()).first().date
    timestamp_arr = most_recent_date.split('-')
    start_date = dt.date(int(timestamp_arr[0]), int(timestamp_arr[1]), int(timestamp_arr[2])) - dt.timedelta(days = 365)
    last_prcp = session.query(measurement_ref.date, measurement_ref.prcp).\
    filter(measurement_ref.date >= start_date).\
    order_by(measurement_ref.date).all()
    # prcp_df = pd.DataFrame(last_prcp, columns=['Date', 'Precipitation'])
    # prcp_df = prcp_df.sort_values(by='Date', ascending=True)
    result = {}
    for date, prcp in last_prcp:
        result[date] = prcp

    return jsonify(result)


# Question 3
@app.route(api_v1_root + 'stations')
def stations():
   # Design a query to calculate the total number stations in the dataset
    session.query(station_ref.station).count()
    # List the stations and the counts in descending order.
    station_agg = session.query(measurement_ref.station, func.count(measurement_ref.station)).\
    group_by(measurement_ref.station).\
    order_by(func.count(measurement_ref.station).desc()).all()

    station_list = [station[0] for station in station_agg]
    return jsonify(station_list)


# Question 4
@app.route(api_v1_root + 'tobs')
def tobs():
    most_recent_date = session.query(measurement_ref.date).order_by(measurement_ref.date.desc()).first().date
    timestamp_arr = most_recent_date.split('-')
    start_date = dt.date(int(timestamp_arr[0]), int(timestamp_arr[1]), int(timestamp_arr[2])) - dt.timedelta(days = 365)
    station_agg = session.query(measurement_ref.station, func.count(measurement_ref.station)).\
    group_by(measurement_ref.station).\
    order_by(func.count(measurement_ref.station).desc()).all()
    # Using the most active station id from the previous query, calculate the lowest, highest, and average temperature.
    most_active_station = station_agg[0][0]
    temp_observation = session.query(measurement_ref.station, measurement_ref.tobs).\
    filter(measurement_ref.station == most_active_station).\
    filter(measurement_ref.date >= start_date).all()
    
    temp_list = [temp[1] for temp in temp_observation]
    return jsonify(temp_list)



# Question 5

@app.route(api_v1_root + '<start>')
def temp_start(start):
  
    print(start)
    value = Session(engine)

    output = value.query(func.min(measurement_ref.tobs), func.avg(measurement_ref.tobs), func.max(measurement_ref.tobs)).\
                filter(measurement_ref.date >= start).all()

    temp_analysis = {}
    temp_analysis["Min Temp"] = output[0][0]
    temp_analysis["Average Temp"] = output[0][1]
    temp_analysis["Max Temp"] = output[0][2]
    
    return jsonify(temp_analysis)



@app.route(api_v1_root + '<start>/<end>')
def temp_start_end(start, end):
  
    print(start, end)
    round = Session(engine)

    output = round.query(func.min(measurement_ref.tobs), func.avg(measurement_ref.tobs), func.max(measurement_ref.tobs)).\
        filter(measurement_ref.date >= start).\
        filter(measurement_ref.date <= end).all()

    temp_analysis = {}
    temp_analysis["Min Temp"] = output[0][0]
    temp_analysis["Average Temp"] = output[0][1]
    temp_analysis["Max Temp"] = output[0][2]
    
    return jsonify(temp_analysis)

app.run()