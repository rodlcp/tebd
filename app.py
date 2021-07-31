from flask import Blueprint
main = Blueprint('main', __name__)

import json
import datetime
import time
from engine import AllocationEngine
from kafka import KafkaConsumer, KafkaProducer
 
import pyspark.sql.functions as F

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    api_version = (0, 11, 15)
)
 
from flask import Flask, request, render_template, url_for, redirect, jsonify

@main.route("/rider/request", methods=["POST"])
def riderRequest():
    now = datetime.datetime.utcnow()

    data = {
        'riderid': int(request.form.get("riderid")),
        'lat': float(request.form.get("lat")),
        'lon': float(request.form.get("lon")),
        'requestTime': now.isoformat()
    }
    json_payload = str.encode(json.dumps(data))
    producer.send('RIDER', json_payload)
    producer.flush()
    return "True"

@main.route("/rider/interface", methods=["GET"])
def riderInterface():
    return render_template('rider.html', drivers_position=allocationEngine.currentDriversListCoord)

@main.route("/driver/all", methods=["GET"])
def driverAll():
    data = {
        'drivers': allocationEngine.activeDrivers.select('driverid').collect()
    }
    return json.dumps(data)

@main.route("/driver/all/location", methods=["GET"])
def driverAllLocation():
    return json.dumps(allocationEngine.driversPosition)

@main.route("/driver/activate", methods=["POST"])
def driverActivate():
    now = datetime.datetime.utcnow()
    
    data = {
        'driverid': int(request.form.get("driverid")),
        'requestTime': now.isoformat(),
        'active': True
    }

    json_payload = str.encode(json.dumps(data))
    producer.send('DRIVER', json_payload)
    producer.flush()
    return 'True'

@main.route("/driver/deactivate", methods=["POST"])
def driverDeactivate():
    now = datetime.datetime.utcnow()

    data = {
        'driverid': int(request.form.get("driverid")),
        'requestTime': now.isoformat(),
        'active': False
    }

    json_payload = str.encode(json.dumps(data))
    producer.send('DRIVER', json_payload)
    producer.flush()
    return 'True'

@main.route("/driver/<int:driverid>/location", methods=["GET"])
def driverLocation(driverid):
    # this should be replaced by pinging the drivers mobile device
    if driverid not in allocationEngine.driversPosition:
        point = allocationEngine.mobileUse.sample(0.0001).collect()[0][2]
        allocationEngine.driversPosition[driverid] = {'lat': point.y, 'lon':point.x}
    return json.dumps(allocationEngine.driversPosition[driverid])

@main.route("/dashboard/update", methods=["GET"])
def dashboardUpdate():
    riders_request = allocationEngine.currentRiders
    last_allocation = allocationEngine.lastAllocationList
    current_drivers = allocationEngine.currentDriversListCoord
    return jsonify(
        riders_request=riders_request,
        last_allocation=last_allocation,
        current_drivers=current_drivers
    )

@main.route("/dashboard", methods=["GET"])
def dashboard():
    return render_template('dashboard.html')

def create_app(spark, originalDataPath, dbPath):
    global allocationEngine

    allocationEngine = AllocationEngine(spark, originalDataPath, dbPath)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app