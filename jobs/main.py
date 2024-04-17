import os
import uuid
from datetime import datetime, timedelta
import random
import simplejson as json
from confluent_kafka import SerializingProducer

# Constants for the simulation
LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')

# Timestamp for simulation
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': str(uuid.uuid4()),
        'device_id': device_id,
        'timestamp': timestamp.isoformat(),
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp, location):
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'cameraId': str(uuid.uuid4()),
        'timestamp': timestamp.isoformat(),
        'snapshot': 'Base64EncodedString'
    }

def generate_weather_data(device_id, timestamp, location):
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp.isoformat(),
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rainy', 'Snow']),
        'precipitation': random.uniform(0, 100),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'airQualityIndex': random.uniform(0, 500)
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': str(uuid.uuid4()),
        'device_id': device_id,
        'incidentId': str(uuid.uuid4()),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp.isoformat(),
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }

def simulate_vehicle_movement():
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    return start_location.copy()

def generate_vehicle_data(device_id):
    timestamp = get_next_time()
    location = simulate_vehicle_movement()
    vehicle_data = {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': timestamp.isoformat(),
        'location': location,
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'
    }
    return vehicle_data, timestamp, location

def simulate_journey(producer, device_id):
    while True:
        vehicle_data, timestamp, location = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, timestamp)
        traffic_camera_data = generate_traffic_camera_data(device_id, timestamp, location)
        weather_data = generate_weather_data(device_id, timestamp, location)
        emergency_data = generate_emergency_incident_data(device_id, timestamp, location)

        # Print data for verification
        print(vehicle_data)
        print(gps_data)
        print(traffic_camera_data)
        print(weather_data)
        print(emergency_data)

        # Assuming this is commented out to run indefinitely
        break

if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f"Kafka error: {err}")
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-CodeWithYu-123')
    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f"Unexpected Error occurred: {e}")
