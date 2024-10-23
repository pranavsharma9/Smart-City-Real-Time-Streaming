import os
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random
import uuid
import time 
CHAMPAIGN_COORDINATES={
    "latitude":51.5074,
    "longitude":-0.1278
}

CHICAGO_COORDINATES={
    "latitude":52.4862,
    "longitude":-1.8904
}

LATITUDE_INCREMENT=(CHICAGO_COORDINATES["latitude"]-CHAMPAIGN_COORDINATES["latitude"])/100
LONGITUDE_INCREMENT=(CHICAGO_COORDINATES["longitude"]-CHAMPAIGN_COORDINATES["longitude"])/100

KAFKA_BOOSTRAP_SERVERS = os.environ.get('KAFKA_BOOSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC=os.environ.get('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC=os.environ.get('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC=os.environ.get('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC=os.environ.get('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC=os.environ.get('EMERGENCY_TOPIC', 'emergency_data')

start_time=datetime.now()
start_location=CHAMPAIGN_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time+=timedelta(seconds=random.randint(30,60))
    return start_time

def simulate_vehicle_movement():
    global start_location
    start_location["latitude"]+=LATITUDE_INCREMENT
    start_location["longitude"]+=LONGITUDE_INCREMENT

    start_location["latitude"]+=random.uniform(-0.0005,0.0005)
    start_location["longitude"]+=random.uniform(-0.0005,0.0005)

    return start_location

def generate_vehicle_data(device_id):
    location=simulate_vehicle_movement()

    return{
        'id':uuid.uuid4(),
        'deviceId': device_id ,
        'timestamp':get_next_time().isoformat(),
        'location': (location["latitude"],location["longitude"]),
        'speed': random.uniform(10,40),
        'direction': 'North-East',
        'make': 'BMW',
        'model':'C500',
        'year':2024,
        'fuelType':'Hybrid'
    }

def generate_gps_data(device_id,timestamp,vehicle_type='private'):
    return{
        'id':uuid.uuid4(),
        'deviceId': device_id,
        'timestamp':timestamp,
        'speed':random.uniform(10,40),
        'direction':'North-East',
        'vehicleType':vehicle_type
    
    }

def generate_traffic_camera_data(device_id,timestamp,camera_id):
    return {
        'id':uuid.uuid4(),
        'deviceId': device_id,
        'cameraId':camera_id,
        'timestamp':timestamp,
        'snapshot': 'Base64EncodedString',
    }

def generate_weather_data(device_id,timestamp,location):
    return {
        'id':uuid.uuid4(),
        'deviceId': device_id,
        'location':location,
        'timestamp':timestamp,
        'temperature':random.uniform(-5,26),
        'weatherCondition':random.choice(['Sunny','Rain','Cloud','Snow']),
        'preciptation':random.uniform(0,25),
        'windSpeed':random.uniform(0,100),
        'humidity':random.uniform(0,100),
        'airQualityIndex':random.uniform(0,500)
    }

def generate_emergency_incident_data(device_id,timestamp,location):
    return {
        'id':uuid.uuid4(),
        'deviceId': device_id,
        'incidentId':uuid.uuid4(),
        'type':random.choice(['Accident','Fire','Police','Medical','None']),
        'location':location,
        'timestamp':timestamp,
        'incidentType':random.choice(['Accident','RoadBlock','TrafficJam']),
        'status':random.choice(['Active','Resolved']),
        'description':'Description of Incident'
    }

def json_serializer(obj):
    if isinstance(obj,uuid.UUID):
        return str(obj)
    raise TypeError(f'Type {type(obj)} not serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer,topic,data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data,default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()

def simulate_journey(producer,device_id):
    while True:
        vehicle_data=generate_vehicle_data(device_id)
        gps_data=generate_gps_data(device_id,vehicle_data['timestamp'])
        traffic_data=generate_traffic_camera_data(device_id,vehicle_data['timestamp'],'Nikon-Cam123')
        weather_data=generate_weather_data(device_id,vehicle_data['timestamp'],vehicle_data['location'])
        emergency_data=generate_emergency_incident_data(device_id,vehicle_data['timestamp'],vehicle_data['location'])

        # print(vehicle_data)
        # print(gps_data)
        # print(traffic_camera_data)
        # print(weather_data)
        # print(emergency_data)

        if (vehicle_data['location'][0]>=CHICAGO_COORDINATES['latitude'] and vehicle_data['location'][1]<=CHICAGO_COORDINATES['longitude']):
            print('Journey Ended')
            break

        produce_data_to_kafka(producer,VEHICLE_TOPIC,vehicle_data)
        produce_data_to_kafka(producer,GPS_TOPIC,gps_data)
        produce_data_to_kafka(producer,TRAFFIC_TOPIC,traffic_data)
        produce_data_to_kafka(producer,WEATHER_TOPIC,weather_data)
        produce_data_to_kafka(producer,EMERGENCY_TOPIC,emergency_data)

        time.sleep(1)

if __name__=="__main__":
    producer_config={
        'bootstrap.servers': KAFKA_BOOSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka Error: {err}')
    }

    producer=SerializingProducer(producer_config)

    try:
        simulate_journey(producer,'Vehicle-CodeWithPranav-123')
    
    except KeyboardInterrupt:
        print('Ended by user')
    except Exception as e:
        print(f'An error occurred: {e}')
