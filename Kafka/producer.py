import json
import random
import time
import uuid
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer

LA_AREAS = [
    "CENTRAL", "RAMPART", "SOUTHWEST", "HOLLENBECK", "HARBOR", "HOLLYWOOD",
    "WILSHIRE", "WEST LA", "VAN NUYS", "WEST VALLEY", "NORTHEAST", "77TH STREET",
    "NEWTON", "PACIFIC", "N HOLLYWOOD", "FOOTHILL", "DEVONSHIRE", "SOUTHEAST",
    "MISSION", "OLYMPIC", "TOPANGA"
]

INCIDENT_TYPES = [
    "BURGLARY", "ROBBERY", "ASSAULT", "DOMESTIC VIOLENCE", "TRAFFIC ACCIDENT",
    "SUSPICIOUS ACTIVITY", "NOISE COMPLAINT", "MEDICAL EMERGENCY", "FIRE",
    "VANDALISM", "THEFT", "DRUG ACTIVITY", "WEAPON VIOLATION", "MISSING PERSON",
    "WELFARE CHECK", "ALARM", "DISTURBANCE", "FRAUD", "VEHICLE THEFT"
]

LA_COORDINATES = {
    "min_lat": 33.7037,
    "max_lat": 34.3373,
    "min_lon": -118.6681,
    "max_lon": -118.1553
}

UNIT_PREFIXES = ["LAPD", "LAFD", "EMS"]

def generate_phone_number():
    return f"XXX-XXX-{random.randint(1000, 9999)}"

def generate_coordinates():
    lat = random.uniform(LA_COORDINATES["min_lat"], LA_COORDINATES["max_lat"])
    lon = random.uniform(LA_COORDINATES["min_lon"], LA_COORDINATES["max_lon"])
    return round(lat, 6), round(lon, 6)

def generate_address():
    street_num = random.randint(100, 9999)
    street_names = ["MAIN ST", "BROADWAY", "FIGUEROA ST", "WILSHIRE BLVD", "SUNSET BLVD", 
                   "HOLLYWOOD BLVD", "VENTURA BLVD", "PICO BLVD", "OLYMPIC BLVD", "MELROSE AVE"]
    return f"{street_num} {random.choice(street_names)}"

def generate_units(incident_type):
    units = []
    
    if incident_type in ["MEDICAL EMERGENCY", "FIRE"]:
        units.append(f"LAFD-{random.randint(1, 99)}")
        units.append(f"EMS-{random.randint(1, 50)}")
    
    if incident_type in ["BURGLARY", "ROBBERY", "ASSAULT", "WEAPON VIOLATION"]:
        units.extend([f"LAPD-{random.randint(1, 999)}" for _ in range(random.randint(1, 3))])
    else:
        units.append(f"LAPD-{random.randint(1, 999)}")
    
    return units

def generate_911_call():
    call_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()
    call_type = random.choices(
        ["EMERGENCY", "NON_EMERGENCY", "TRANSFER"],
        weights=[70, 25, 5]
    )[0]
    
    incident_type = random.choice(INCIDENT_TYPES)
    priority = random.choices(
        [1, 2, 3, 4, 5],
        weights=[15, 25, 35, 20, 5]
    )[0]
    
    lat, lon = generate_coordinates()
    area_name = random.choice(LA_AREAS)
    
    is_dispatched = random.choice([True, False]) if call_type == "EMERGENCY" else random.choices([True, False], weights=[80, 20])[0]
    
    call_data = {
        "call_id": call_id,
        "timestamp": timestamp,
        "call_type": call_type,
        "incident_type": incident_type,
        "priority": priority,
        "location": {
            "address": generate_address(),
            "area_name": area_name,
            "latitude": lat,
            "longitude": lon
        },
        "caller_info": {
            "phone_number": generate_phone_number(),
            "is_anonymous": random.choice([True, False])
        },
        "dispatch_info": {
            "units_dispatched": generate_units(incident_type) if is_dispatched else [],
            "response_time_minutes": round(random.uniform(2.0, 15.0), 1) if is_dispatched else None,
            "is_dispatched": is_dispatched
        },
        "status": random.choices(
            ["RECEIVED", "PROCESSING", "DISPATCHED", "CLOSED"],
            weights=[20, 30, 35, 15]
        )[0]
    }
    
    return call_data

def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None
    )

    topic = os.getenv('KAFKA_TOPIC', '911-calls')
    
    print(f"Starting 911 call generator. Sending to topic: {topic}")
    print("Press Ctrl+C to stop...")
    
    try:
        while True:
            call_data = generate_911_call()
            
            producer.send(
                topic,
                key=call_data['call_id'],
                value=call_data
            )
            
            print(f"Sent call: {call_data['call_id']} - {call_data['incident_type']} in {call_data['location']['area_name']}")
            
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\nStopping call generator...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
