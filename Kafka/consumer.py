import json
import logging
from kafka import KafkaConsumer
from datetime import datetime
import csv
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Call911Consumer:
    def __init__(self, topic='911-calls', bootstrap_servers=['localhost:9092']):
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            group_id='call-processing-group',
            auto_offset_reset='latest'
        )
        
        self.output_dir = '/tmp/911_calls'
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.call_count = 0
        self.priority_stats = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        self.incident_stats = {}
        
    def process_call(self, call_data):
        self.call_count += 1
        
        priority = call_data.get('priority', 5)
        self.priority_stats[priority] += 1
        
        incident_type = call_data.get('incident_type', 'UNKNOWN')
        self.incident_stats[incident_type] = self.incident_stats.get(incident_type, 0) + 1
        
        if priority <= 2:
            logger.warning(f"HIGH PRIORITY CALL: {call_data['call_id']} - {incident_type} in {call_data['location']['area_name']}")
        
        self.save_to_csv(call_data)
        
        if self.call_count % 10 == 0:
            self.print_stats()
    
    def save_to_csv(self, call_data):
        filename = f"{self.output_dir}/calls_{datetime.now().strftime('%Y%m%d')}.csv"
        file_exists = os.path.isfile(filename)
        
        with open(filename, 'a', newline='') as csvfile:
            fieldnames = [
                'call_id', 'timestamp', 'call_type', 'incident_type', 'priority',
                'address', 'area_name', 'latitude', 'longitude', 'status',
                'is_dispatched', 'units_count', 'response_time'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            row = {
                'call_id': call_data['call_id'],
                'timestamp': call_data['timestamp'],
                'call_type': call_data['call_type'],
                'incident_type': call_data['incident_type'],
                'priority': call_data['priority'],
                'address': call_data['location']['address'],
                'area_name': call_data['location']['area_name'],
                'latitude': call_data['location']['latitude'],
                'longitude': call_data['location']['longitude'],
                'status': call_data['status'],
                'is_dispatched': call_data['dispatch_info']['is_dispatched'],
                'units_count': len(call_data['dispatch_info']['units_dispatched']),
                'response_time': call_data['dispatch_info']['response_time_minutes']
            }
            
            writer.writerow(row)
    
    def print_stats(self):
        print("\n" + "="*50)
        print(f"PROCESSED CALLS: {self.call_count}")
        print("\nPRIORITY BREAKDOWN:")
        for priority, count in self.priority_stats.items():
            percentage = (count / self.call_count) * 100 if self.call_count > 0 else 0
            print(f"  Priority {priority}: {count} ({percentage:.1f}%)")
        
        print("\nTOP INCIDENT TYPES:")
        sorted_incidents = sorted(self.incident_stats.items(), key=lambda x: x[1], reverse=True)[:5]
        for incident, count in sorted_incidents:
            percentage = (count / self.call_count) * 100 if self.call_count > 0 else 0
            print(f"  {incident}: {count} ({percentage:.1f}%)")
        print("="*50 + "\n")
    
    def start_consuming(self):
        logger.info(f"Starting consumer for topic: {self.topic}")
        logger.info("Press Ctrl+C to stop...")
        
        try:
            for message in self.consumer:
                call_data = message.value
                logger.info(f"Received call: {call_data['call_id']} - {call_data['incident_type']}")
                self.process_call(call_data)
                
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        finally:
            self.consumer.close()
            self.print_stats()

def main():
    consumer = Call911Consumer()
    consumer.start_consuming()

if __name__ == "__main__":
    main()
