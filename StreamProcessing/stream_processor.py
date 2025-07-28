import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
from kafka import KafkaConsumer, KafkaProducer
from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
from geopy.distance import geodesic
import time
import threading
from collections import defaultdict, deque

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamProcessor:
    def __init__(self, kafka_servers=['localhost:9092'], es_host='localhost:9200'):
        self.kafka_servers = kafka_servers
        
        # Kafka setup
        self.consumer = KafkaConsumer(
            '911-calls',
            bootstrap_servers=kafka_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            group_id='stream-processing-group',
            auto_offset_reset='latest'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        
        # ElasticSearch setup
        self.es = Elasticsearch([es_host])
        
        # Window storage for aggregations
        self.time_windows = {
            '1min': deque(maxlen=1000),
            '5min': deque(maxlen=1000),
            '15min': deque(maxlen=1000),
            '1hour': deque(maxlen=1000)
        }
        
        # Historical data for joining
        self.area_stats = {}
        self.incident_patterns = defaultdict(list)
        
        # Load batch data for joining
        self.load_batch_data()
        
    def load_batch_data(self):
        """Load historical batch data for stream-to-batch joining"""
        # Simulate historical area crime statistics
        self.area_stats = {
            "CENTRAL": {"avg_priority": 2.3, "crime_rate": 0.85, "response_time": 8.2},
            "RAMPART": {"avg_priority": 2.1, "crime_rate": 0.72, "response_time": 7.8},
            "SOUTHWEST": {"avg_priority": 2.5, "crime_rate": 0.68, "response_time": 9.1},
            "HOLLENBECK": {"avg_priority": 2.2, "crime_rate": 0.61, "response_time": 8.7},
            "HARBOR": {"avg_priority": 2.4, "crime_rate": 0.73, "response_time": 10.2},
            "HOLLYWOOD": {"avg_priority": 2.0, "crime_rate": 0.91, "response_time": 6.9},
            "WILSHIRE": {"avg_priority": 2.1, "crime_rate": 0.64, "response_time": 7.5}
        }
        logger.info("Loaded batch data for stream joining")

    def transformation_1_priority_escalation(self, call_data: Dict) -> Dict:
        """Complex Transformation 1: Dynamic Priority Escalation"""
        incident_type = call_data.get('incident_type', '')
        area = call_data['location']['area_name']
        current_priority = call_data.get('priority', 5)
        
        # Escalate priority based on area crime rate and incident type
        area_data = self.area_stats.get(area, {"crime_rate": 0.5})
        crime_rate = area_data["crime_rate"]
        
        escalation_factor = 0
        
        # High crime areas get priority boost
        if crime_rate > 0.8:
            escalation_factor += 1
        elif crime_rate > 0.6:
            escalation_factor += 0.5
            
        # Critical incident types
        if incident_type in ["ASSAULT", "ROBBERY", "WEAPON VIOLATION"]:
            escalation_factor += 1
        elif incident_type in ["BURGLARY", "DOMESTIC VIOLENCE"]:
            escalation_factor += 0.5
            
        new_priority = max(1, current_priority - int(escalation_factor))
        
        call_data['original_priority'] = current_priority
        call_data['escalated_priority'] = new_priority
        call_data['escalation_reason'] = f"Crime rate: {crime_rate}, Type: {incident_type}"
        
        return call_data

    def transformation_2_geographic_clustering(self, call_data: Dict) -> Dict:
        """Complex Transformation 2: Geographic Incident Clustering"""
        lat = call_data['location']['latitude']
        lon = call_data['location']['longitude']
        incident_type = call_data['incident_type']
        
        # Find nearby incidents in last 15 minutes
        recent_incidents = []
        current_time = datetime.fromisoformat(call_data['timestamp'].replace('Z', '+00:00'))
        
        for window_call in self.time_windows['15min']:
            if window_call['incident_type'] == incident_type:
                call_time = datetime.fromisoformat(window_call['timestamp'].replace('Z', '+00:00'))
                if (current_time - call_time).total_seconds() <= 900:  # 15 minutes
                    distance = geodesic(
                        (lat, lon),
                        (window_call['location']['latitude'], window_call['location']['longitude'])
                    ).kilometers
                    
                    if distance <= 2.0:  # Within 2km
                        recent_incidents.append({
                            'call_id': window_call['call_id'],
                            'distance_km': distance,
                            'time_diff_minutes': (current_time - call_time).total_seconds() / 60
                        })
        
        call_data['cluster_info'] = {
            'nearby_incidents_count': len(recent_incidents),
            'nearby_incidents': recent_incidents[:5],  # Limit to 5 most recent
            'is_cluster_event': len(recent_incidents) >= 2
        }
        
        return call_data

    def transformation_3_resource_optimization(self, call_data: Dict) -> Dict:
        """Complex Transformation 3: Dynamic Resource Allocation"""
        area = call_data['location']['area_name']
        priority = call_data.get('escalated_priority', call_data.get('priority', 5))
        incident_type = call_data['incident_type']
        
        # Calculate optimal unit allocation
        base_units = 1
        additional_units = 0
        
        # Priority-based allocation
        if priority <= 2:
            additional_units += 2
        elif priority == 3:
            additional_units += 1
            
        # Incident type requirements
        unit_requirements = {
            "FIRE": {"LAFD": 2, "EMS": 1, "LAPD": 1},
            "MEDICAL EMERGENCY": {"EMS": 2, "LAFD": 1},
            "ASSAULT": {"LAPD": 2},
            "ROBBERY": {"LAPD": 3},
            "WEAPON VIOLATION": {"LAPD": 3, "SWAT": 1}
        }
        
        recommended_units = unit_requirements.get(incident_type, {"LAPD": base_units + additional_units})
        
        # Area-based adjustment
        area_data = self.area_stats.get(area, {"response_time": 8.0})
        if area_data["response_time"] > 10.0:
            # High response time areas get extra units
            for unit_type in recommended_units:
                recommended_units[unit_type] = min(5, recommended_units[unit_type] + 1)
        
        call_data['resource_allocation'] = {
            'recommended_units': recommended_units,
            'total_units_needed': sum(recommended_units.values()),
            'allocation_reason': f"Priority: {priority}, Area response time: {area_data['response_time']}min"
        }
        
        return call_data

    def transformation_4_anomaly_detection(self, call_data: Dict) -> Dict:
        """Complex Transformation 4: Real-time Anomaly Detection"""
        area = call_data['location']['area_name']
        incident_type = call_data['incident_type']
        timestamp = datetime.fromisoformat(call_data['timestamp'].replace('Z', '+00:00'))
        
        # Calculate recent incident rates
        hour_incidents = []
        for window_call in self.time_windows['1hour']:
            call_time = datetime.fromisoformat(window_call['timestamp'].replace('Z', '+00:00'))
            if (timestamp - call_time).total_seconds() <= 3600:  # Last hour
                if window_call['location']['area_name'] == area:
                    hour_incidents.append(window_call)
        
        hourly_rate = len(hour_incidents)
        
        # Get historical average for this area and hour
        hour_of_day = timestamp.hour
        historical_avg = self.get_historical_average(area, hour_of_day, incident_type)
        
        # Detect anomalies
        anomaly_threshold = historical_avg * 2.5
        is_anomaly = hourly_rate > anomaly_threshold
        
        anomaly_score = hourly_rate / max(historical_avg, 1) if historical_avg > 0 else 1
        
        call_data['anomaly_detection'] = {
            'is_anomaly': is_anomaly,
            'anomaly_score': round(anomaly_score, 2),
            'hourly_incident_count': hourly_rate,
            'historical_average': historical_avg,
            'confidence': min(1.0, len(hour_incidents) / 10)  # More data = higher confidence
        }
        
        return call_data

    def transformation_5_predictive_routing(self, call_data: Dict) -> Dict:
        """Complex Transformation 5: AI-Powered Response Route Optimization"""
        lat = call_data['location']['latitude']
        lon = call_data['location']['longitude']
        priority = call_data.get('escalated_priority', call_data.get('priority', 5))
        
        # Simulate traffic and route analysis
        time_of_day = datetime.fromisoformat(call_data['timestamp'].replace('Z', '+00:00')).hour
        
        # Traffic multipliers by time of day
        traffic_multipliers = {
            range(6, 9): 1.8,    # Morning rush
            range(16, 19): 1.9,  # Evening rush
            range(22, 6): 0.7,   # Night
        }
        
        traffic_factor = 1.0
        for time_range, multiplier in traffic_multipliers.items():
            if time_of_day in time_range:
                traffic_factor = multiplier
                break
        
        # Calculate optimal response strategy
        base_response_time = self.area_stats.get(
            call_data['location']['area_name'], 
            {"response_time": 8.0}
        )["response_time"]
        
        estimated_response_time = base_response_time * traffic_factor
        
        # Route optimization suggestions
        route_suggestions = []
        if traffic_factor > 1.5:
            route_suggestions.append("Use alternative routes due to high traffic")
        if priority <= 2:
            route_suggestions.append("Emergency lights and sirens authorized")
        if call_data.get('cluster_info', {}).get('is_cluster_event'):
            route_suggestions.append("Coordinate with units responding to nearby incidents")
            
        call_data['routing_optimization'] = {
            'estimated_response_time_minutes': round(estimated_response_time, 1),
            'traffic_factor': traffic_factor,
            'optimal_departure_time': 'immediate' if priority <= 3 else 'within_5_minutes',
            'route_suggestions': route_suggestions,
            'coordination_needed': len(route_suggestions) > 1
        }
        
        return call_data

    def get_historical_average(self, area: str, hour: int, incident_type: str) -> float:
        """Get historical average for anomaly detection"""
        # Simulate historical data lookup
        base_rates = {
            "ASSAULT": 0.8, "ROBBERY": 0.4, "BURGLARY": 0.6,
            "DOMESTIC VIOLENCE": 0.9, "MEDICAL EMERGENCY": 1.2,
            "FIRE": 0.2, "TRAFFIC ACCIDENT": 1.0
        }
        
        base_rate = base_rates.get(incident_type, 0.5)
        
        # Adjust by time of day
        if 22 <= hour or hour <= 6:  # Night
            base_rate *= 0.6
        elif 18 <= hour <= 22:  # Evening
            base_rate *= 1.3
        elif 6 <= hour <= 18:  # Day
            base_rate *= 1.0
            
        return base_rate

    def windowed_aggregation(self, window_size: str) -> Dict:
        """Perform windowed aggregations"""
        window_data = list(self.time_windows[window_size])
        
        if not window_data:
            return {}
            
        # Priority distribution
        priorities = [call.get('escalated_priority', call.get('priority', 5)) for call in window_data]
        priority_dist = {i: priorities.count(i) for i in range(1, 6)}
        
        # Incident types
        incident_types = [call['incident_type'] for call in window_data]
        incident_counts = {}
        for incident in incident_types:
            incident_counts[incident] = incident_counts.get(incident, 0) + 1
            
        # Area analysis
        areas = [call['location']['area_name'] for call in window_data]
        area_counts = {}
        for area in areas:
            area_counts[area] = area_counts.get(area, 0) + 1
            
        # Response metrics
        dispatched_calls = [call for call in window_data if call['dispatch_info']['is_dispatched']]
        avg_response_time = np.mean([
            call['dispatch_info']['response_time_minutes'] 
            for call in dispatched_calls 
            if call['dispatch_info']['response_time_minutes']
        ]) if dispatched_calls else 0
        
        aggregation = {
            'window': window_size,
            'timestamp': datetime.now().isoformat(),
            'total_calls': len(window_data),
            'priority_distribution': priority_dist,
            'top_incident_types': dict(sorted(incident_counts.items(), key=lambda x: x[1], reverse=True)[:5]),
            'top_areas': dict(sorted(area_counts.items(), key=lambda x: x[1], reverse=True)[:5]),
            'avg_response_time': round(avg_response_time, 2),
            'dispatch_rate': round(len(dispatched_calls) / len(window_data) * 100, 1) if window_data else 0,
            'anomaly_count': len([call for call in window_data if call.get('anomaly_detection', {}).get('is_anomaly', False)]),
            'cluster_events': len([call for call in window_data if call.get('cluster_info', {}).get('is_cluster_event', False)])
        }
        
        return aggregation

    def stream_to_stream_join(self, call_data: Dict) -> Dict:
        """Join current call with recent related calls"""
        current_area = call_data['location']['area_name']
        current_type = call_data['incident_type']
        
        # Find related calls in the last 10 minutes
        related_calls = []
        current_time = datetime.fromisoformat(call_data['timestamp'].replace('Z', '+00:00'))
        
        for window_call in self.time_windows['15min']:
            call_time = datetime.fromisoformat(window_call['timestamp'].replace('Z', '+00:00'))
            time_diff = (current_time - call_time).total_seconds()
            
            if 0 < time_diff <= 600:  # Last 10 minutes, not current call
                # Same area or same incident type
                if (window_call['location']['area_name'] == current_area or 
                    window_call['incident_type'] == current_type):
                    related_calls.append({
                        'call_id': window_call['call_id'],
                        'incident_type': window_call['incident_type'],
                        'area': window_call['location']['area_name'],
                        'priority': window_call.get('escalated_priority', window_call.get('priority')),
                        'minutes_ago': round(time_diff / 60, 1)
                    })
        
        call_data['stream_join'] = {
            'related_calls_count': len(related_calls),
            'related_calls': sorted(related_calls, key=lambda x: x['minutes_ago'])[:10],
            'pattern_detected': len(related_calls) >= 3,
            'join_type': 'stream_to_stream'
        }
        
        return call_data

    def stream_to_batch_join(self, call_data: Dict) -> Dict:
        """Join stream data with batch historical data"""
        area = call_data['location']['area_name']
        incident_type = call_data['incident_type']
        
        # Join with area statistics
        area_stats = self.area_stats.get(area, {})
        
        call_data['batch_join'] = {
            'area_crime_rate': area_stats.get('crime_rate', 0.5),
            'area_avg_priority': area_stats.get('avg_priority', 3.0),
            'area_avg_response_time': area_stats.get('response_time', 8.0),
            'risk_assessment': self.calculate_risk_score(call_data, area_stats),
            'join_type': 'stream_to_batch'
        }
        
        return call_data

    def calculate_risk_score(self, call_data: Dict, area_stats: Dict) -> float:
        """Calculate risk score based on joined data"""
        score = 0.0
        
        # Priority factor
        priority = call_data.get('escalated_priority', call_data.get('priority', 5))
        score += (6 - priority) * 0.2
        
        # Area crime rate factor
        crime_rate = area_stats.get('crime_rate', 0.5)
        score += crime_rate * 0.3
        
        # Incident type factor
        high_risk_incidents = ["ASSAULT", "ROBBERY", "WEAPON VIOLATION", "DOMESTIC VIOLENCE"]
        if call_data['incident_type'] in high_risk_incidents:
            score += 0.3
            
        # Cluster factor
        if call_data.get('cluster_info', {}).get('is_cluster_event'):
            score += 0.2
            
        return min(1.0, score)

    def store_to_elasticsearch(self, processed_call: Dict):
        """Store processed call to ElasticSearch"""
        try:
            index_name = f"911-calls-{datetime.now().strftime('%Y-%m')}"
            
            self.es.index(
                index=index_name,
                id=processed_call['call_id'],
                body=processed_call
            )
            
            logger.info(f"Stored call {processed_call['call_id']} to ElasticSearch")
            
        except Exception as e:
            logger.error(f"Failed to store to ElasticSearch: {e}")

    def store_aggregation_to_elasticsearch(self, aggregation: Dict):
        """Store windowed aggregations to ElasticSearch"""
        try:
            index_name = f"911-aggregations-{datetime.now().strftime('%Y-%m')}"
            doc_id = f"{aggregation['window']}_{int(datetime.now().timestamp())}"
            
            self.es.index(
                index=index_name,
                id=doc_id,
                body=aggregation
            )
            
            logger.info(f"Stored {aggregation['window']} aggregation to ElasticSearch")
            
        except Exception as e:
            logger.error(f"Failed to store aggregation to ElasticSearch: {e}")

    def add_to_windows(self, call_data: Dict):
        """Add call to time windows for aggregation"""
        call_time = datetime.fromisoformat(call_data['timestamp'].replace('Z', '+00:00'))
        current_time = datetime.now()
        
        # Only add recent calls to windows
        if (current_time - call_time).total_seconds() <= 3600:  # Last hour
            for window in self.time_windows:
                self.time_windows[window].append(call_data)

    def process_stream(self):
        """Main stream processing loop"""
        logger.info("Starting stream processor...")
        
        # Start aggregation thread
        aggregation_thread = threading.Thread(target=self.run_aggregations)
        aggregation_thread.daemon = True
        aggregation_thread.start()
        
        try:
            for message in self.consumer:
                call_data = message.value
                logger.info(f"Processing call: {call_data['call_id']}")
                
                # Apply all transformations
                processed_call = call_data.copy()
                processed_call = self.transformation_1_priority_escalation(processed_call)
                processed_call = self.transformation_2_geographic_clustering(processed_call)
                processed_call = self.transformation_3_resource_optimization(processed_call)
                processed_call = self.transformation_4_anomaly_detection(processed_call)
                processed_call = self.transformation_5_predictive_routing(processed_call)
                
                # Apply joins
                processed_call = self.stream_to_stream_join(processed_call)
                processed_call = self.stream_to_batch_join(processed_call)
                
                # Add to windows for aggregation
                self.add_to_windows(processed_call)
                
                # Store to ElasticSearch
                self.store_to_elasticsearch(processed_call)
                
                # Send to processed topic
                self.producer.send(
                    '911-calls-processed',
                    key=processed_call['call_id'],
                    value=processed_call
                )
                
                logger.info(f"Completed processing for call: {call_data['call_id']}")
                
        except KeyboardInterrupt:
            logger.info("Stopping stream processor...")
        finally:
            self.consumer.close()
            self.producer.close()

    def run_aggregations(self):
        """Run windowed aggregations periodically"""
        while True:
            try:
                time.sleep(60)  # Run every minute
                
                # Generate aggregations for all windows
                for window_size in self.time_windows.keys():
                    aggregation = self.windowed_aggregation(window_size)
                    if aggregation:
                        # Send to aggregation topic
                        self.producer.send(
                            '911-aggregations',
                            key=f"{window_size}_{int(datetime.now().timestamp())}",
                            value=aggregation
                        )
                        
                        # Store to ElasticSearch
                        self.store_aggregation_to_elasticsearch(aggregation)
                        
                logger.info("Completed windowed aggregations")
                
            except Exception as e:
                logger.error(f"Error in aggregation thread: {e}")
                time.sleep(60)

if __name__ == "__main__":
    processor = StreamProcessor()
    processor.process_stream()
