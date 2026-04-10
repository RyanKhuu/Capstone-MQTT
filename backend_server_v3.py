from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import logging
import pygeohash as geohash_lib
from datetime import datetime
from dotenv import load_dotenv
import json

from ras_mqtt_service import initialize_mqtt_service, get_mqtt_service

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

app = Flask(__name__)
CORS(app)

mqtt_service = initialize_mqtt_service(
    broker_host=os.getenv('MQTT_BROKER_HOST'),
    broker_port=int(os.getenv('MQTT_BROKER_PORT', 8883)),
    username=os.getenv('MQTT_BACKEND_USERNAME'),
    password=os.getenv('MQTT_BACKEND_PASSWORD'),
    use_tls=os.getenv('MQTT_USE_TLS', 'True').lower() == 'true'
)

GEOHASH_PRECISIONS = [7]


def transform_for_android_auto(payload: dict):
    severity_map = {
        'low': 'LOW',
        'medium': 'MEDIUM',
        'high': 'HIGH',
        'critical': 'CRITICAL'
    }
    
    behaviour_map = {
        'phone_usage': 'DISTRACTED',
        'unsafe_turn': 'TURN',
        'hard_braking': 'BRAKING',
        'hard_acceleration': 'ACCELERATION',
        'no_signal': 'NO_SIGNAL',
        'tailgating': 'TAILGATING',
        'lane_departure': 'LANE_DEPARTURE',
        'drowsy_driving': 'DROWSY'
    }

    severity = payload.get('severity', 'low')
    risk_types = payload.get('risk_types', [])
    direction = payload.get('direction', 'Unknown')
    location = payload.get('location', {})

    return {
        'risk': severity_map.get(severity.lower(), 'LOW'),
        'catBehaviours': [behaviour_map.get(r.lower(), 'UNKNOWN') for r in risk_types],
        'direction': direction,
        'lat': float(location.get('lat', 0.0)),
        'lon': float(location.get('lon', 0.0))
    }


def handle_incident_from_pi(topic: str, payload: dict):
    try:
        logger.info(f"Received incident from Pi via MQTT: {topic}")
        logger.debug(f"Original payload: {payload}")

        is_valid, error = validate_incident_data(payload)
        if not is_valid:
            logger.error(f"Invalid incident data: {error}")
            return

        android_payload = transform_for_android_auto(payload)
        logger.info(f"Transformed payload for Android Auto: {android_payload}")

        lat = float(payload['location']['lat'])
        lon = float(payload['location']['lon'])

        service = get_mqtt_service()
        if not service or not service.connected:
            logger.error("MQTT service not available")
            return

        publish_results = service.publish_incident_multi_precision(
            lat=lat,
            lon=lon,
            incident_data=android_payload,
            precisions=GEOHASH_PRECISIONS
        )

        incident_geohash = service.calculate_geohash(lat, lon, precision=7)

        logger.info(f"Incident processed and published: {android_payload} at geohash {incident_geohash}")
        logger.info(f"Published to precisions: {list(publish_results.keys())}")

    except Exception as e:
        logger.error(f"Error processing incident from Pi: {e}", exc_info=True)


logger.info("Subscribing to incidents/reports for Raspberry Pi messages...")
mqtt_service.subscribe("incidents/reports", handle_incident_from_pi)


VALID_RISK_TYPES = [
    'phone_usage',
    'unsafe_turn',
    'hard_braking',
    'hard_acceleration',
    'no_signal',
    'tailgating',
    'lane_departure',
    'drowsy_driving'
]

VALID_SEVERITIES = ['low', 'medium', 'high', 'critical']


def validate_incident_data(data):
    """
    Returns:
        (is_valid, error_message)
    """
    required_fields = ['device_id', 'risk_types', 'location', 'severity', 'timestamp']
    for field in required_fields:
        if field not in data:
            return False, f"Missing required field: {field}"
    
    if not isinstance(data['risk_types'], list) or len(data['risk_types']) == 0:
        return False, "risk_types must be a non-empty array"
    
    for risk in data['risk_types']:
        if risk not in VALID_RISK_TYPES:
            return False, f"Invalid risk_types '{risk}'. Must be one of: {', '.join(VALID_RISK_TYPES)}"
    
    if data['severity'] not in VALID_SEVERITIES:
        return False, f"Invalid severity. Must be one of: {', '.join(VALID_SEVERITIES)}"
    
    if 'lat' not in data['location'] or 'lon' not in data['location']:
        return False, "Location must contain 'lat' and 'lon'"
    
    try:
        lat = float(data['location']['lat'])
        lon = float(data['location']['lon'])
        
        if not (-90 <= lat <= 90):
            return False, "Latitude must be between -90 and 90"
        if not (-180 <= lon <= 180):
            return False, "Longitude must be between -180 and 180"
    except (ValueError, TypeError):
        return False, "Invalid lat/lon values"
    
    try:
        datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return False, "Invalid timestamp format. Use ISO 8601 format"
    
    return True, None


def validate_location_update(data):
    """
    Returns:
        (is_valid, error_message)
    """
    required_fields = ['user_id', 'lat', 'lon']
    for field in required_fields:
        if field not in data:
            return False, f"Missing required field: {field}"
    
    try:
        lat = float(data['lat'])
        lon = float(data['lon'])
        
        if not (-90 <= lat <= 90):
            return False, "Latitude must be between -90 and 90"
        if not (-180 <= lon <= 180):
            return False, "Longitude must be between -180 and 180"
    except (ValueError, TypeError):
        return False, "Invalid lat/lon values"
    
    return True, None



@app.route('/health', methods=['GET'])
def health_check():
    service = get_mqtt_service()
    
    return jsonify({
        'status': 'healthy',
        'mqtt_connected': service.connected if service else False,
        'mqtt_stats': service.get_stats() if service else {}
    }), 200


@app.route('/api/report-incident', methods=['POST'])
def report_incident():
    """
    Expected payload:
    {
        "device_id": "pi_vehicle_123",
        "risk_types": ["phone_usage"],
        "location": {"lat": 43.6532, "lon": -79.3832},
        "severity": "high",
        "timestamp": "2026-02-11T14:30:00Z",
        "direction": "forward"
    }
    """
    try:
        data = request.get_json()
        
        is_valid, error = validate_incident_data(data)
        if not is_valid:
            return jsonify({'error': error}), 400
        
        lat = float(data['location']['lat'])
        lon = float(data['location']['lon'])
        
        service = get_mqtt_service()
        if not service or not service.connected:
            logger.error("MQTT service not available")
            return jsonify({'error': 'MQTT service unavailable'}), 503
        
        android_payload = transform_for_android_auto(data)
        
        publish_results = service.publish_incident_multi_precision(
            lat=lat,
            lon=lon,
            incident_data=android_payload,
            precisions=GEOHASH_PRECISIONS
        )
        
        incident_geohash = service.calculate_geohash(lat, lon, precision=7)
        
        logger.info(f"Incident processed: {data['risk_types']} at geohash {incident_geohash}")
        
        return jsonify({
            'status': 'success',
            'geohash': incident_geohash,
            'geohashes_published': {
                str(precision): service.calculate_geohash(lat, lon, precision)
                for precision in GEOHASH_PRECISIONS
            },
            'mqtt_published': all(publish_results.values()),
            'publish_results': publish_results
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing incident: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/geohash/calculate', methods=['POST'])
def calculate_geohash():
    """
    Expected payload:
    {
        "lat": 43.6532,
        "lon": -79.3832,
        "precision": 7
    }
    """
    try:
        data = request.get_json()
        
        if 'lat' not in data or 'lon' not in data:
            return jsonify({'error': 'Missing lat or lon'}), 400
        
        lat = float(data['lat'])
        lon = float(data['lon'])
        precision = int(data.get('precision', 7))
        
        if not (1 <= precision <= 12):
            return jsonify({'error': 'Precision must be between 1 and 12'}), 400
        
        service = get_mqtt_service()
        if not service:
            return jsonify({'error': 'MQTT service unavailable'}), 503
        
        geohash = service.calculate_geohash(lat, lon, precision)
        neighbors = service.get_neighbor_geohashes(geohash)
        
        return jsonify({
            'geohash': geohash,
            'precision': precision,
            'location': {'lat': lat, 'lon': lon},
            'neighbors': neighbors
        }), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Error calculating geohash: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/stats', methods=['GET'])
def get_stats():
    service = get_mqtt_service()
    
    stats = {
        'mqtt': service.get_stats() if service else {}
    }
    
    return jsonify(stats), 200


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    debug = os.getenv('DEBUG', 'False').lower() == 'true'
    
    logger.info(f"Starting RAS Backend Server on port {port}")
    logger.info(f"MQTT Broker: {os.getenv('MQTT_BROKER_HOST')}")
    logger.info(f"Geohash precisions: {GEOHASH_PRECISIONS}")
    
    app.run(host='0.0.0.0', port=5001, debug=True, use_reloader=False)