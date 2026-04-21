from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import logging
import pygeohash as geohash_lib
from datetime import datetime
from dotenv import load_dotenv
import json

from ras_mqtt_service import initialize_mqtt_service, get_mqtt_service

# Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

app = Flask(__name__)
CORS(app)

# Constants
GEOHASH_PRECISIONS = [7]
VALID_SEVERITIES = ['low', 'medium', 'high', 'critical']
VALID_RISK_TYPES = [
    'phone_usage', 'unsafe_turn', 'hard_braking', 'hard_acceleration',
    'no_signal', 'tailgating', 'lane_departure', 'drowsy_driving',
    'sharp steering', 'no turn signal while turning',
    'drowsy with failed observation', 'driver is drowsy',
    'failed to complete observation', 'alert but completely distracted'
]

mqtt_service = initialize_mqtt_service(
    broker_host=os.getenv('MQTT_BROKER_HOST'),
    broker_port=int(os.getenv('MQTT_BROKER_PORT', 8883)),
    username=os.getenv('MQTT_BACKEND_USERNAME'),
    password=os.getenv('MQTT_BACKEND_PASSWORD'),
    use_tls=os.getenv('MQTT_USE_TLS', 'True').lower() == 'true'
)

def transform_for_android_auto(payload: dict):
    severity_map = {
        'low': 'LOW',
        'medium': 'MEDIUM',
        'high': 'HIGH',
        'critical': 'CRITICAL'
    }
    
    behaviour_map = {
        'sharp steering': 'Unsafe Turn',
        'no turn signal while turning': 'No turn signal',
        'drowsy with failed observation': 'Drowsy Driver',
        'driver is drowsy': 'Drowsy Driver',
        'phone_usage': 'Distracted Driver',
        'hard braking': 'Hard Braking',
        'aggressive throttle': 'Aggressive Throttle',
        'hard acceleration': 'Hard Acceleration',
        'tailgating': 'Tailgating',
        'Unsignaled lane departure': 'Lane departure',
        'drowsy driving': 'Drowsy Driver',
        'High-speed swerving': "High Speed Swerving",
        'Unsafe reverse speed': 'Unsafe Reverse Speed',
        'Late braking reaction': 'Late Braking',
        'failed to complete observation': 'Did not complete observation',
        'Alert but completely distracted': 'Distracted Driving'
    }

    severity = str(payload.get('severity', 'low')).lower()

    raw_risk_data = payload.get('risk_types', "")
    if isinstance(raw_risk_data, str):
        risk_list = [r.strip().lower() for r in raw_risk_data.split(',') if r.strip()]
    elif isinstance(raw_risk_data, list):
        risk_list = [str(r).lower() for r in raw_risk_data]
    else:
        risk_list = []

    direction = payload.get('direction', 'Unknown')
    location = payload.get('location', {})

    vehicle_make = payload.get('vehicle_make', 'Unknown')
    vehicle_model = payload.get('vehicle_model', 'Unknown')
    vehicle_color = payload.get('color', 'Unknown')

    return {
        'risk': severity_map.get(severity, 'LOW'),
        'catBehaviours': [behaviour_map.get(r, 'UNKNOWN') for r in risk_list],
        'direction': direction,
        'lat': float(location.get('lat', 0.0)),
        'lon': float(location.get('lon', 0.0)),
        'vehicle_make': vehicle_make,
        'vehicle_model': vehicle_model,
        'color': vehicle_color
    }

def validate_incident_data(data):
    """
    Returns: (is_valid, error_message)
    """
    required_fields = ['device_id', 'risk_types', 'location', 'severity', 'timestamp']
    for field in required_fields:
        if field not in data:
            return False, f"Missing required field: {field}"

    # ✅ Validate vehicle fields (optional but must be strings if present)
    for field in ['vehicle_make', 'vehicle_model', 'vehicle_color']:
        if field in data and not isinstance(data[field], str):
            return False, f"{field} must be a string"

    # Handle risk_types
    risk_data = data['risk_types']
    if isinstance(risk_data, str):
        if not risk_data.strip():
            return False, "risk_types string cannot be empty"
        risks_to_check = [r.strip().lower() for r in risk_data.split(',') if r.strip()]
    elif isinstance(risk_data, list):
        if len(risk_data) == 0:
            return False, "risk_types array cannot be empty"
        risks_to_check = [str(r).lower() for r in risk_data]
    else:
        return False, "risk_types must be a string or a non-empty array"

    for risk in risks_to_check:
        if risk not in VALID_RISK_TYPES:
            logger.warning(f"Unrecognized risk type: {risk}")

    if str(data['severity']).lower() not in VALID_SEVERITIES:
        return False, f"Invalid severity. Must be one of: {', '.join(VALID_SEVERITIES)}"

    if 'lat' not in data['location'] or 'lon' not in data['location']:
        return False, "Location must contain 'lat' and 'lon'"

    try:
        lat = float(data['location']['lat'])
        lon = float(data['location']['lon'])
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            return False, "Lat/Lon out of bounds"
    except (ValueError, TypeError):
        return False, "Invalid lat/lon values"

    try:
        datetime.fromisoformat(str(data['timestamp']).replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return False, "Invalid timestamp format"

    return True, None

def handle_incident_from_pi(topic: str, payload: dict):
    try:
        logger.info(f"Received incident from Pi via MQTT: {topic}")

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
        logger.info(f"Incident processed and published at geohash {incident_geohash}")

    except Exception as e:
        logger.error(f"Error processing incident from Pi: {e}", exc_info=True)

# MQTT Subscription
logger.info("Subscribing to incidents/reports for Raspberry Pi messages...")
mqtt_service.subscribe("incidents/reports", handle_incident_from_pi)

# --- Flask Routes ---

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
    try:
        data = request.get_json()

        is_valid, error = validate_incident_data(data)
        if not is_valid:
            return jsonify({'error': error}), 400

        android_payload = transform_for_android_auto(data)
        service = get_mqtt_service()

        lat = float(data['location']['lat'])
        lon = float(data['location']['lon'])

        publish_results = service.publish_incident_multi_precision(
            lat=lat,
            lon=lon,
            incident_data=android_payload,
            precisions=GEOHASH_PRECISIONS
        )

        return jsonify({
            'status': 'success',
            'mqtt_published': all(publish_results.values())
        }), 200

    except Exception as e:
        logger.error(f"Error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/geohash/calculate', methods=['POST'])
def calculate_geohash():
    try:
        data = request.get_json()
        lat, lon = float(data['lat']), float(data['lon'])
        precision = int(data.get('precision', 7))

        service = get_mqtt_service()
        geohash = service.calculate_geohash(lat, lon, precision)

        return jsonify({
            'geohash': geohash,
            'neighbors': service.get_neighbor_geohashes(geohash)
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)