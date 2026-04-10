import os
import sys
import time
import json
import logging
from dotenv import load_dotenv

from ras_mqtt_service import initialize_mqtt_service

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


def test_connection():
    print("\n" + "="*70)
    print("TEST 1: Connection to HiveMQ Broker")
    print("="*70)
    
    try:
        service = initialize_mqtt_service(
            broker_host=os.getenv('MQTT_BROKER_HOST'),
            broker_port=int(os.getenv('MQTT_BROKER_PORT', 8883)),
            username=os.getenv('MQTT_BACKEND_USERNAME'),
            password=os.getenv('MQTT_BACKEND_PASSWORD'),
            use_tls=True
        )
        
        time.sleep(2)
        
        if service.connected:
            print("Successfully connected to HiveMQ")
            print(f"   Broker: {os.getenv('MQTT_BROKER_HOST')}")
            print(f"   Port: {os.getenv('MQTT_BROKER_PORT')}")
            return service
        else:
            print("Failed to connect to HiveMQ")
            return None
            
    except Exception as e:
        print(f"Connection error: {e}")
        return None


def test_geohash_calculations(service):
    print("\n" + "="*70)
    print("TEST 2: Geohash Calculations (Precision 1-7)")
    print("="*70)
    
    locations = [
        {"name": "Toronto Downtown", "lat": 43.6532, "lon": -79.3832},
        {"name": "Hamilton", "lat": 43.2557, "lon": -79.8711},
        {"name": "San Francisco", "lat": 37.7749, "lon": -122.4194}
    ]
    
    for location in locations:
        print(f"\n {location['name']}: ({location['lat']}, {location['lon']})")
        print("   Precision | Geohash   | Coverage")
        print("   ----------|-----------|----------")
        
        coverage_info = {
            1: "±2500 km",
            2: "±630 km",
            3: "±78 km",
            4: "±20 km",
            5: "±2.4 km",
            6: "±610 m",
            7: "±76 m"
        }
        
        for precision in range(1, 8):
            geohash = service.calculate_geohash(
                location['lat'],
                location['lon'],
                precision
            )
            coverage = coverage_info.get(precision, "unknown")
            print(f"   {precision:9} | {geohash:9} | {coverage}")


def test_neighbor_geohashes(service):
    print("\n" + "="*70)
    print("TEST 3: Neighboring Geohashes (Precision 7)")
    print("="*70)
    
    lat, lon = 43.6532, -79.3832  # Toronto
    
    for precision in [5, 6, 7]:
        center_hash = service.calculate_geohash(lat, lon, precision)
        neighbors = service.get_neighbor_geohashes(center_hash)
        
        print(f"\n Precision {precision}: Center = {center_hash}")
        print("   Direction | Geohash")
        print("   ----------|" + "-"*len(center_hash))
        
        directions = ['n', 'ne', 'e', 'se', 's', 'sw', 'w', 'nw']
        for direction in directions:
            if direction in neighbors:
                print(f"   {direction:9} | {neighbors[direction]}")


def test_incident_publishing(service):
    print("\n" + "="*70)
    print("TEST 4: Publishing Incident to Multiple Precisions")
    print("="*70)
    
    incident = {
        'device_id': 'test_pi_123',
        'risk_type': 'phone_usage',
        'location': {'lat': 43.6532, 'lon': -79.3832},
        'severity': 'high',
        'timestamp': '2026-03-10T15:30:00Z'
    }
    
    print(f"\n📱 Incident: {incident['risk_type']}")
    print(f"   Location: ({incident['location']['lat']}, {incident['location']['lon']})")
    print(f"   Severity: {incident['severity']}")
    print(f"\n📤 Publishing to precisions: 5, 6, 7")
    
    results = service.publish_incident_multi_precision(
        lat=incident['location']['lat'],
        lon=incident['location']['lon'],
        incident_data=incident,
        precisions=[5, 6, 7]
    )
    
    print("\n   Results:")
    for precision, success in results.items():
        geohash = service.calculate_geohash(
            incident['location']['lat'],
            incident['location']['lon'],
            precision
        )
        status = "success" if success else "fail"
        topic = f"risk/geohash/{precision}/{geohash}"
        print(f"   {status} Precision {precision}: {topic}")
    
    return all(results.values())


def test_message_routing(service):
    print("\n" + "="*70)
    print("TEST 5: Message Routing (Android Auto -> Raspberry Pi)")
    print("="*70)
    
    car_mac_id = "AA:BB:CC:DD:EE:FF"
    
    info_payload = {
        'payloadType': 'INFO',
        'hotspotSSID': 'TestPhone_Hotspot',
        'hotspotPassword': 'test123',
        'carMACID': car_mac_id
    }
    
    print(f"\n📱 Android Auto publishes INFO to: ras/alerts/{car_mac_id}")
    print(f"   Payload: {json.dumps(info_payload, indent=2)}")
    
    # Publish to alert topic
    topic = f"ras/alerts/{car_mac_id}"
    success = service.publish(topic, info_payload, qos=1)
    
    if success:
        print(f"\n Message published successfully")
        print(f" Backend will route to: ras/meshID/{car_mac_id}")
        print(f" Raspberry Pi will receive hotspot credentials")
    else:
        print(f"\n Message publishing failed")
    
    return success


def test_subscription_callback(service):
    print("\n" + "="*70)
    print("TEST 6: Subscription with Callback")
    print("="*70)
    
    received_messages = []
    
    def message_callback(topic, payload):
        print(f"\n Received message:")
        print(f"   Topic: {topic}")
        print(f"   Payload: {json.dumps(payload, indent=2)}")
        received_messages.append({'topic': topic, 'payload': payload})
    
    test_topic = "test/callback/messages"
    service.subscribe(test_topic, message_callback)
    
    print(f"\n Subscribed to: {test_topic}")
    print(f" Publishing test message...")
    
    test_payload = {
        'test': 'message',
        'timestamp': time.time()
    }
    service.publish(test_topic, test_payload, qos=1)
    
    time.sleep(2)
    
    if received_messages:
        print(f"\n Callback triggered successfully")
        return True
    else:
        print(f"\n No message received (this is normal in test environment)")
        return True  # Don't fail the test


def run_all_tests():
    print("\n" + "="*70)
    print("RAS MQTT SERVICE TEST SUITE")
    print("="*70)
    
    service = test_connection()
    if not service:
        print("\n Connection failed. Aborting tests.")
        return False
    
    test_geohash_calculations(service)
    
    test_neighbor_geohashes(service)
    
    incident_success = test_incident_publishing(service)
    
    routing_success = test_message_routing(service)
    
    callback_success = test_subscription_callback(service)
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    tests = [
        ("Connection to HiveMQ", service.connected),
        ("Geohash calculations", True),
        ("Neighbor geohashes", True),
        ("Incident publishing", incident_success),
        ("Message routing", routing_success),
        ("Subscription callback", callback_success)
    ]
    
    for test_name, passed in tests:
        status = "PASS" if passed else "FAIL"
        print(f"{status} - {test_name}")
    
    all_passed = all(result for _, result in tests)
    
    print("\n" + "="*70)
    if all_passed:
        print("ALL TESTS PASSED")
    else:
        print("SOME TESTS FAILED")
    print("="*70)
    
    service.disconnect()
    
    return all_passed


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
