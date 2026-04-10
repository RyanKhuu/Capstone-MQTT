import paho.mqtt.client as mqtt
import json
import logging
import pygeohash as pgh
from typing import Dict, Optional, List, Callable
import threading
import time

logger = logging.getLogger(__name__)


class RASMQTTService:
    """
    Handles:
    - Connection to HiveMQ broker
    - Publishing incident alerts to geographic topics
    - Routing INFO messages between Android Auto and Raspberry Pi
    - Neighbor geohash calculations
    - Message callbacks
    """
    
    def __init__(
        self,
        broker_host: str,
        broker_port: int,
        username: str,
        password: str,
        use_tls: bool = True
    ):
        """
        Args:
            broker_host: HiveMQ cluster hostname
            broker_port: MQTT port (8883 for TLS, 1883 for non-TLS)
            username: MQTT username
            password: MQTT password
            use_tls: Whether to use TLS encryption
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        
        self.client = None
        self.connected = False
        self.message_callbacks = {}
        
        # Statistics
        self.stats = {
            'messages_published': 0,
            'messages_received': 0,
            'info_messages_routed': 0,
            'connection_errors': 0
        }
    
    def connect(self):
        """Connect to MQTT broker"""
        try:
            self.client = mqtt.Client(
                client_id="ras_backend_server",
                clean_session=True,
                protocol=mqtt.MQTTv311,
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2
            )
            
            self.client.username_pw_set(self.username, self.password)
            
            if self.use_tls:
                import ssl
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                self.client.tls_set_context(context)
                
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message
            self.client.on_publish = self._on_publish
            
            logger.info(f"Connecting to MQTT broker at {self.broker_host}:{self.broker_port}")
            self.client.connect(self.broker_host, self.broker_port, keepalive=60)
            
            self.client.loop_start()
            
            timeout = 10
            start_time = time.time()
            while not self.connected and (time.time() - start_time) < timeout:
                time.sleep(0.1)
            
            if not self.connected:
                raise Exception("Failed to connect to MQTT broker within timeout")
            
            logger.info("MQTT Service initialized and connected")
            
        except Exception as e:
            logger.error(f"Failed to initialize MQTT service: {e}")
            self.stats['connection_errors'] += 1
            raise
    
    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            logger.info("Connected to MQTT broker successfully")
            self.connected = True
            
            self.subscribe("ras/alerts/+", self._handle_alert_message)
            logger.info("Subscribed to ras/alerts/+ for message routing")
            
        else:
            error_messages = {
                1: "Connection refused - incorrect protocol version",
                2: "Connection refused - invalid client identifier",
                3: "Connection refused - server unavailable",
                4: "Connection refused - bad username or password",
                5: "Connection refused - not authorized"
            }
            error_msg = error_messages.get(reason_code, f"Unknown error code: {reason_code}")
            logger.error(f"Failed to connect to MQTT broker: {error_msg}")
            self.connected = False
            self.stats['connection_errors'] += 1
    
    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        self.connected = False
        if reason_code != 0:
            logger.warning(f"⚠️ Unexpected MQTT disconnection (code: {reason_code}). Reconnecting...")
            self.stats['connection_errors'] += 1
        else:
            logger.info("Disconnected from MQTT broker")
    
    def _on_message(self, client, userdata, msg, properties=None):
        try:
            topic = msg.topic
            payload = json.loads(msg.payload.decode())
            
            self.stats['messages_received'] += 1
            logger.debug(f"Received message on topic {topic}")
            
            for topic_pattern, callback in self.message_callbacks.items():
                if self._topic_matches(topic, topic_pattern):
                    callback(topic, payload)
                    
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message payload as JSON: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)
    
    def _on_publish(self, client, userdata, mid, reason_code=None, properties=None):
        logger.debug(f"Message {mid} published successfully")
        self.stats['messages_published'] += 1
    
    def _topic_matches(self, topic: str, pattern: str) -> bool:
        topic_parts = topic.split('/')
        pattern_parts = pattern.split('/')
        
        if len(topic_parts) != len(pattern_parts):
            return False
        
        for topic_part, pattern_part in zip(topic_parts, pattern_parts):
            if pattern_part == '+':
                continue
            elif pattern_part == '#':
                return True
            elif topic_part != pattern_part:
                return False
        
        return True
    
    def subscribe(self, topic: str, callback: Callable[[str, Dict], None]):
        """
        Args:
            topic: MQTT topic (can include wildcards + and #)
            callback: Function to call when message received
        """
        if not self.connected:
            logger.error("Cannot subscribe - not connected to MQTT broker")
            return False
        
        try:
            self.client.subscribe(topic, qos=1)
            self.message_callbacks[topic] = callback
            logger.info(f"Subscribed to topic: {topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to subscribe to {topic}: {e}")
            return False
    
    def publish(self, topic: str, payload: Dict, qos: int = 1) -> bool:
        """
        Args:
            topic: MQTT topic
            payload: Message payload (will be JSON encoded)
            qos: Quality of Service (0, 1, or 2)
            
        Returns:
            True if published successfully
        """
        if not self.connected:
            logger.error("Cannot publish - not connected to MQTT broker")
            return False
        
        try:
            payload_json = json.dumps(payload)
            result = self.client.publish(topic, payload_json, qos=qos)
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info(f"Published to topic: {topic}")
                return True
            else:
                logger.error(f"Failed to publish to {topic}: {result.rc}")
                return False
                
        except Exception as e:
            logger.error(f"Error publishing to {topic}: {e}")
            return False
    
    def _handle_alert_message(self, topic: str, payload: Dict):
        try:
            payload_type = payload.get('payloadType')
            
            if payload_type == 'INFO':
                parts = topic.split('/')
                
                if len(parts) >= 3:
                    car_mac_id = parts[2]
                    
                    pi_topic = f"ras/meshID/{car_mac_id}"
                    
                    logger.info(f"Routing INFO message: {topic} → {pi_topic}")
                    logger.debug(f"INFO payload: {payload}")
                    
                    self.publish(pi_topic, payload, qos=1)
                    self.stats['info_messages_routed'] += 1
                else:
                    logger.warning(f"Invalid topic format: {topic}")
            else:
                logger.debug(f"Received non-INFO message on {topic}: {payload_type}")
                
        except Exception as e:
            logger.error(f"Error handling alert message: {e}", exc_info=True)
    
    def calculate_geohash(self, lat: float, lon: float, precision: int = 5) -> str:
        """
        Calculate geohash for given coordinates
        
        Args:
            lat: Latitude
            lon: Longitude
            precision: Geohash precision (1-12)
                      1 = ±2500 km
                      2 = ±630 km
                      3 = ±78 km
                      4 = ±20 km
                      5 = ±2.4 km (default)
                      6 = ±610 m
                      7 = ±76 m
                      8 = ±19 m
                      9 = ±2.4 m
                      10 = ±60 cm
                      11 = ±7.4 cm
                      12 = ±1.9 cm
        
        Returns:
            Geohash string
        """
        if not (1 <= precision <= 12):
            raise ValueError("Precision must be between 1 and 12")
        
        return pgh.encode(lat, lon, precision=precision)
    
    def get_neighbor_geohashes(self, geohash_code: str) -> Dict[str, str]:
        try:
            return {
                "n":  pgh.get_adjacent(geohash_code, "top"),
                "ne": pgh.get_adjacent(pgh.get_adjacent(geohash_code, "top"), "right"),
                "e":  pgh.get_adjacent(geohash_code, "right"),
                "se": pgh.get_adjacent(pgh.get_adjacent(geohash_code, "bottom"), "right"),
                "s":  pgh.get_adjacent(geohash_code, "bottom"),
                "sw": pgh.get_adjacent(pgh.get_adjacent(geohash_code, "bottom"), "left"),
                "w":  pgh.get_adjacent(geohash_code, "left"),
                "nw": pgh.get_adjacent(pgh.get_adjacent(geohash_code, "top"), "left"),
            }
        except Exception as e:
            logger.error(f"Error getting neighbors for {geohash_code}: {e}")
        return {}
    
    def publish_incident(
        self,
        lat: float,
        lon: float,
        incident_data: Dict,
        precision: int = 5,
        include_neighbors: bool = True
    ) -> bool:
        """
        Publish incident alert to geographic topics
        
        Args:
            lat: Incident latitude
            lon: Incident longitude
            incident_data: Incident details
            precision: Geohash precision (1-12)
            include_neighbors: Whether to publish to neighboring geohashes
            
        Returns:
            True if published successfully to primary geohash
        """
        try:
            # Calculate geohash
            geohash = self.calculate_geohash(lat, lon, precision)
            
            # Add geohash to incident data
            incident_data['geohash'] = geohash
            incident_data['geohash_precision'] = precision
            
            # Publish to primary geohash
            primary_topic = f"risk/geohash/{precision}/{geohash}"
            success = self.publish(primary_topic, incident_data, qos=1)
            
            if success:
                logger.info(f"Published incident to geohash {geohash} (precision {precision})")
            
            # Publish to neighboring geohashes for border coverage
            if include_neighbors:
                neighbors = self.get_neighbor_geohashes(geohash)
                for direction, neighbor_hash in neighbors.items():
                    neighbor_topic = f"risk/geohash/{precision}/{neighbor_hash}"
                    self.publish(neighbor_topic, incident_data, qos=1)
                    logger.debug(f"Published to neighbor {direction}: {neighbor_hash}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error publishing incident: {e}", exc_info=True)
            return False
    
    def publish_incident_multi_precision(
        self,
        lat: float,
        lon: float,
        incident_data: Dict,
        precisions: List[int] = [5, 6, 7]
    ) -> Dict[int, bool]:
        """
        Publish incident to multiple precision levels
        
        This allows different subscribers to receive alerts at different
        geographic resolutions (e.g., 5km, 600m, 75m)
        
        Args:
            lat: Incident latitude
            lon: Incident longitude
            incident_data: Incident details
            precisions: List of precision levels to publish to
            
        Returns:
            Dictionary of precision -> success status
        """
        results = {}
        
        for precision in precisions:
            success = self.publish_incident(
                lat, lon,
                incident_data.copy(),  # Copy to avoid mutation
                precision=precision,
                include_neighbors=True
            )
            results[precision] = success
            
        return results
    
    def get_stats(self) -> Dict:
        """Get service statistics"""
        return {
            **self.stats,
            'connected': self.connected,
            'broker': self.broker_host
        }
    
    def disconnect(self):
        """Disconnect from MQTT broker"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("Disconnected from MQTT broker")
            self.connected = False


_mqtt_service = None


def initialize_mqtt_service(
    broker_host: str,
    broker_port: int,
    username: str,
    password: str,
    use_tls: bool = True
) -> RASMQTTService:
    """
    Args:
        broker_host: HiveMQ cluster hostname
        broker_port: MQTT port
        username: MQTT username
        password: MQTT password
        use_tls: Whether to use TLS
        
    Returns:
        Initialized MQTT service
    """
    global _mqtt_service
    
    _mqtt_service = RASMQTTService(
        broker_host=broker_host,
        broker_port=broker_port,
        username=username,
        password=password,
        use_tls=use_tls
    )
    
    _mqtt_service.connect()
    return _mqtt_service


def get_mqtt_service() -> Optional[RASMQTTService]:
    return _mqtt_service


if __name__ == '__main__':
    import os
    from dotenv import load_dotenv
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    load_dotenv()
    
    service = initialize_mqtt_service(
        broker_host=os.getenv('MQTT_BROKER_HOST'),
        broker_port=int(os.getenv('MQTT_BROKER_PORT', 8883)),
        username=os.getenv('MQTT_BACKEND_USERNAME'),
        password=os.getenv('MQTT_BACKEND_PASSWORD'),
        use_tls=True
    )
    
    time.sleep(2)
    
    print("\n" + "="*60)
    print("TEST 1: Publishing incident to multiple precision levels")
    print("="*60)
    
    test_incident = {
        'risk_type': 'phone_usage',
        'location': {'lat': 43.6532, 'lon': -79.3832},
        'severity': 'high',
        'timestamp': '2026-02-11T14:30:00Z',
        'device_id': 'pi_vehicle_123'
    }
    
    results = service.publish_incident_multi_precision(
        lat=43.6532,
        lon=-79.3832,
        incident_data=test_incident,
        precisions=[5, 6, 7]
    )
    
    print(f"\nPublish results: {results}")
    print("\n" + "="*60)
    print("TEST 2: Geohash calculation at different precisions")
    print("="*60)
    
    lat, lon = 43.6532, -79.3832
    print(f"\nLocation: Toronto ({lat}, {lon})")
    print(f"\nGeohash precisions:")
    for precision in range(1, 8):
        gh = service.calculate_geohash(lat, lon, precision)
        print(f"  Precision {precision}: {gh}")
    
    print("\n" + "="*60)
    print("TEST 3: Neighboring geohashes (precision 7)")
    print("="*60)
    
    gh7 = service.calculate_geohash(lat, lon, 7)
    neighbors = service.get_neighbor_geohashes(gh7)
    print(f"\nCenter geohash: {gh7}")
    print(f"Neighbors:")
    for direction, neighbor in neighbors.items():
        print(f"  {direction:2s}: {neighbor}")
    
    print("\n" + "="*60)
    print("TEST 4: Service statistics")
    print("="*60)
    
    stats = service.get_stats()
    print(f"\nStatistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n" + "="*60)
    print("Service running... Press Ctrl+C to exit")
    print("="*60)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        service.disconnect()