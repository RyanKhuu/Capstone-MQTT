import paho.mqtt.client as mqtt

BROKER_HOST = "489cedb546414dd88de068b71438af7d.s1.eu.hivemq.cloud"
BROKER_PORT = 8883
USERNAME = "Anmol"
PASSWORD = "RASPassword1"
USE_TLS = True

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("incidents/+")
    print("Subscribed to ras/+")

def on_message(client, userdata, msg):
    print(f"Received message on topic {msg.topic}: {msg.payload.decode()}")

client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)

if USE_TLS:
    client.tls_set()

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_HOST, BROKER_PORT, 60)
client.loop_forever()