# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np
import os

# ----------------------------------------------------------
# PARAMETERS (adapted to your setup)
# ----------------------------------------------------------

# We have cars lab4-car-1 .. lab4-car-10
device_st = 1          # inclusive
device_end = 11        # exclusive → 1..10

# Vehicle CSVs are in folder "vehicle data"
# Expected filenames: vehicle1.csv, vehicle2.csv, ..., vehicle10.csv
VEHICLE_DATA_DIR = "vehicle-data"
data_path_template = os.path.join(VEHICLE_DATA_DIR, "vehicle{0}.csv")

# Cert/key paths for each car:
# car_certs/lab4-car-1/certificate.pem.crt, private.pem.key, etc.
certificate_formatter = "car_certs/lab4-car-{0}/certificate.pem.crt"
key_formatter         = "car_certs/lab4-car-{0}/private.pem.key"

# Root CA
root_ca_path = "AmazonRootCA1.pem"

# AWS IoT Core endpoint (from IoT Core → Settings)
IOT_ENDPOINT = "a1skgjdme9i992-ats.iot.us-west-2.amazonaws.com"  

# ----------------------------------------------------------
# MQTT CLIENT CLASS 
# ----------------------------------------------------------
class MQTTClient:
    def __init__(self, numeric_id, cert, key):
        """
        numeric_id: integer 1..10 (for CSV index)
        device_id:  string "lab4-car-<n>" (for MQTT clientId/logs)
        """
        self.numeric_id = numeric_id                     # 1..10
        self.device_id = f"lab4-car-{numeric_id}"        # Thing / clientId
        self.state = 0

        # For certificate-based connection
        self.client = AWSIoTMQTTClient(self.device_id)

        # TODO 2: broker address (fixed to use your IoT endpoint)
        self.client.configureEndpoint(IOT_ENDPOINT, 8883)
        self.client.configureCredentials(root_ca_path, key, cert)

        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline queue
        self.client.configureDrainingFrequency(2)        # 2 Hz
        self.client.configureConnectDisconnectTimeout(10)
        self.client.configureMQTTOperationTimeout(5)

        self.client.onMessage = self.customOnMessage

    # TODO 3: show received message
    def customOnMessage(self, message):
        print(
            "client {} received payload {} from topic {}".format(
                self.device_id,
                message.payload.decode("utf-8"),
                message.topic,
            )
        )

    # Suback callback
    def customSubackCallback(self, mid, data):
        pass

    # Puback callback
    def customPubackCallback(self, mid):
        pass

    def publish(self, topic="vehicle/emission/data"):
        """
        Publish rows from this device's CSV file as JSON to the given topic.
        CSV: vehicle data/vehicle<numeric_id>.csv
        """
        csv_path = data_path_template.format(self.numeric_id)

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Missing CSV file: {csv_path}")

        df = pd.read_csv(csv_path)

        for index, row in df.iterrows():
            payload = json.dumps(row.to_dict())
            print(f"Publishing from {self.device_id}: {payload}")

            self.client.publishAsync(
                topic,
                payload,
                0,
                ackCallback=self.customPubackCallback,
            )

            # Sleep to simulate real-time streaming
            time.sleep(0.2)


# ----------------------------------------------------------
# MAIN SCRIPT
# ----------------------------------------------------------

print("Loading vehicle data...")
data = []
for i in range(device_st, device_end):
    csv_file = data_path_template.format(i)
    if os.path.exists(csv_file):
        a = pd.read_csv(csv_file)
        data.append(a)
    else:
        print(f"WARNING: {csv_file} not found!")

print("Initializing MQTTClients...")

clients = []

for numeric_id in range(device_st, device_end):
    cert_path = certificate_formatter.format(numeric_id)
    key_path = key_formatter.format(numeric_id)

    if not (os.path.exists(cert_path) and os.path.exists(key_path)):
        print(
            f"Skipping {numeric_id} (lab4-car-{numeric_id}) — "
            f"missing cert or key.\n  cert={cert_path}\n  key ={key_path}"
        )
        continue

    client = MQTTClient(numeric_id, cert_path, key_path)
    print(f"{client.device_id}: connecting...")
    client.client.connect()
    print(f"{client.device_id}: connected.")
    clients.append(client)

if not clients:
    print("No clients initialized — check cert paths and indexes.")
    raise SystemExit(1)

while True:
    print("Press 's' to send data or 'd' to disconnect:")
    x = input().strip()

    if x == "s":
        for c in clients:
            c.publish()

    elif x == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected.")
        break

    else:
        print("Invalid key pressed.")

    time.sleep(2)
