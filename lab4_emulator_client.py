import boto3
import json
import os
import time
import pandas as pd

# -----------------------------
# CONFIGURE THESE PARAMETERS
# -----------------------------

AWS_PROFILE = "rahul_uiuc"           #  AWS CLI profile name
AWS_REGION = "us-west-2"          #  region

#  IoT data-plane endpoint:
# From IoT Core → Settings → Device data endpoint
# Use it with "https://"
IOT_DATA_ENDPOINT = "https://a1skgjdme9i992-ats.iot.us-west-2.amazonaws.com"  # <-- change

# Topic name (from  lab instructions)
TOPIC = "vehicle/emission/data"

# Car IDs (1..10 → lab4-car-1 .. lab4-car-10, vehicle1.csv..vehicle10.csv)
CAR_ID_START = 1
CAR_ID_END = 11   # exclusive, so 1..10

VEHICLE_DATA_DIR = "vehicle-data"  # folder with CSVs


def create_iot_data_client():
    """
    Create boto3 IoT Data Plane client using a specific profile.
    This publishes messages over HTTPS (port 443), not MQTT/TLS:8883.
    """
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    return session.client("iot-data", endpoint_url=IOT_DATA_ENDPOINT)


def publish_car_data(iot_data, car_id):
    """
    Publish all rows from vehicle{car_id}.csv to IoT Core topic.
    """
    device_id = f"lab4-car-{car_id}"
    csv_path = os.path.join(VEHICLE_DATA_DIR, f"vehicle{car_id}.csv")

    if not os.path.exists(csv_path):
        print(f"[WARN] CSV not found for {device_id}: {csv_path}")
        return

    df = pd.read_csv(csv_path)

    print(f"[INFO] Publishing data for {device_id} from {csv_path}")

    for index, row in df.iterrows():
        payload = {
            "device_id": device_id,
            "data": row.to_dict()
        }

        payload_str = json.dumps(payload)
        print(f"[PUBLISH] {device_id} → {TOPIC}: {payload_str}")

        # Publish via IoT Data Plane
        iot_data.publish(
            topic=TOPIC,
            qos=0,
            payload=payload_str
        )

        # Sleep to simulate real-time streaming
        time.sleep(0.2)


def main():
    print(f"Using AWS profile: {AWS_PROFILE}, region: {AWS_REGION}")
    print(f"IoT Data endpoint: {IOT_DATA_ENDPOINT}")
    print(f"Topic: {TOPIC}")

    iot_data = create_iot_data_client()

    while True:
        cmd = input("Press 's' to send data, 'd' to exit: ").strip().lower()

        if cmd == "s":
            # loop over all cars
            for car_id in range(CAR_ID_START, CAR_ID_END):
                publish_car_data(iot_data, car_id)
            print("[INFO] Finished sending for all cars.")

        elif cmd == "d":
            print("Exiting.")
            break

        else:
            print("Invalid input, use 's' or 'd'.")

        time.sleep(1)


if __name__ == "__main__":
    main()
