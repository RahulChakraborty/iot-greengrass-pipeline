import json
import logging
import os
import sys
import time
from typing import Any, Dict

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import process_emission  # uses lambda_handler(event, context=None)


# ============================================================================
# CONFIGURATION: UPDATE THESE VALUES FOR YOUR ENVIRONMENT
# ============================================================================

# Your AWS IoT endpoint (same one you used for the car simulator)
IOT_ENDPOINT = "a1skgjdme9i992-ats.iot.us-west-2.amazonaws.com"

# A unique client ID for this Greengrass component
CLIENT_ID = "gg-vehicle-emission-processor"

# Paths to certificates/keys on the Greengrass core device.
# You can reuse the same cert/key that the car simulator used,
# or another IoT Thing cert. Adjust these as needed.
ROOT_CA_PATH = "/greengrass/v2/AmazonRootCA1.pem"
CERT_PATH = "/greengrass/v2/device.pem.crt"
KEY_PATH = "/greengrass/v2/private.pem.key"

# Topics
INPUT_TOPIC = "vehicle/emission/data"     # where the cars publish
OUTPUT_TOPIC_PREFIX = "iot/Vehicle_"      # output: iot/Vehicle_<vehicle_id>

# NEW: Firehose Delivery Stream
FIREHOSE_STREAM_NAME = "lab4-vehicle-emissions-stream"
AWS_REGION = "us-west-2"

# ============================================================================
# LOGGING SETUP
# ============================================================================

logger = logging.getLogger("vehicle_emission_main")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# ============================================================================
# MQTT CLIENT SETUP
# ============================================================================

def configure_mqtt_client() -> AWSIoTMQTTClient:
    logger.info("Configuring MQTT client...")

    # Debug: verify cert/key files exist
    for p in [ROOT_CA_PATH, CERT_PATH, KEY_PATH]:
        logger.info("Checking path %s exists=%s", p, os.path.exists(p))

    client = AWSIoTMQTTClient(CLIENT_ID)
    client.configureEndpoint(IOT_ENDPOINT, 8883)
    client.configureCredentials(ROOT_CA_PATH, KEY_PATH, CERT_PATH)

    client.configureOfflinePublishQueueing(-1)
    client.configureDrainingFrequency(2)
    client.configureConnectDisconnectTimeout(10)
    client.configureMQTTOperationTimeout(5)

    logger.info("Connecting to IoT Core endpoint %s ...", IOT_ENDPOINT)
    client.connect()
    logger.info("Connected successfully.")

    return client


# ============================================================================
# FIREHOSE CLIENT SETUP
# ============================================================================

def configure_firehose_client():
    """
    Create a boto3 Firehose client using Greengrass IAM role credentials.
    """
    try:
        logger.info("Initializing Firehose client for region %s", AWS_REGION)
        firehose = boto3.client("firehose", region_name=AWS_REGION)
        return firehose
    except Exception as e:
        logger.exception("Failed to initialize Firehose client: %s", e)
        return None


# ============================================================================
# VEHICLE DATA PROCESSING
# ============================================================================

def handle_vehicle_message(mqtt_client: AWSIoTMQTTClient, firehose_client, event: Dict[str, Any]) -> None:
    """
    Processes incoming simulator payload and publishes:
    1. Processed data back to IoT Core
    2. Same processed data to Firehose (newline-delimited)
    """
    try:
        logger.info("Received raw event: %s", json.dumps(event))

        # Unwrap nested payload: event["data"]
        payload = event.get("data", event)

        # Call your existing process_emission logic
        result = process_emission.lambda_handler(payload, None)

        vehicle_id = (
            result.get("vehicle_id")
            or payload.get("vehicle_id")
            or "unknown"
        )

        output_topic = f"{OUTPUT_TOPIC_PREFIX}{vehicle_id}"
        result_json = json.dumps(result)

        # ---------- Publish to IoT Core ----------
        logger.info(
            "Publishing processed event to topic=%s payload=%s",
            output_topic,
            result_json,
        )
        mqtt_client.publish(output_topic, result_json, 0)

        # ---------- Publish to Firehose ----------
        if firehose_client:
            try:
                firehose_client.put_record(
                    DeliveryStreamName=FIREHOSE_STREAM_NAME,
                    Record={"Data": result_json + "\n"}  # newline for Athena
                )
                logger.info(
                    "Sent processed event for vehicle_id=%s to Firehose stream=%s",
                    vehicle_id,
                    FIREHOSE_STREAM_NAME,
                )
            except Exception as fe:
                logger.exception("Failed to publish to Firehose: %s", fe)
        else:
            logger.warning("Firehose client not available; skipping Firehose publish.")

    except Exception as e:
        logger.exception("Error processing vehicle message: %s", e)


# ============================================================================
# SUBSCRIPTION CALLBACK
# ============================================================================

def on_message_callback(client, userdata, message):
    """
    AWSIoTPythonSDK subscribe callback wrapper.
    """
    try:
        payload_str = message.payload.decode("utf-8")
        logger.info(
            "MQTT message received on topic=%s payload=%s",
            message.topic,
            payload_str,
        )
        event = json.loads(payload_str)
    except Exception as e:
        logger.exception("Failed to decode/parse message: %s", e)
        return

    handle_vehicle_message(
        mqtt_client=userdata["mqtt_client"],
        firehose_client=userdata["firehose_client"],
        event=event,
    )


# ============================================================================
# MAIN LOOP
# ============================================================================

def main():
    logger.info("Vehicle emission Greengrass Processor starting...")

    # Protect against unset endpoint
    if IOT_ENDPOINT.startswith("<YOUR_IOT_ENDPOINT"):
        logger.error("ERROR: IOT_ENDPOINT is not configured.")
        sys.exit(1)

    mqtt_client = configure_mqtt_client()
    firehose_client = configure_firehose_client()

    userdata = {
        "mqtt_client": mqtt_client,
        "firehose_client": firehose_client,
    }

    logger.info("Subscribing to topic: %s", INPUT_TOPIC)
    mqtt_client.subscribe(
        INPUT_TOPIC,
        1,
        lambda c, u, m: on_message_callback(c, userdata, m)
    )
    logger.info("Subscription established. Waiting for messages...")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Interrupted. Disconnecting MQTT client...")
        mqtt_client.disconnect()
        logger.info("Shutting down.")


if __name__ == "__main__":
    main()