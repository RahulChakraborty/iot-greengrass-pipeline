import os
import json
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# -------------------------------------------
# USER CONFIGURATION — UPDATE THESE AS NEEDED
# -------------------------------------------

AWS_PROFILE = "rahul_uiuc"              # <-- set your AWS CLI profile here
AWS_REGION = "us-west-2"             # <-- set your region here
POLICY_NAME = "lab4-iot-policy"
THING_GROUP_NAME = "lab4-car-fleet"
THING_NAME_PREFIX = "lab4-car-"
CAR_COUNT = 10                       # number of cars to create

CERTS_BASE_DIR = Path("car_certs")   # where to store certs locally


# -------------------------------------------
# INTERNAL FUNCTIONS
# -------------------------------------------

def ensure_output_dir():
    CERTS_BASE_DIR.mkdir(parents=True, exist_ok=True)


def create_iot_client():
    """
    Create a boto3 IoT client using the AWS profile specified above.
    """
    session = boto3.Session(profile_name=AWS_PROFILE)
    return session.client("iot", region_name=AWS_REGION)


def create_thing(iot, thing_name):
    try:
        resp = iot.create_thing(thingName=thing_name)
        print(f"[+] Created thing: {thing_name}")
        return resp
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print(f"[!] Thing already exists: {thing_name}, skipping creation")
            return iot.describe_thing(thingName=thing_name)
        else:
            raise


def create_keys_and_cert(iot, thing_name):
    """
    Create certificate and keys for this Thing.
    Save them into car_certs/<thing_name>/.
    """
    resp = iot.create_keys_and_certificate(setAsActive=True)

    certificate_arn = resp["certificateArn"]
    certificate_pem = resp["certificatePem"]
    private_key = resp["keyPair"]["PrivateKey"]
    public_key = resp["keyPair"]["PublicKey"]
    cert_id = resp["certificateId"]

    thing_dir = CERTS_BASE_DIR / thing_name
    thing_dir.mkdir(parents=True, exist_ok=True)

    # Save files
    (thing_dir / "certificate.pem.crt").write_text(certificate_pem)
    (thing_dir / "private.pem.key").write_text(private_key)
    (thing_dir / "public.pem.key").write_text(public_key)

    meta = {
        "thing_name": thing_name,
        "certificate_arn": certificate_arn,
        "certificate_id": cert_id,
    }
    (thing_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    print(f"[+] Created cert/keys for {thing_name} → {thing_dir}")
    return certificate_arn


def attach_policy(iot, certificate_arn):
    try:
        iot.attach_policy(policyName=POLICY_NAME, target=certificate_arn)
        print(f"[+] Attached policy {POLICY_NAME} to cert {certificate_arn}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print(f"[!] Policy already attached, skipping")
        else:
            raise


def attach_thing_principal(iot, thing_name, certificate_arn):
    try:
        iot.attach_thing_principal(
            thingName=thing_name,
            principal=certificate_arn
        )
        print(f"[+] Attached principal → thing {thing_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print(f"[!] Principal already attached, skipping")
        else:
            raise


def add_thing_to_group(iot, thing_name):
    try:
        iot.add_thing_to_thing_group(
            thingGroupName=THING_GROUP_NAME,
            thingName=thing_name
        )
        print(f"[+] Added {thing_name} to group {THING_GROUP_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print(f"[!] Already in group, skipping")
        else:
            raise


# -------------------------------------------
# MAIN WORKFLOW
# -------------------------------------------

def main():
    print(f"Using AWS Profile: {AWS_PROFILE}")
    print(f"Using Region:      {AWS_REGION}")
    print(f"Creating {CAR_COUNT} cars...")

    ensure_output_dir()
    iot = create_iot_client()

    for i in range(1, CAR_COUNT + 1):
        thing_name = f"{THING_NAME_PREFIX}{i}"
        print(f"\n=== Processing {thing_name} ===")

        create_thing(iot, thing_name)

        cert_arn = create_keys_and_cert(iot, thing_name)

        attach_policy(iot, cert_arn)

        attach_thing_principal(iot, thing_name, cert_arn)

        add_thing_to_group(iot, thing_name)

    print("\nAll cars created successfully!")
    print(f"Certificates saved in: {CERTS_BASE_DIR.resolve()}")


if __name__ == "__main__":
    main()
