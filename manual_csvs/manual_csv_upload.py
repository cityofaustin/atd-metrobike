"""
Made for one-off manual CSV uploads. Around March 2024, Metrobike staff stopped being able to upload to dropbox,
and we were unable to figure out why. So, they just sent us the CSV files over email.
"""
import csv
import logging
import os
import sys

from cerberus import Validator
import dateutil.parser
import dropbox
import requests
from sodapy import Socrata

SOCRATA_API_KEY_ID = os.getenv("SOCRATA_API_KEY_ID")
SOCRATA_API_KEY_SECRET = os.getenv("SOCRATA_API_KEY_SECRET")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

RESOURCE_ID = "tyfh-5r8s"

FIELDS = {
    "TripId": "trip_id",
    "MembershipType": "membership_type",
    "Bike": "bicycle_id",
    "BikeType": "bike_type",
    "CheckoutDateLocal": "checkout_date",
    "CheckoutTimeLocal": "checkout_time",
    "CheckoutKioskID": "checkout_kiosk_id",
    "CheckoutKioskName": "checkout_kiosk",
    "ReturnKioskID": "return_kiosk_id",
    "ReturnKioskName": "return_kiosk",
    "DurationMins": "trip_duration_minutes",
}

"""
yes, all these types are strings. we're letting Socrata coerce trip_duration_minutes to
a number. we could do better. the main purpose of the schema validation is to ensure
all fields are present and not-null
"""
SCHEMA = {
    "trip_id": {"type": "string"},
    "membership_type": {"type": "string"},
    "bicycle_id": {"type": "string"},
    "checkout_date": {"type": "string"},
    "checkout_time": {"type": "string"},
    "checkout_datetime": {"type": "string"},
    "checkout_kiosk_id": {"type": "string"},
    "checkout_kiosk": {"type": "string"},
    "return_kiosk_id": {"type": "string"},
    "return_kiosk": {"type": "string"},
    "trip_duration_minutes": {"type": "string"},
    "bike_type": {"type": "string"},
    "month": {"type": "string"},
    "year": {"type": "string"},
}


def getLogger(name, level=logging.INFO):
    """Return a module logger that streams to stdout"""
    logger = logging.getLogger(name)
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(fmt=" %(name)s.%(levelname)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def handle_value(key, value, date_keys=["CheckoutDateLocal"]):
    if value == "":
        return None
    if not value or key not in date_keys:
        return value
    # Format a socrata-friendly date
    return dateutil.parser.parse(value).strftime("%Y-%m-%d")


def map_row(row):
    return {
        FIELDS[key]: handle_value(key, value)
        for key, value in row.items()
        if key in FIELDS
    }


def classify_bike_type(data):
    """
    Classifying Bikes into Electric or Classic types based on the ID pattern supplied by CapMetro.
    All e-bikes will have 5 digit bike numbers starting with “15” and up.
    """
    for row in data:
        try:
            if len(row["bicycle_id"]) == 5 and int(row["bicycle_id"][0:2]) >= 15:
                row["bike_type"] = "electric"
            # Additional case where the ID is a six character ID with a trailing E are also E-bikes
            elif (
                    len(row["bicycle_id"]) == 6
                    and int(row["bicycle_id"][0:2]) >= 15
                    and row["bicycle_id"][5] == "E"
            ):
                row["bike_type"] = "electric"
            else:
                row["bike_type"] = "classic"
        except:
            row["bike_type"] = "classic"

    return data


def populate_month_year(data):
    """
    Extracts the month and year of the checkout date for two columns in the Socrata dataset
    """
    for row in data:
        date = dateutil.parser.parse(row["checkout_date"])
        row["year"] = date.strftime("%Y")
        row["month"] = date.strftime("%-m")
    return data


def datetime_creation(data):
    """
    Joins together the checkout time and date columns to a single datetime field.
    """
    for row in data:
        # Note that hour is not zero padded in the data
        row["checkout_datetime"] = f"{row['checkout_date']}T{row['checkout_time'].zfill(8)}"
    return data


def handle_data(data):
    # Get bike type
    classify_bike_type(data)
    # Add month/year columns
    populate_month_year(data)
    # Add datetime column
    datetime_creation(data)
    # Remove trips that are less than 2 minutes in length
    data = [d for d in data if int(d.get("trip_duration_minutes", 0)) > 1]
    # Filter rows where "membership_type" is not null
    data = [row for row in data if row['membership_type']]
    return data


def validate_row(row, validator):
    if not validator.validate(row):
        raise ValueError(f"Schema validation error: {validator.errors}")


# Function to read CSV file and convert it to a list of dictionaries
def read_csv_to_dicts(file_path):
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        list_of_dicts = [map_row(row) for row in csv_reader]
    return list_of_dicts


def main():
    client = Socrata(
        "datahub.austintexas.gov",
        SOCRATA_APP_TOKEN,
        username=SOCRATA_API_KEY_ID,
        password=SOCRATA_API_KEY_SECRET,
        timeout=30,
    )
    validator = Validator(SCHEMA)

    files_in_directory = os.listdir("")
    csv_files = [file for file in files_in_directory if file.endswith('.csv')]

    for csv_file in csv_files:
        data = read_csv_to_dicts(f"manual_csvs/{csv_file}")
        logger.info(f"Uploading data from {csv_file}")

        logger.info(f"Transforming data...")
        data = handle_data(data)

        logger.info(f"Validating data...")
        [validate_row(row, validator) for row in data]

        logger.info(f"Uploading {len(data)} trips...")
        client.upsert(RESOURCE_ID, data)


if __name__ == "__main__":
    logger = getLogger(__file__)
    main()
