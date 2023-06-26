"""
Check for new metrobike data in Dropbox share and upload to Open Data portal (Socrata)
Metrobike staff put new trip data in Dropbox share on a monthly basis.
"""
import csv
from datetime import datetime
import logging
import os
import sys

from cerberus import Validator
import dateutil.parser
from dateutil.relativedelta import relativedelta
import dropbox
import requests
from sodapy import Socrata

METROBIKE_DROPBOX_TOKEN = os.getenv("METROBIKE_DROPBOX_TOKEN")
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


def get_max_socrata_date(resource_id):
    url = f"https://data.austintexas.gov/resource/{resource_id}.json"
    params = {
        "$query": "SELECT checkout_date as date where checkout_date is not null ORDER BY checkout_date DESC LIMIT 1"
    }
    res = requests.get(url, params=params)
    res.raise_for_status()
    try:
        datestring = res.json()[0]["date"]
    except (KeyError, IndexError):
        raise ValueError(
            "No existing data found. There may be something wrong with the dataset?"
        )
    return dateutil.parser.parse(datestring)


def get_data(path, token):
    """Get trip data file as string from dropbox"""
    dbx = dropbox.Dropbox(token)

    try:
        metadata, res = dbx.files_download(path)

    except dropbox.exceptions.ApiError:
        #  file not found - we take that to mean the data has not been uploaded to Dropbox yet by Metrobike staff
        logger.warning(f"No trip data file found at {path}")
        return None
    res.raise_for_status()
    return res.text


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
        if len(row["bicycle_id"]) == 5 and int(row["bicycle_id"][0:2]) >= 15:
            row["bike_type"] = "Electric Bike"
        # Additional case where the ID is a six character ID with a trailing E are also E-bikes
        elif len(row["bicycle_id"]) == 6 and int(row["bicycle_id"][0:2]) >= 15 and row["bicycle_id"][5] == "E":
            row["bike_type"] = "Electric Bike"
        else:
            row["bike_type"] = "Classic Bike"

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


def handle_data(csv_text):
    """Parse CSV"""
    rows = csv_text.splitlines()
    reader = csv.DictReader(rows)
    data = [map_row(row) for row in reader]
    # Get bike type
    data = classify_bike_type(data)
    # Add month/year columns
    data = populate_month_year(data)
    return data


def validate_row(row, validator):
    if not validator.validate(row):
        raise ValueError(f"Schema validation error: {validator.errors}")


def main():
    client = Socrata(
        "datahub.austintexas.gov",
        SOCRATA_APP_TOKEN,
        username=SOCRATA_API_KEY_ID,
        password=SOCRATA_API_KEY_SECRET,
        timeout=30,
    )
    today = datetime.today()
    # bcycle dropbox data is only ever available for the previous month
    max_file_date = today + relativedelta(months=-1)
    current_file_date = get_max_socrata_date(RESOURCE_ID)

    if current_file_date >= max_file_date:
        return

    validator = Validator(SCHEMA)

    while True:
        current_file_date = current_file_date + relativedelta(months=+1)
        dropbox_file_dt = current_file_date.strftime("%m%Y")
        current_file = "TripReport-{}.csv".format(dropbox_file_dt)
        root = "austinbcycletripdata"  # note the lowercase-ness
        path = "/{}/{}/{}".format(root, current_file_date.year, current_file)

        logger.info(f"Checking for Dropbox data at {path}")

        csv_text = get_data(path, METROBIKE_DROPBOX_TOKEN)
        if not csv_text:
            return

        logger.info(f"Transforming data...")
        data = handle_data(csv_text)

        logger.info(f"Validating data...")
        [validate_row(row, validator) for row in data]

        logger.info(f"Uploading {len(data)} trips...")
        client.upsert(RESOURCE_ID, data)

if __name__ == "__main__":
    logger = getLogger(__file__)
    main()
