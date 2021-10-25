"""
Check for new metrobike data in Dropbox share and upload to Open Data portal (Socrata)
Metrobike staff put new trip data in Dropbox share on a monthly basis.
"""
import csv
from datetime import datetime
import logging
import os
import sys

import arrow
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
    url = "https://data.austintexas.gov/resource/{}.json?$query=SELECT checkout_date as date ORDER BY checkout_date DESC LIMIT 1".format(
        resource_id
    )
    res = requests.get(url)
    res.raise_for_status()
    return arrow.get(res.json()[0]["date"])


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


def handle_value(key, value, date_keys=["checkout_date"]):
    if not value or key not in date_keys:
        return value
    # Format a socrata-friendly date
    return arrow.get(value, "M/D/YY").format("YYYY-MM-DD")


def map_row(row):
    return {
        FIELDS[key]: handle_value(key, value)
        for key, value in row.items()
        if key in FIELDS
    }


def handle_data(csv_text):
    """Parse CSV"""
    rows = csv_text.splitlines()
    reader = csv.DictReader(rows)
    data = [map_row(row) for row in reader]
    return data


def main():
    client = Socrata(
        "data.austintexas.gov",
        SOCRATA_APP_TOKEN,
        username=SOCRATA_API_KEY_ID,
        password=SOCRATA_API_KEY_SECRET,
        timeout=30,
    )
    today = arrow.get(datetime.today())
    # bcycle dropbox data is only ever available for the previous month
    max_file_date = today.shift(months=-1)
    current_file_date = get_max_socrata_date(RESOURCE_ID)

    if current_file_date >= max_file_date:
        return

    while True:
        current_file_date = current_file_date.shift(months=+1)
        dropbox_file_dt = current_file_date.format("MMYYYY")
        current_file = "TripReport-{}.csv".format(dropbox_file_dt)
        root = "austinbcycletripdata"  # note the lowercase-ness
        path = "/{}/{}/{}".format(root, current_file_date.year, current_file)

        logger.info(f"Checking for Dropbox data at {path}")

        csv_text = get_data(path, METROBIKE_DROPBOX_TOKEN)
        if not csv_text:
            return

        data = handle_data(csv_text)
        logger.info(f"Uploading {len(data)} trips...")
        client.upsert(RESOURCE_ID, data)


if __name__ == "__main__":
    logger = getLogger(__file__)
    main()
