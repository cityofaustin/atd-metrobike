"""
Script that publishes data to Socrata from a file that is stored on Dropbox that contains data for
trips between December 2013 and January 2016. This file is in a different format with different column
definitions so that's why this script needed to be created.
"""

import pandas as pd
import dropbox
import os
from io import StringIO
from pytz import timezone
from sodapy import Socrata
import numpy as np

FIELDS = [
    "trip_id",
    "membership_type",
    "bicycle_id",
    "bike_type",
    "checkout_date",
    "checkout_time",
    "checkout_datetime",
    "checkout_kiosk_id",
    "checkout_kiosk",
    "return_kiosk_id",
    "return_kiosk",
    "trip_duration_minutes",
    "month",
    "year",
]

METROBIKE_DROPBOX_TOKEN = os.getenv("METROBIKE_DROPBOX_TOKEN")
SOCRATA_API_KEY_ID = os.getenv("SOCRATA_API_KEY_ID")
SOCRATA_API_KEY_SECRET = os.getenv("SOCRATA_API_KEY_SECRET")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
RESOURCE_ID = "tyfh-5r8s"

client = Socrata(
    "datahub.austintexas.gov",
    SOCRATA_APP_TOKEN,
    username=SOCRATA_API_KEY_ID,
    password=SOCRATA_API_KEY_SECRET,
    timeout=60,
)

dbx = dropbox.Dropbox(METROBIKE_DROPBOX_TOKEN)


def get_data(path, token):
    """Get trip data file as string from dropbox"""
    dbx = dropbox.Dropbox(token)

    try:
        metadata, res = dbx.files_download(path)

    except dropbox.exceptions.ApiError:
        #  file not found - we take that to mean the data has not been uploaded to Dropbox yet by Metrobike staff
        # logger.warning(f"No trip data file found at {path}")
        return None
    res.raise_for_status()
    return res


# Downloading CSV from dropbox
data = get_data(
    "/austinbcycletripdata/AustinTripQuery_pre2018.csv", METROBIKE_DROPBOX_TOKEN
)
df = pd.read_csv(StringIO(data.text))

# Mapping for columns that are already in the right format
column_mapping = {
    "TripId": "trip_id",
    "MembershipType": "membership_type",
    "BikeVisibleId": "bicycle_id",
    "Duration": "trip_duration_minutes",
}
df.rename(columns=column_mapping, inplace=True)

# Filtering out maintenance trips and short trips
df = df[df["trip_duration_minutes"] > 1]
df = df[df["ActualCharge"] >= 0]

# Date/time fields
central_timezone = timezone("US/Central")

df["RentalDate CST"] = pd.to_datetime(df["RentalDate CST"].str[:-7])
df["RentalDate CST"] = (
    df["RentalDate CST"].dt.tz_localize("utc").dt.tz_convert(central_timezone)
)
df["checkout_date"] = df["RentalDate CST"].dt.strftime("%Y-%m-%d")
df["checkout_time"] = df["RentalDate CST"].dt.strftime("%-H:%M:%S")
df["checkout_datetime"] = df["RentalDate CST"].dt.strftime("%Y-%m-%dT%H:%M:%S")
df["year"] = df["RentalDate CST"].dt.strftime("%Y")
df["month"] = df["RentalDate CST"].dt.strftime("%-m")
df["hour"] = df["RentalDate CST"].dt.strftime("%H")

# Kiosk IDs lookup city asset number value in another table
kiosks = pd.read_csv("https://data.austintexas.gov/resource/qd73-bsdg.csv")

# Adding in some missing kiosk IDs into the dataset
kiosks.loc[kiosks["kiosk_id"] == 2550, "city_asset_num"] = 2550
kiosks.loc[kiosks["kiosk_id"] == 2576, "city_asset_num"] = 2576
kiosks.loc[kiosks["kiosk_id"] == 2564, "city_asset_num"] = 2564
kiosks.loc[kiosks["kiosk_id"] == 2536, "city_asset_num"] = 2536
kiosks.loc[kiosks["kiosk_id"] == 3381, "city_asset_num"] = 3381
kiosks.loc[kiosks["kiosk_id"] == 2546, "city_asset_num"] = 2546
kiosks.loc[kiosks["kiosk_id"] == 2545, "city_asset_num"] = 2545
kiosks.loc[kiosks["kiosk_id"] == 2712, "city_asset_num"] = 6
kiosks.loc[kiosks["kiosk_id"] == 1006, "city_asset_num"] = 1006
kiosks.loc[kiosks["kiosk_id"] == 2500, "city_asset_num"] = 2500

# Create lookup table between city_asset_num used by the CSV and kiosk_ids we use in the dataset
kiosks = kiosks[kiosks["city_asset_num"] >= 0]
kiosks["city_asset_num"] = kiosks["city_asset_num"].astype("Int32")
kiosks = kiosks[["kiosk_id", "city_asset_num"]]

# Cleaning up some kiosk IDs
df["CheckoutKioskOptionalId"] = df["CheckoutKioskOptionalId"].astype(str)
df["ReturnKioskOptionalId"] = df["ReturnKioskOptionalId"].astype(str)
df["CheckoutKioskOptionalId"] = df["CheckoutKioskOptionalId"].str.replace(" UT", "")
df["ReturnKioskOptionalId"] = df["ReturnKioskOptionalId"].str.replace(" UT", "")

df["CheckoutKioskOptionalId"] = (
    df["CheckoutKioskOptionalId"].astype(float).astype("Int32")
)
df["ReturnKioskOptionalId"] = df["ReturnKioskOptionalId"].astype(float).astype("Int32")

# fixing several IDs manually...
df["CheckoutKioskOptionalId"] = df["CheckoutKioskOptionalId"].replace(2, 16742)
df["ReturnKioskOptionalId"] = df["ReturnKioskOptionalId"].replace(2, 16742)

df["CheckoutKioskOptionalId"] = df["CheckoutKioskOptionalId"].replace(32517, 16729)
df["ReturnKioskOptionalId"] = df["ReturnKioskOptionalId"].replace(32517, 16729)

df.loc[
    df["CheckoutKiosk"] == "Republic Square @ Guadalupe & 4th St.",
    "CheckoutKioskOptionalId",
] = 2550
df.loc[
    df["ReturnKiosk"] == "Republic Square @ Guadalupe & 4th St.",
    "ReturnKioskOptionalId",
] = 2550

df.loc[df["CheckoutKiosk"] == "Rainey @ River St", "CheckoutKioskOptionalId"] = 2576
df.loc[df["ReturnKiosk"] == "Rainey @ River St", "ReturnKioskOptionalId"] = 2576

df.loc[df["CheckoutKiosk"] == "5th & San Marcos", "CheckoutKioskOptionalId"] = 2564
df.loc[df["ReturnKiosk"] == "5th & San Marcos", "ReturnKioskOptionalId"] = 2564

df.loc[
    df["CheckoutKiosk"] == "East 7th & Pleasant Valley", "CheckoutKioskOptionalId"
] = 3381
df.loc[
    df["ReturnKiosk"] == "East 7th & Pleasant Valley", "ReturnKioskOptionalId"
] = 3381

df.loc[df["CheckoutKiosk"] == "Waller & 6th St.", "CheckoutKioskOptionalId"] = 2536
df.loc[df["ReturnKiosk"] == "Waller & 6th St.", "ReturnKioskOptionalId"] = 2536

df.loc[
    df["CheckoutKiosk"] == "ACC - West & 12th Street", "CheckoutKioskOptionalId"
] = 2546
df.loc[df["ReturnKiosk"] == "ACC - West & 12th Street", "ReturnKioskOptionalId"] = 2546

df.loc[
    df["CheckoutKiosk"] == "ACC - Rio Grande & 12th", "CheckoutKioskOptionalId"
] = 2545
df.loc[df["ReturnKiosk"] == "ACC - Rio Grande & 12th", "ReturnKioskOptionalId"] = 2545

df.loc[
    df["CheckoutKiosk"] == "ACC - Rio Grande & 12th", "CheckoutKioskOptionalId"
] = 2545
df.loc[df["ReturnKiosk"] == "ACC - Rio Grande & 12th", "ReturnKioskOptionalId"] = 2545

df.loc[df["CheckoutKiosk"] == "Zilker Park West", "CheckoutKioskOptionalId"] = 1006
df.loc[df["ReturnKiosk"] == "Zilker Park West", "ReturnKioskOptionalId"] = 1006

df.loc[df["CheckoutKiosk"] == "Republic Square ", "CheckoutKioskOptionalId"] = 2500
df.loc[df["ReturnKiosk"] == "Republic Square ", "ReturnKioskOptionalId"] = 2500

# Join in data from the lookup table to get kiosk IDs for return and checkout kiosks
df = df.merge(
    kiosks, left_on="CheckoutKioskOptionalId", right_on="city_asset_num", how="left"
)

df.rename(
    columns={"kiosk_id": "checkout_kiosk_id", "CheckoutKiosk": "checkout_kiosk"},
    inplace=True,
)
df = df.merge(
    kiosks, left_on="ReturnKioskOptionalId", right_on="city_asset_num", how="left"
)
df.rename(
    columns={"kiosk_id": "return_kiosk_id", "ReturnKiosk": "return_kiosk"}, inplace=True
)

# Socrata expects text field for kiosk IDs, so drop any trailing zeroes
df["return_kiosk_id"] = df["return_kiosk_id"].astype(str).replace("\.0", "", regex=True)
df["checkout_kiosk_id"] = (
    df["checkout_kiosk_id"].astype(str).replace("\.0", "", regex=True)
)

# all bikes are classic in pre-2016 data
df["bike_type"] = "classic"

# Verify we have all the correct columns
for field in FIELDS:
    assert field in df.columns
df = df[FIELDS]


def df_to_socrata(soda, df, dataset_id, include_index):
    if include_index:
        df = df.reset_index()
    df = df.replace({np.nan: None})
    payload = df.to_dict(orient="records")
    num = 0
    while payload:
        n = 10000
        batch, payload = payload[:n], payload[n:]
        print(f"uploading batch: {num}")
        num += 1
        try:
            res = soda.upsert(dataset_id, batch)
        except Exception as e:
            raise e

# Upsert data to socrata
df_to_socrata(client, df, RESOURCE_ID, False)
