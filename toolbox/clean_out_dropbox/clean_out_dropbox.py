#!/usr/bin/env python3

import os
import argparse
import dropbox
from tqdm import tqdm
import boto3
from datetime import datetime

# Get the Dropbox token from environment variables
DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")

# Get the AWS access key and secret access key from environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Create a Dropbox client
dbx = dropbox.Dropbox(DROPBOX_TOKEN)

# Create a S3 client
s3 = boto3.client(
    "s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Initialize total size to 0
total_size = 0


# Recursive function to count files
def count_files(path):
    count = 0
    for entry in dbx.files_list_folder(path).entries:
        if isinstance(entry, dropbox.files.FileMetadata):
            count += 1
        elif isinstance(entry, dropbox.files.FolderMetadata):
            count += count_files(entry.path_display)
    return count


# Recursive function to list files
def list_files(path):
    global total_size
    for entry in dbx.files_list_folder(path).entries:
        if isinstance(entry, dropbox.files.FileMetadata):
            size_mb = entry.size / 1024 / 1024  # Convert size to megabytes
            print(f"{entry.path_display} ({size_mb:.2f} MB)")
            total_size += size_mb
        elif isinstance(entry, dropbox.files.FolderMetadata):
            list_files(entry.path_display)


# Recursive function to mirror files
def mirror_files(path, pbar, s3_bucket=None, s3_folder=None):
    for entry in dbx.files_list_folder(path).entries:
        if isinstance(entry, dropbox.files.FileMetadata):
            if s3_bucket and s3_folder:
                # Upload the file to S3
                s3.upload_fileobj(
                    dbx.files_download(entry.path_display).file,
                    s3_bucket,
                    f"{s3_folder}/{entry.path_display}",
                )
            else:
                # Create the mirror directory
                local_path = os.path.join("dropbox_mirror", entry.path_display[1:])
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                # Download the file
                dbx.files_download_to_file(local_path, entry.path_display)

            # Update progress bar
            pbar.update(1)
            pbar.set_description(entry.path_display)
        elif isinstance(entry, dropbox.files.FolderMetadata):
            mirror_files(entry.path_display, pbar, s3_bucket, s3_folder)


# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument(
    "-i", "--inspect", action="store_true", help="list the files showing the sizes"
)
parser.add_argument(
    "-m",
    "--mirror",
    action="store_true",
    help="mirror the contents of the Dropbox account, defaulting to using a local directory",
)
parser.add_argument(
    "-s",
    "--mirror-to-s3",
    action="store_true",
    help="mirror the contents of the Dropbox account to S3",
)
args = parser.parse_args()

# Start listing files from the root
if args.inspect:
    list_files("")
    print(f"Total size: {total_size:.2f} MB")

if args.mirror:
    total_files = count_files("")
    with tqdm(total=total_files, dynamic_ncols=True) as pbar:
        if args.mirror_to_s3:
            # Create a new folder in the root of the bucket
            s3_folder = "backup_" + datetime.now().strftime("%y%m%d-%H%M%S")
            mirror_files("", pbar, "metrobike-historic-data", s3_folder)
        else:
            mirror_files("", pbar)
