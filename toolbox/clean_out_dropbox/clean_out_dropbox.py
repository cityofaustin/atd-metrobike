#!/usr/bin/env python3

import os
import argparse
import dropbox

# Get the Dropbox token from environment variables
DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")

# Create a Dropbox client
dbx = dropbox.Dropbox(DROPBOX_TOKEN)

# Initialize total size to 0
total_size = 0


# Recursive function to list files
def list_files(path, mirror):
    global total_size
    for entry in dbx.files_list_folder(path).entries:
        if isinstance(entry, dropbox.files.FileMetadata):
            size_mb = entry.size / 1024 / 1024  # Convert size to megabytes
            print(f"{entry.path_display} ({size_mb:.2f} MB)")
            total_size += size_mb

            if mirror:
                # Create the mirror directory
                local_path = os.path.join("dropbox_mirror", entry.path_display[1:])
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                # Download the file
                dbx.files_download_to_file(local_path, entry.path_display)
        elif isinstance(entry, dropbox.files.FolderMetadata):
            print(entry.path_display + "/")
            list_files(entry.path_display, mirror)


# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument(
    "-i", "--inspect", action="store_true", help="list the files showing the sizes"
)
parser.add_argument(
    "-m",
    "--mirror",
    action="store_true",
    help="mirror the contents of the Dropbox account",
)
args = parser.parse_args()

# Start listing files from the root
list_files("", args.mirror)

# Print total size
print(f"Total size: {total_size:.2f} MB")
