#!/usr/bin/env python3

import os
import dropbox

# Get the Dropbox token from environment variables
DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")

# Create a Dropbox client
dbx = dropbox.Dropbox(DROPBOX_TOKEN)

# Initialize total size to 0
total_size = 0


# Recursive function to list files
def list_files(path):
    global total_size
    for entry in dbx.files_list_folder(path).entries:
        if isinstance(entry, dropbox.files.FileMetadata):
            size_mb = entry.size / 1024 / 1024  # Convert size to megabytes
            print(f"{entry.path_display} ({size_mb:.2f} MB)")
            total_size += size_mb
        elif isinstance(entry, dropbox.files.FolderMetadata):
            print(entry.path_display + "/")
            list_files(entry.path_display)


# Start listing files from the root
list_files("")

# Print total size
print(f"Total size: {total_size:.2f} MB")
