# Dropbox to S3 Mirroring Script

This script is used to mirror the contents of a Dropbox account to a local directory or an AWS S3 bucket. It can also list the files in the Dropbox account along with their sizes.

## Requirements

- Python 3
- Dropbox Python SDK
- Boto3
- tqdm

## Environment Variables

The script uses the following environment variables:

- `DROPBOX_TOKEN`: The access token for the Dropbox account.
- `AWS_ACCESS_KEY`: The access key for the AWS account.
- `AWS_SECRET_ACCESS_KEY`: The secret access key for the AWS account.

## Usage

The script has the following command-line arguments:

- `-i` or `--inspect`: List the files in the Dropbox account along with their sizes.
- `-m` or `--mirror`: Mirror the contents of the Dropbox account to a local directory.
- `-s` or `--mirror-to-s3`: Mirror the contents of the Dropbox account to an AWS S3 bucket.

### Examples

List the files in the Dropbox account:

```bash
python clean_out_dropbox.py -i
```

Mirror the contents of the Dropbox account to a local directory:

```bash
./clean_out_dropbox.py -m
```

Mirror the contents of the Dropbox account to an AWS S3 bucket:

```bash
./clean_out_dropbox.py -ms
```

You can also combine the -i, -m, and -s options. For example, to list the files and then mirror them to a local directory:

Or to list the files and then mirror them to an AWS S3 bucket:

p

```

```
