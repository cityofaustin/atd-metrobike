# atd-metrobike

This repository contains a script which publishes Austin Metrobike trip data to our [public trip dataset](https://data.austintexas.gov/Transportation-and-Mobility/Austin-MetroBike-Trips/tyfh-5r8s).

Metrobike staff upload trip records to a Dropbox folder on a monthly basis. This script fetches those files, transforms the records, and publishes them to the Open Data Portal (Socrata).

## Get it running

1. Build the docker image

```shell
$ docker build -t atddocker/atd-metrobike .
```

2. Create an environment file (`env_file`) with the following variables:

```shell
METROBIKE_DROPBOX_TOKEN
SOCRATA_API_KEY_ID
SOCRATA_API_KEY_SECRET
SOCRATA_APP_TOKEN
```

3. From the same directory as your environment file, run:

```shell
$ docker run -it --rm --env-file env_file atddocker/atd-metrobike python publish_trips.py
```

## License

As a work of the City of Austin, this project is in the public domain within the United States.

Additionally, we waive copyright and related rights in the work worldwide through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
