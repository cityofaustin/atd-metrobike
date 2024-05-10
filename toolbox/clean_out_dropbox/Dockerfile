# Use the latest version of Ubuntu
FROM ubuntu:latest

# Update the system and install Python and pip
RUN apt-get update -y && \
  apt-get install -y python3-pip python3-dev

# Copy the requirements.txt file into the Docker image
COPY requirements.txt /app/requirements.txt

# Set the working directory to /app
WORKDIR /app

# Install the Python libraries specified in requirements.txt
RUN pip3 install -r requirements.txt --break-system-packages

# Set /bin/bash as the entry point
ENTRYPOINT ["/bin/bash"]