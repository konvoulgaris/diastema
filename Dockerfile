# Base image
FROM ubuntu:20.04

# Expose ports
EXPOSE 5000

# Update system and install prerequisites (Python)
RUN apt update -y
RUN apt upgrade -y
RUN apt install -y python3 python3-pip python-is-python3
RUN pip install -U pip

# Create python user
RUN useradd -ms /bin/bash python

# Create project directory and set ownership
RUN mkdir -p /app
RUN chown -R python:python /app

# Change to python user
USER python

# Use project directory
WORKDIR /app

# Copy project files
ADD ./producer ./producer
ADD ./worker ./worker
ADD ./wrapper.sh .

# Install requirements
RUN pip install -r ./producer/requirements.txt
RUN pip install -r ./worker/requirements.txt

# Entrypoint
CMD ./wrapper.sh
