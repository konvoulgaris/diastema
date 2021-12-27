# Base image
FROM ubuntu:20.04

# Expose ports
EXPOSE 5000

# Update system and install prerequisites (Python and Gunicorn)
RUN apt update -y
RUN apt upgrade -y
RUN apt install -y python3 python3-pip python-is-python3
RUN pip install -U pip
RUN pip install gunicorn

# Create python user
RUN useradd -ms /bin/bash python

# Create project directory and set ownership
RUN mkdir -p /app
RUN chown -R python:python /app

# Change to python user
USER python

# Use project directory
WORKDIR /app

# Copy and install requirements
ADD ./requirements.txt .
RUN pip install -r ./requirements.txt

# Copy project files
ADD . .

# Entrypoint
ENTRYPOINT gunicorn --chdir src --bind :5000 app:app
