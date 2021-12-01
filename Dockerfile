# Base image
FROM continuumio/miniconda3

# Update system
RUN apt update -y
RUN apt upgrade -y

# Create app directory and add files
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
ADD . .

# Install dependencies
RUN conda install -y --file spec-file.txt

# Install Gunicorn
RUN conda install -y gunicorn

# Expose ports
EXPOSE 5000

# Entrypoint
ENTRYPOINT gunicorn --chdir src --bind 0.0.0.0:5000 app:app
