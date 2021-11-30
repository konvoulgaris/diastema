# Base image
FROM continuumio/miniconda3

# Update system
RUN apt update -y
RUN apt upgrade -y

# Install Gunicorn
RUN apt install -y gunicorn

# Create app directory and add files
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
ADD . .

# Install dependencies
RUN conda install -y --file spec-file.txt

# Expose ports
EXPOSE 5000

# Entrypoint
CMD python src/app.py
