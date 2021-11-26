# Base image
FROM continuumio/miniconda3

# Expose ports
EXPOSE 5000

# Update system
RUN apt -y update
RUN apt -y upgrade

# Entrypoint
ENTRYPOINT python ./src/app.py
