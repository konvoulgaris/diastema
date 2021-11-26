# Base image
FROM continuumio/miniconda3

# Update system
RUN apt -y update
RUN apt -y upgrade

# Expose ports
EXPOSE 5000

# Entrypoint
CMD echo "Hello"
