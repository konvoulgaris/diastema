# Base image
FROM continuumio/miniconda3

# Expose ports
EXPOSE 5000

# Update system and install prerequisites (Gunicorn)
RUN apt update -y
RUN apt upgrade -y
RUN pip install -U pip
RUN conda install -y gunicorn

# Create and use project directory
WORKDIR /app

# Copy and install requirements
ADD ./spec-file.txt .
RUN conda install -y --file ./spec-file.txt

# Copy project files
ADD ./src .

# Entrypoint
ENTRYPOINT gunicorn --bind :5000 app:app
