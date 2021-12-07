# Base image
FROM continuumio/miniconda3

# Update system
RUN apt update -y
RUN apt upgrade -y
RUN pip install -U pip

# Install Gunicorn
RUN conda install -y gunicorn

# Create and use project directory
WORKDIR /usr/src/app

# Install dependencies
ADD spec-file.txt .
RUN conda install -y --file spec-file.txt

# Copy project files
ADD src .

# Expose ports
EXPOSE 5000

# Entrypoint
ENTRYPOINT gunicorn --bind :5000 --workers 3 app:app
