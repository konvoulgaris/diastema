# Base image
FROM python:3.8-alpine

# Expose ports
EXPOSE 5000

# Update system and install prerequisites (Gunicorn)
RUN apk update
RUN apk upgrade --available
RUN pip install --upgrade pip
RUN pip install gunicorn

# Create and use project directory
RUN mkdir -p /app
WORKDIR /app

# Copy and install requirements
ADD requirements.txt .
RUN pip install -r requirements.txt

# Copy project files
ADD . .

# Entrypoint
ENTRYPOINT gunicorn --chdir src/ --bind :5000 app:app
