FROM python:3.8-alpine

EXPOSE 5000

RUN apk update
RUN apk upgrade --available
RUN pip install --upgrade pip
RUN pip install gunicorn

WORKDIR /app

ADD requirements.txt .
RUN pip install -r requirements.txt

ADD wsgi.py .
ADD app app

ENTRYPOINT gunicorn --bind :5000 wsgi:wsgi
