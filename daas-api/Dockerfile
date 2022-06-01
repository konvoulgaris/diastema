FROM python:3.8

EXPOSE 5000

RUN apt update -y
RUN apt upgrade -y
RUN pip install --upgrade pip
RUN pip install gunicorn

WORKDIR /app

ADD requirements.txt .
RUN pip install -r requirements.txt

ADD src .

ENTRYPOINT gunicorn --bind :5000 wsgi:wsgi
