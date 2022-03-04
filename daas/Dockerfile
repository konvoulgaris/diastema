FROM python:3.8

EXPOSE 5000

RUN apt update -y
RUN apt upgrade -y
RUN pip install -U pip

WORKDIR /app

ADD requirements.txt .
RUN pip install -r requirements.txt

ADD src src

ENTRYPOINT python src/worker.py
