FROM python:3.8-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /code

RUN pip3 install --no-cache-dir -U pip setuptools wheel

COPY ./requirements.txt /code/

RUN pip3 install --no-cache-dir -r /code/requirements.txt

COPY . /code/