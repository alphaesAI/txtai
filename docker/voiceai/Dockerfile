FROM python:3.10-slim

WORKDIR /app

COPY src/python/txtai /app
COPY config.yml /app/config.yml

RUN python -m pip install --upgrade pip
RUN pip install torch==2.8.0+cpu -f https://download.pytorch.org/whl/torch
RUN pip install -e .

