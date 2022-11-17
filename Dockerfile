# syntax=docker/dockerfile:1

FROM openjdk:slim

COPY --from=python:3.9 / /

RUN pip install --upgrade pip

WORKDIR /radarpipeline

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . .

RUN pip install -e .

CMD ["python", "."]
