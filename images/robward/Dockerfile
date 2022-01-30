FROM python:3.10

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY example.py ./
COPY dataload.py ./
COPY test_dataload.py ./
