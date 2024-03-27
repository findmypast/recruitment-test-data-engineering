FROM python:3.10

WORKDIR /app

COPY images/load_data/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt


COPY images/load_data/read_data.py ./
COPY model.py ./
