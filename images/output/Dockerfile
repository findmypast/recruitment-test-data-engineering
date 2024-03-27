FROM python:3.10

WORKDIR /app

COPY images/output/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY images/output/summary.py ./
COPY model.py ./
