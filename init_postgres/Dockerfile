FROM python:3.10-slim

WORKDIR /app

COPY ./init_postgres/requirements.txt .
COPY ./migrations /migrations
RUN pip install --no-cache-dir -r requirements.txt

COPY ./init_postgres/load_data.py .


CMD ["python", "load_data.py"]
