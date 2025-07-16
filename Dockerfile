FROM python:3.11
LABEL maintainer="josef.strba"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY prometheus.yml .

CMD ["python", "main.py"]
