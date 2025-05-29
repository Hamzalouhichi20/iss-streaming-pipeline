FROM python:3.10-slim

WORKDIR /app

# Installer directement les bibliothèques nécessaires
RUN pip install --no-cache-dir requests kafka-python

COPY producer.py .

CMD ["python", "producer.py"]
