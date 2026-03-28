FROM python:3.11-slim

WORKDIR /app

COPY market_pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY market_pipeline/ .

EXPOSE 8080

ENV PORT=8080
ENV PYTHONUNBUFFERED=1

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "4", "--timeout", "120", "app:app"]
