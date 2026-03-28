FROM python:3.11-slim

WORKDIR /app

# Copy requirements
COPY market_pipeline/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY market_pipeline/ .

# Expose port
EXPOSE 8080

# Environment
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Start app
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "4", "--timeout", "120", "app:app"]
