FROM python:3.11-slim

WORKDIR /app

# Copy requirements
COPY market_pipeline/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY market_pipeline/ .

# Run pipeline
CMD ["python", "main.py"]
