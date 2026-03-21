FROM python:3.11-slim

WORKDIR /app

# Copy requirements
COPY market_pipeline/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy market_pipeline folder contents
COPY market_pipeline/ .

# Expose port
EXPOSE 8080

# Set environment
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Run Flask app (pipeline runs in background thread)
CMD ["python", "app.py"]
