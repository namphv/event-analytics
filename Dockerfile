# --- Base Stage ---
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# --- Dependencies Stage ---
FROM base as dependencies

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# --- Development Stage ---
FROM dependencies as development

# Copy dev requirements
COPY requirements-dev.txt .

# Install dev dependencies
RUN pip install --no-cache-dir -r requirements-dev.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Run with auto-reload for development
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# --- Production Stage ---
FROM dependencies as production

# Copy only necessary application files
COPY ./app /app/app
COPY ./scripts /app/scripts

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Run without auto-reload for production
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
