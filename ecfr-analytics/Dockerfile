# eCFR Analytics - Combined UI + API Container for Cloud Run
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    nginx \
    && rm -rf /var/lib/apt/lists/*

# Install uv for Python package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Copy and install Python dependencies
COPY api/requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

# Copy API code
COPY api/main.py .
COPY api/.env.example .env

# Copy UI files and configure nginx
COPY ui/ /var/www/html/
COPY api/nginx.conf /etc/nginx/sites-available/default

# Set proper permissions for UI files
RUN chmod -R 644 /var/www/html/ && chmod 755 /var/www/html/

# Create startup script
COPY api/start.sh .
RUN chmod +x start.sh

# Cloud Run expects port 8080
ENV PORT=8080
EXPOSE 8080

# Start both nginx (UI) and uvicorn (API)
CMD ["./start.sh"]