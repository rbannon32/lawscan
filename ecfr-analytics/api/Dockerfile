# Container for FastAPI service
FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app
COPY pyproject.toml .
RUN uv pip install --system --no-cache .

COPY main.py .

ENV HOST=0.0.0.0 PORT=8080
CMD [ "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080" ]
