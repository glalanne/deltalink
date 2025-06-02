# Use a slim Python 3.11 base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV POETRY_VERSION=1.8.2
ENV FASTAPI_ENV=production
ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 

# Install system dependencies and Poetry
RUN apt-get update && apt-get install -y curl build-essential pipx \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"

# Install Poetry using pipx
RUN pipx install "poetry==$POETRY_VERSION"

# Set work directory
WORKDIR /app

# Copy only the dependency files first for caching
COPY pyproject.toml poetry.lock* ./

# Install dependencies
RUN poetry install --no-interaction --no-ansi

# Copy the rest of the application
COPY ./deltalink ./deltalink

# Default command (adjust as needed)
CMD ["poetry", "run", "gunicorn", "-k", "uvicorn.workers.UvicornWorker", "deltalink.main:app"]
