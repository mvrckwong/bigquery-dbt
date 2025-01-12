FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /usr/app

# Copy project files
COPY . .

# Install dbt and other dependencies
RUN pip install --no-cache-dir dbt-bigquery google-cloud-bigquery

# Create directory for dbt logs
RUN mkdir -p /root/.dbt

# Default command
CMD ["bash"]
