FROM ghcr.io/dbt-labs/dbt-bigquery:1.8.2 AS build

# Set working directory
WORKDIR /usr/app

# Set environment variable
ENV DBT_PROFILES_DIR=/root/.dbt

# Copy necessary files
COPY /config/profiles.yml /root/.dbt/profiles.yml
COPY /config/bq-keyfile.json /root/.dbt/bq-keyfile.json

# Entry point for the application
ENTRYPOINT [ "dbt" ]