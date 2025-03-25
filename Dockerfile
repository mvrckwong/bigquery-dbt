FROM ghcr.io/dbt-labs/dbt-bigquery:1.8.2 AS build

# Set working directory
WORKDIR /usr/app

# Set environment variable
ENV DBT_PROFILES_DIR=/root/.dbt

# Copy necessary files
COPY /config/profiles.yml /root/.dbt/profiles.yml
COPY /config/bq-keyfile.json /root/.dbt/bq-keyfile.json

# If needed, create a non-root user (check if the image already does this)
# RUN addgroup -S app && adduser -S app -G app
# RUN chown -R app:app /usr/app

# Final stage for minimal image (if needed)
# FROM alpine:latest
# WORKDIR /usr/app
# COPY --from=build /usr/app /usr/app

# Entry point for the application
ENTRYPOINT [ "dbt" ]