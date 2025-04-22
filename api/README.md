# DBT Runner API Usage

This document describes how to interact with the DBT Runner API using `curl`.

## Prerequisites

1.  The API server must be running (e.g., via `uvicorn api.server:app --reload` or your deployment method).
2.  You need a valid API key. This key is configured on the server (likely via an environment variable `API_KEY`).

## Authentication

All endpoints under the `/v1/` path require authentication. You must include your API key in the `X-API-Key` header with each request.

Replace `YOUR_API_KEY` in the examples below with your actual key.

## Base URL

The examples assume the API is running at `http://localhost:8000`. Adjust the URL if your server is running elsewhere.

## Endpoints

### Health Check

Check if the API server is running and healthy. No authentication is required for this endpoint.

```bash
curl http://localhost:8000/health
```

Expected Response:

```json
{"status":"ok"}
```

### Trigger `dbt run`

This endpoint triggers a `dbt run` command on the server.

**Run all models:**

```bash
curl -X POST http://localhost:8000/v1/run \
     -H "X-API-Key: YOUR_API_KEY" \
     -H "Content-Type: application/json"
```

**Run specific models, selectors, or tags:**

Provide a JSON body with a list in the `models` field. This list can contain model names, selectors defined in your dbt project, or tags prefixed with `tag:`.

*Example with specific models:*
```bash
curl -X POST http://localhost:8000/v1/run \
     -H "X-API-Key: YOUR_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"models": ["stg_customers_adworks", "fact_sales_adworks"]}'
```

*Example with a tag:*
```bash
curl -X POST http://localhost:8000/v1/run \
     -H "X-API-Key: YOUR_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"models": ["tag:daily"]}'
```

*Example with a mix:*
```bash
curl -X POST http://localhost:8000/v1/run \
     -H "X-API-Key: YOUR_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"models": ["stg_products_adworks", "tag:hourly"]}'
```

### Trigger `dbt test`

This endpoint triggers a `dbt test` command on the server.

**Test all models:**

```bash
curl -X POST http://localhost:8000/v1/test \
     -H "X-API-Key: YOUR_API_KEY" \
     -H "Content-Type: application/json"
```

**Test specific models, selectors, or tags:**

Provide a JSON body with a list in the `models` field, similar to the `dbt run` endpoint. This list can contain model names, selectors, or tags prefixed with `tag:`.

*Example with specific models:*
```bash
curl -X POST http://localhost:8000/v1/test \
     -H "X-API-Key: YOUR_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"models": ["stg_customers_adworks", "dim_customers_adworks"]}'
```

*Example with a tag:*
```bash
curl -X POST http://localhost:8000/v1/test \
     -H "X-API-Key: YOUR_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"models": ["tag:staging"]}'
```

## Responses

Successful `dbt run` or `dbt test` requests will return a JSON response containing the command executed, status, stdout, and stderr from the dbt process. Errors during dbt execution or authentication issues will result in appropriate HTTP error codes and JSON details.
