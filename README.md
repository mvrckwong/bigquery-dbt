# General DBT Bigquery - Data Transformation

## Project Overview

This dbt (data build tool) project is designed to transform raw data into analytics-ready datasets. It follows a modular and layered approach, leveraging dbt's best practices to manage and orchestrate complex data transformations. The project focuses on modeling data from various sources into a structured and efficient data warehouse, ready for business intelligence and analytical use cases.

### Technologies Used

*   **dbt (Data Build Tool):** The core technology for data transformation, enabling SQL-based development and testing.
*   **Docker:** Used for containerization, ensuring consistency across different development and deployment environments.
*   **PostgreSQL:** The chosen data warehouse solution for storing and managing transformed data.
* **Make:** Used for project automation and ease the execution of tasks
* **VSCode:** As code editor

## Deployment Strategy

This project can be deployed using the following methods:

1.  **Local Deployment:** Ideal for development and testing in a local environment.
2.  **Docker Deployment:** Provides a consistent and reproducible environment, suitable for staging and production.
3.  **Cloud Deployment:** Enables scalable and managed deployments in cloud environments (e.g., AWS, GCP, Azure).

## Project Structure

The project is organized into several key directories:

*   **models/:** Contains the SQL files that define the transformation logic, split into staging (silver), intermediate, and presentation (gold) layers.
*   **snapshots/:** Stores the definitions of how to capture changes in the source data over time.
*   **macros/:** Houses reusable SQL code snippets that can be called from models or tests.
* **config/**: Contains the dbt profile file.
* **.vscode/**: Contains the project settings for vscode
* **tests/**: Includes test cases to validate the transformation logic.


## Getting Started

### Prerequisites

*   Docker

docker exec -it dbt-instance-prod-dbt-docs-server-1 /bin/bash -c "dbt run"

### Running the Project

1.  **Build and run the docker container**

    