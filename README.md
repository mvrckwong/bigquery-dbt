# BigQuery dbt Project

A modern data transformation project using dbt (data build tool) with BigQuery.

## Project Structure

The project follows a medallion architecture with three main layers:

### Bronze Layer (Raw)
- Located in `models/bronze/`
- Contains raw data ingestion models
- Minimal transformations, primarily type casting and renaming
- Preserves source data granularity

### Silver Layer (Staging)
- Located in `models/silver/`
- Contains cleaned and standardized data models
- Implements data quality tests and constraints
- Handles data type conversions and basic transformations
- Establishes relationships between different data entities

### Gold Layer (Production)
- Located in `models/gold/`
- Contains business-level aggregations and metrics
- Implements business logic and complex transformations
- Optimized for analytical queries and reporting

## Key Features

- **Incremental Processing**: Models use incremental loading where appropriate
- **Data Quality**: Comprehensive testing framework implemented in schema.yml files
- **Audit Trails**: Built-in audit columns tracking data lineage
- **Documentation**: Detailed documentation for all models and transformations

## Getting Started

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up your BigQuery credentials in `~/.dbt/profiles.yml`

3. Run the models:
```bash
dbt run
```

4. Run tests:
```bash
dbt test
```

## Best Practices

1. **Naming Conventions**:
   - Models: Use prefixes based on layer (bronze_, stg_, fct_, dim_)
   - Tests: Name custom tests with descriptive names
   - Sources: Use meaningful source names that reflect origin

2. **Testing**:
   - All primary keys must have unique and not_null tests
   - Foreign keys must have referential integrity tests
   - Custom data quality tests for business rules

3. **Documentation**:
   - All models must have descriptions
   - Critical columns must be documented
   - Business logic must be explained in model descriptions

4. **Materialization Strategy**:
   - Bronze: Usually incremental
   - Silver: Mix of incremental and table based on size/update frequency
   - Gold: Usually table materialization for query performance

## Macros

- `generate_schema_name`: Handles schema name generation
- `audit_helper`: Provides standardized audit columns
- `override_schema_name`: Custom schema naming logic

## Contributing

1. Create a new branch for your changes
2. Follow the existing code structure and naming conventions
3. Add appropriate tests and documentation
4. Submit a pull request

## Resources
- [dbt Documentation](https://docs.getdbt.com/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [dbt Discourse](https://discourse.getdbt.com/)