run_dbt:
	uv run dbt run

run_dbt_docs:
	uv run dbt docs generate
	uv run dbt docs serve --port 8080

make_init_reqs:
	pip install uv