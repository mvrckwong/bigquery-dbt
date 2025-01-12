run_dbt:
	poetry run dbt run

run_dbt_docs:
	poetry run dbt docs generate
	poetry run dbt docs serve --port 8001

make_init_reqs:
	pip install uv