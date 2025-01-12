install_init_reqs:
	pip install uv

run_dbt:

run_dbt_docs:
	uv run dbt docs generate
	uv run dbt docs serve --port 8080

run_compose_dbt:
	docker compose -f 'compose.dbt.yml' down
	docker compose -f 'compose.dbt.yml' up -d --build