install_init_reqs:
	pip install uv

run_prod_compose:
	docker compose -f 'compose.dbt.prod.yml' down
	docker compose -f 'compose.dbt.prod.yml' --profile run up -d --build

run_dev_compose:
	docker compose -f 'compose.dbt.yml' down
	docker compose -f 'compose.dbt.yml' up -d --build

run_local:
	uv run dbt deps
	uv run dbt build --select tag:tests