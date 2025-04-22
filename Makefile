install_init_reqs:
	pip install uv

deploy_dbt:
	docker compose -f 'compose.dbt.prod.yml' down
	docker compose -f 'compose.dbt.prod.yml' up -d --build --remove-orphans --force-recreate

deploy_dbt_debug:
	docker compose -f 'compose.dbt.prod.yml' down
	docker compose -f 'compose.dbt.prod.yml' --profile debug up -d --build --remove-orphans --force-recreate

deploy_dbt_dev:
	docker compose -f 'compose.dbt.dev.yml' down
	docker compose -f 'compose.dbt.dev.yml' up -d --build --remove-orphans --force-recreate

run_local:
	uv run dbt deps
	uv run dbt build --select tag:tests