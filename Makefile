# Ensures uv is installed
install_init_reqs:
	pip install uv

# Generates requirements.txt from pyproject.toml using uv
# This ensures Docker uses dependencies consistent with the local uv setup
generate_requirements: pyproject.toml
	uv pip compile pyproject.toml -o requirements.txt
	@echo "Generated requirements.txt from pyproject.toml"

deploy_dbt: generate_requirements
	docker compose -f 'compose.dbt.prod.yml' --profile debug down --remove-orphans
	docker compose -f 'compose.dbt.prod.yml' up -d --build --remove-orphans --force-recreate

deploy_dbt_debug: generate_requirements
	docker compose -f 'compose.dbt.prod.yml' --profile debug down --remove-orphans
	docker compose -f 'compose.dbt.prod.yml' --profile debug up -d --build --remove-orphans --force-recreate

deploy_dbt_dev: generate_requirements
	docker compose -f 'compose.dbt.dev.yml' down --remove-orphans
	docker compose -f 'compose.dbt.dev.yml' up -d --build --remove-orphans --force-recreate

# Runs dbt commands locally using uv, after ensuring requirements are synced
run_local: generate_requirements
	uv run dbt deps
	uv run dbt build --select tag:tests

# Add a clean target for generated files (optional but good practice)
clean:
	rm -f requirements.txt

.PHONY: install_init_reqs generate_requirements deploy_dbt deploy_dbt_debug deploy_dbt_dev run_local clean
