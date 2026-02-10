# ============================================================================
# Makefile - HackerNews Data Lake
# Comandos básicos para desarrollo local
# ============================================================================

.DEFAULT_GOAL := help
.PHONY: help

# ----------------------------------------------------------------------------
# Variables
# ----------------------------------------------------------------------------
DOCKER_COMPOSE = docker-compose -f infrastructure/docker/docker-compose.yml
TERRAFORM_IMAGE = hashicorp/terraform:1.6
TF_DIR = infrastructure/terraform
-include infrastructure/docker/.env
export

# ----------------------------------------------------------------------------
# Ayuda
# ----------------------------------------------------------------------------
help: ## Muestra los comandos disponibles
	@grep -h -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ----------------------------------------------------------------------------
# Docker
# ----------------------------------------------------------------------------

up: ## Levanta los servicios principales
	$(DOCKER_COMPOSE) up -d

down: ## Detiene todos los servicios
	$(DOCKER_COMPOSE) down -v

restart: down up ## Reinicia los servicios

logs: ## Muestra logs de todos los servicios
	$(DOCKER_COMPOSE) logs -f

ps: ## Lista contenedores activos
	$(DOCKER_COMPOSE) ps

# ----------------------------------------------------------------------------
# Airflow
# ----------------------------------------------------------------------------
airflow-shell: ## Accede al contenedor del scheduler
	$(DOCKER_COMPOSE) exec airflow-scheduler bash

airflow-dags: ## Lista DAGs disponibles
	$(DOCKER_COMPOSE) exec airflow-scheduler airflow dags list

airflow-trigger: ## Ejecuta un DAG (make airflow-trigger DAG=nombre)
	$(DOCKER_COMPOSE) exec airflow-scheduler airflow dags trigger $(DAG)

airflow-scheduler-logs: ## Muestra logs del scheduler
	$(DOCKER_COMPOSE) logs -f airflow-scheduler

airflow-webserver-logs: ## Muestra logs del webserver
	$(DOCKER_COMPOSE) logs -f airflow-webserver

# ----------------------------------------------------------------------------
# MinIO
# ----------------------------------------------------------------------------
minio-shell: ## Accede al contenedor de MinIO
	@$(DOCKER_COMPOSE) exec minio sh -c "mc alias set myminio $(MINIO_ENDPOINT_URL) $(MINIO_ROOT_USER) $(MINIO_ROOT_PASSWORD) 2>/dev/null && echo 'Alias myminio configurado. Usa: mc ls myminio/' && sh"

minio-ls: ## Lista los buckets de MinIO
	@$(DOCKER_COMPOSE) exec -T minio sh -c "mc alias set myminio $(MINIO_ENDPOINT_URL) $(MINIO_ROOT_USER) $(MINIO_ROOT_PASSWORD) 2>/dev/null && mc ls myminio/"

minio-ls-bucket: ## Lista archivos en un bucket (make minio-ls-bucket BUCKET=hackernews-datalake)
	@if [ -z "$(BUCKET)" ]; then \
		echo "Uso: make minio-ls-bucket BUCKET=nombre-del-bucket"; \
		exit 1; \
	fi
	@$(DOCKER_COMPOSE) exec -T minio sh -c "mc alias set myminio $(MINIO_ENDPOINT_URL) $(MINIO_ROOT_USER) $(MINIO_ROOT_PASSWORD) 2>/dev/null && mc ls myminio/$(BUCKET) --recursive"

# ----------------------------------------------------------------------------
# Terraform (usando contenedor)
# ----------------------------------------------------------------------------
tf-init: ## Inicializa Terraform
	docker run --rm \
		-v $(PWD)/$(TF_DIR):/workspace \
		-w /workspace \
		$(TERRAFORM_IMAGE) init

tf-plan: ## Muestra el plan de Terraform
	docker run --rm \
		-v $(PWD)/$(TF_DIR):/workspace \
		-w /workspace \
		--network docker_airflow-network \
		$(TERRAFORM_IMAGE) plan

tf-apply: ## Aplica la infraestructura
	docker run --rm \
		-v $(PWD)/$(TF_DIR):/workspace \
		-w /workspace \
		--network docker_airflow-network \
		$(TERRAFORM_IMAGE) apply -auto-approve

tf-destroy: ## Elimina la infraestructura
	docker run --rm -it \
		-v $(PWD)/$(TF_DIR):/workspace \
		-w /workspace \
		--network docker_airflow-network \
		$(TERRAFORM_IMAGE) destroy

# ----------------------------------------------------------------------------
# Contenedores de desarrollo
# ----------------------------------------------------------------------------
dev-shell: ## Abre una shell en el contenedor de desarrollo
	$(DOCKER_COMPOSE) --profile dev run --rm app-dev bash

dev-run: ## Ejecuta un comando en el contenedor de desarrollo (CMD=...)
	@if [ -z "$(CMD)" ]; then \
		echo "Uso: make dev-run CMD='comando'"; \
		exit 1; \
	fi
	$(DOCKER_COMPOSE) --profile dev run --rm app-dev $(CMD)

# ----------------------------------------------------------------------------
# Contenedor prod
# ----------------------------------------------------------------------------
prod-shell: ## Abre una shell en el contenedor prod
	$(DOCKER_COMPOSE) --profile prod run --rm app-prod bash

prod-run: ## Ejecuta un comando en el contenedor prod (CMD=...)
	@if [ -z "$(CMD)" ]; then \
		echo "Uso: make prod-run CMD='comando'"; \
		exit 1; \
	fi
	$(DOCKER_COMPOSE) --profile prod run --rm app-prod $(CMD)

# ----------------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------------
test: ## Ejecuta los tests en el contenedor de desarrollo
	$(DOCKER_COMPOSE) --profile dev run --rm app-dev uv run pytest
