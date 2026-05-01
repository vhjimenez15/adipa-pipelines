.PHONY: dev prod down-dev down-prod logs ps run-light run-heavy

dev:
	docker compose -f docker-compose.dev.yml --env-file .env.dev up --build

prod:
	docker compose -f docker-compose.prod.yml --env-file .env.prod up -d --build

down-dev:
	docker compose -f docker-compose.dev.yml --env-file .env.dev down -v

down-prod:
	docker compose -f docker-compose.prod.yml --env-file .env.prod down -v

logs:
	docker compose -f docker-compose.dev.yml --env-file .env.dev logs -f

ps:
	docker compose -f docker-compose.dev.yml --env-file .env.dev ps

run-light:
	docker compose -f docker-compose.dev.yml --env-file .env.dev exec worker-light python -c \
		"from sync_orders import sync_orders; sync_orders()"

run-heavy:
	docker compose -f docker-compose.dev.yml --env-file .env.dev exec worker-heavy python -c \
		"from daily_kpi_report import daily_kpi_report; daily_kpi_report()"
