.PHONY: dev prod down-dev down-prod logs ps run-light run-heavy

dev:
	docker compose -f docker-compose.dev.yml up --build

prod:
	docker compose -f docker-compose.prod.yml up -d --build

down-dev:
	docker compose -f docker-compose.dev.yml down -v

down-prod:
	docker compose -f docker-compose.prod.yml down -v

logs:
	docker compose -f docker-compose.dev.yml logs -f

ps:
	docker compose -f docker-compose.dev.yml ps

run-light:
	docker compose -f docker-compose.dev.yml exec worker-light python -c \
		"from sync_orders import sync_orders; sync_orders()"

run-heavy:
	docker compose -f docker-compose.dev.yml exec worker-heavy python -c \
		"from daily_kpi_report import daily_kpi_report; daily_kpi_report()"
