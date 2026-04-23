.PHONY: up down logs ps test clean aggregate export generate-events

# Поднять весь стек
up:
	docker compose up -d --build
	@echo ""
	@echo "========================================================"
	@echo "Стек запущен. Сервисы:"
	@echo "  Producer API    : http://localhost:8000/docs"
	@echo "  Aggregation API : http://localhost:8001/docs"
	@echo "  Export API      : http://localhost:8002/docs"
	@echo "  ClickHouse      : http://localhost:8123"
	@echo "  Grafana         : http://localhost:3000 (admin/admin)"
	@echo "  MinIO console   : http://localhost:9002 (minioadmin/minioadmin)"
	@echo "  Schema Registry : http://localhost:8081"
	@echo "========================================================"

# Остановить все сервисы (данные в volumes сохраняются)
down:
	docker compose down

# Полный сброс — удалить и контейнеры, и volumes
clean:
	docker compose down -v
	@echo "Все данные удалены."

# Логи всех сервисов
logs:
	docker compose logs -f --tail=100

# Статус контейнеров
ps:
	docker compose ps

# Запустить интеграционный тест
test:
	docker compose --profile tests run --rm tests

# Запуск одного цикла агрегации за вчера (через HTTP)
aggregate:
	curl -X POST http://localhost:8001/run

# Ручной экспорт в S3 за вчера
export:
	curl -X POST http://localhost:8002/run

# Проверить данные в ClickHouse
query-ch:
	docker exec -it clickhouse clickhouse-client --query "SELECT event_type, count() FROM analytics.movie_events_raw GROUP BY event_type ORDER BY event_type"

# Проверить агрегаты в PostgreSQL
query-pg:
	docker exec -it postgres psql -U analytics -d analytics -c "SELECT metric_date, metric_name, metric_value FROM metrics_daily ORDER BY metric_date DESC, metric_name LIMIT 20;"

# Проверить файлы в MinIO
query-s3:
	docker exec -it minio-init mc ls -r local/movie-analytics/ || docker run --rm --network task5_default -e MC_HOST_local=http://minioadmin:minioadmin@minio:9000 minio/mc ls -r local/movie-analytics/

# Тестовое событие через продюсер
send-test-event:
	curl -X POST http://localhost:8000/events \
		-H "Content-Type: application/json" \
		-d '{"user_id":"demo","movie_id":"movie_1","event_type":"VIEW_STARTED","device_type":"DESKTOP","session_id":"demo-session","progress_seconds":0}'
