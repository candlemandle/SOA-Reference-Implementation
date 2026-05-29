# Домашнее задание №7 — CI/CD, Testing & Observability

## Система

Warehouse Management System из ДЗ №6:
- **WMS Service** (FastAPI, порт 8000) — HTTP API для отправки складских событий в Kafka
- **Consumer Service** (FastAPI, порт 8001) — Kafka consumer, обрабатывает события и записывает состояние в Cassandra
- **Kafka** + Schema Registry + Zookeeper — очередь сообщений с Avro-сериализацией
- **Cassandra** — хранение состояния инвентаря
- **Prometheus** — сбор метрик
- **Grafana** — визуализация
- **Alertmanager** — алерты
- **kafka-exporter** — метрики Kafka для Prometheus

## Быстрый старт

```bash
cd "Task 7"
docker compose up -d --build
```

Все компоненты поднимаются одной командой. Дождитесь готовности:
```bash
bash scripts/wait_for_services.sh
```

### URL-адреса

| Компонент | URL                                  |
|---|--------------------------------------|
| WMS Service | http://localhost:8000                |
| Consumer Service | http://localhost:8001                |
| Prometheus | http://localhost:9090                |
| Grafana | http://localhost:3000 (admin/admin1) |
| Alertmanager | http://localhost:9093                |

---

## Структура проекта

```
Task 7/
├── .github/workflows/ci.yml        # CI pipeline (GitHub Actions)
├── docker-compose.yml               # Вся инфраструктура
├── wms_service/                     # WMS Service (producer)
│   ├── main.py                      # API + Kafka producer
│   ├── models.py                    # Pydantic-модели событий
│   ├── metrics.py                   # Prometheus middleware
│   ├── Dockerfile
│   └── requirements.txt
├── consumer_service/                # Consumer Service
│   ├── main.py                      # Kafka consumer + HTTP API
│   ├── handlers.py                  # Обработчики событий
│   ├── cassandra_client.py          # Клиент Cassandra с метриками
│   ├── metrics.py                   # Prometheus middleware + event/infra метрики
│   ├── Dockerfile
│   └── requirements.txt
├── schemas/                         # Avro-схемы (v1, v2)
├── cassandra/                       # Схема БД + init-скрипт
├── prometheus/
│   ├── prometheus.yml               # Конфигурация scrape
│   └── alert_rules.yml              # Alert rules + SLI alerts
├── alertmanager/
│   └── alertmanager.yml             # Конфигурация Alertmanager
├── grafana/
│   ├── provisioning/                # Auto-provisioning Grafana
│   └── dashboards/
│       ├── services.json            # Дашборд сервисов
│       └── infrastructure.json      # Дашборд инфраструктуры
├── tests/
│   ├── unit/                        # Модульные тесты
│   │   ├── test_wms_validation.py   # Валидация моделей WMS
│   │   └── test_consumer_handlers.py # Логика обработчиков
│   ├── integration/                 # Интеграционные тесты
│   │   └── test_service_interaction.py
│   └── e2e/                         # E2E тесты
│       └── test_e2e_scenario.py
├── load_tests/
│   └── load_test.js                 # k6 нагрузочный тест
└── scripts/
    ├── wait_for_services.sh         # Ожидание готовности сервисов
    └── check_metrics.sh             # Проверка метрик из Prometheus
```

---

## 1. CI Pipeline (1 балл)

Файл: `.github/workflows/ci.yml`

Pipeline запускается автоматически при push/PR и содержит:
- **build** — сборка Docker-образов обоих сервисов
- **unit-tests** — модульные тесты (pytest) без Docker-зависимостей
- **integration-tests** — поднимает docker-compose, запускает интеграционные, E2E, нагрузочные тесты и проверку метрик

Pipeline падает при любой ошибке. Артефакты (результаты тестов, логи) сохраняются.

```bash
# Запустить unit тесты локально:
pip install -r tests/requirements.txt
pytest tests/unit/ -v
```

## 2. Интеграционные тесты (1 балл)

Файл: `tests/integration/test_service_interaction.py`

Тесты проверяют взаимодействие WMS → Kafka → Consumer → Cassandra:
- Отправка события через WMS API, проверка обработки в Cassandra
- RECEIVE + SHIP pipeline
- Идемпотентность (дублирование событий)
- Доступность /metrics на обоих сервисах

```bash
# Требуют запущенного docker-compose
pytest tests/integration/ -v
```

## 3. E2E тест (1 балл)

Файл: `tests/e2e/test_e2e_scenario.py`

Полный складской сценарий:
1. PRODUCT_RECEIVED (200 единиц) → проверка available=200
2. PRODUCT_RESERVED (50 единиц) → проверка available=150, reserved=50
3. ORDER_CREATED (30 единиц) → проверка заказа в БД, available=120, reserved=80
4. ORDER_COMPLETED → проверка статуса заказа, reserved=50

Проверяются HTTP-статусы, тело ответа (поля, типы), состояние в БД.

```bash
pytest tests/e2e/ -v
```

## 4. Prometheus + базовые метрики (1 балл)

Каждый сервис экспортирует метрики по `/metrics`:

| Метрика | Тип | Labels | Описание |
|---|---|---|---|
| `http_requests_total` | Counter | method, endpoint, status | Количество запросов |
| `http_request_errors_total` | Counter | method, endpoint, error_type | Количество ошибок |
| `http_request_duration_seconds` | Histogram | method, endpoint | Время обработки запроса |

Consumer Service дополнительно экспортирует:
- `events_processed_total` — обработанные события (по типу)
- `event_processing_duration_seconds` — время обработки событий
- `consumer_lag` — лаг консьюмера по партициям
- `cassandra_read/write_duration_seconds` — латенсия Cassandra

Prometheus scrape config: `prometheus/prometheus.yml`

## 5. Grafana: дашборды сервисов (1 балл)

Файл: `grafana/dashboards/services.json`

8 панелей:
1. WMS — Request Rate (RPS) по эндпоинтам
2. WMS — Latency p50/p95/p99
3. WMS — Error Rate
4. WMS — Total Requests (stat)
5. WMS — Error Count (stat с threshold-цветами)
6. Consumer — Events Processed/sec по типам
7. Consumer — Event Processing Latency p50/p95/p99
8. Consumer — Consumer Lag по партициям

Дашборд обновляется каждые 5 секунд. Auto-provisioning через Grafana provisioning.

## 6. Grafana: дашборд инфраструктуры (1 балл)

Файл: `grafana/dashboards/infrastructure.json`

7 панелей:
1. Kafka — Consumer Group Lag
2. Kafka — Messages In/sec по топикам
3. Kafka — Broker Count (stat)
4. Cassandra — Write Latency p50/p95/p99
5. Cassandra — Read Latency p50/p95
6. Cassandra — Write Errors rate
7. Cassandra — Connected Hosts (gauge)

Метрики Kafka собираются через `kafka-exporter`. Метрики Cassandra — через application-level инструментацию в consumer service.

## 7. Нагрузочное тестирование (1 балл)

Файл: `load_tests/load_test.js` (k6)

Конфигурация:
- 10 VU (virtual users), ~40 секунд
- Ramp-up: 10s → 10 VU, sustain 20s, ramp-down 10s
- Тест отправляет PRODUCT_RECEIVED события через WMS API

Пороги (thresholds):
- p95 latency < 500ms
- error rate < 1%

При превышении порогов k6 возвращает exit code 1, CI падает.

```bash
k6 run load_tests/load_test.js
```

## 8. E2E + нагрузка + метрики в одном CI (1 балл)

В CI pipeline (job `integration-tests`):
1. `docker compose up` — поднимает всю систему
2. Интеграционные и E2E тесты
3. k6 нагрузочный тест
4. `scripts/check_metrics.sh` — запрашивает Prometheus API и проверяет:
   - error rate < 1%
   - p95 latency < 1000ms
   - availability > 95%
   - event processing p95 < 5s

CI падает при нарушении любого условия.

## 9. Prometheus alert rules (1 балл)

Файл: `prometheus/alert_rules.yml`

Alert rules:
| Alert | Условие | For |
|---|---|---|
| HighErrorRate | error rate > 5% за 5 мин | 2m |
| HighLatencyP95 | p95 > 1s за 5 мин | 2m |
| ServiceDown | target down | 1m |
| ConsumerLagHigh | lag > 100 сообщений | 2m |

Alertmanager поднимается в docker-compose. Алерты видны в Alertmanager UI (http://localhost:9093) и Grafana (Alerting → Alert Rules).

Демонстрация срабатывания: при отключении consumer-service алерт ServiceDown переходит в firing в течение 1 минуты.

## 10. System-level SLI и пороги отказа (1 балл)

### Определённые SLI

| SLI | PromQL | SLO | Порог отказа |
|---|---|---|---|
| API Latency p95 | `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{job="wms-service"}[5m])) by (le))` | < 500ms | > 1000ms |
| API Availability | `sum(rate(http_requests_total{status=~"2.."}[5m])) / sum(rate(http_requests_total[5m]))` | > 99.5% | < 95% |
| Event Processing Delay p95 | `histogram_quantile(0.95, sum(rate(event_processing_duration_seconds_bucket[5m])) by (le))` | < 2s | > 5s |

### Обоснование порогов

**API Latency p95 < 500ms (SLO), > 1000ms (отказ):**
WMS Service принимает HTTP-запросы и отправляет их в Kafka. Операция produce + flush обычно занимает 5-50ms. SLO 500ms даёт 10x запас. Порог отказа 1000ms означает серьёзную деградацию (Kafka перегружен или сеть).

**API Availability > 99.5% (SLO), < 95% (отказ):**
Для внутреннего складского API 99.5% — разумный SLO. При < 95% (1 из 20 запросов падает) система непригодна для использования.

**Event Processing Delay p95 < 2s (SLO), > 5s (отказ):**
Обработка события включает десериализацию, проверку идемпотентности, запись батча в Cassandra. Обычно 10-100ms. SLO 2s допускает кратковременные задержки. При > 5s накапливается критический consumer lag.

### Использование SLI

- В CI: `scripts/check_metrics.sh` проверяет SLI через Prometheus API после нагрузочного теста
- В алертах: `prometheus/alert_rules.yml` содержит SLI-based alert rules (группа `sli_alerts`), которые срабатывают при пересечении порога отказа
