# shortlink-platform

[![CI](https://github.com/all-in077/shortlink-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/all-in077/shortlink-platform/actions)

Учебная микросервисная платформа на Go. Предметная область (сокращатель ссылок) выбрана
специально простой: основное внимание уделено паттернам, которые используются в
продакшен-системах: transactional outbox, сага с компенсацией, идемпотентность,
консистентность при горизонтальном масштабировании, Kubernetes.

## Архитектура

~~~mermaid
flowchart LR
    C[Клиент] --> G[Gateway]
    G -- gRPC ValidateToken --> A[Auth]
    G -- HTTP --> S[Shortener]
    G -- HTTP login/register --> A
    G -. лимит по IP .-> R[(Redis)]
    S -- quota-requests --> K{{Kafka}}
    K -- quota-requests --> B[Billing]
    B -- quota-responses --> K
    K -- quota-responses --> S
    S -- quota-confirmations --> K
    K -- quota-confirmations --> B
    S -- "outbox: url-events" --> K
    K -- url-events --> AN[Analytics]
    K -- url-events --> N[Notification]
    AN --> CH[(ClickHouse)]
    S --> PG1[(Postgres: shortener)]
    S -. "кэш, идемпотентность" .-> R
    A --> PG2[(Postgres: auth)]
    B --> PG3[(Postgres: billing)]
~~~

| Сервис | Роль | Хранилище |
|---|---|---|
| gateway | единая точка входа: лимит запросов по IP, проверка JWT, проксирование | Redis (счётчики лимитера) |
| auth | регистрация, вход, JWT, ротация refresh-токенов | Postgres |
| shortener | создание ссылок, редиректы, outbox | Postgres, Redis (кэш, идемпотентность) |
| billing | квоты пользователей, резервирование, компенсация | Postgres |
| analytics | пачечная запись событий в ClickHouse | ClickHouse |
| notification | вебхук при достижении порога переходов | в памяти |

У каждого сервиса свой `go.mod` и своя база (database per service).

## Ключевые решения

**Сага резервирования квоты (хореография через Kafka).** Создание ссылки затрагивает две базы
(shortener и billing), общей транзакции между ними нет.

1. Shortener публикует `quota-requests` и ждёт ответ до 3 секунд.
2. Billing атомарно резервирует единицу квоты (`INSERT` брони, затем условный `UPDATE`
   в одной транзакции) и отвечает в `quota-responses`.
3. Shortener создаёт ссылку, публикует `quota-confirmations` и только потом отвечает клиенту.
4. Если подтверждение не пришло за 10 секунд, sweeper в Billing возвращает квоту (компенсация).

Резервирование идемпотентно по `request_id`, поэтому повторная доставка не списывает квоту дважды.
Компенсация использует `DELETE` брони как замок: при нескольких экземплярах квоту вернёт только один.

**Transactional outbox.** Событие и данные пишутся в одной транзакции, воркер переносит события
в Kafka (`FOR UPDATE SKIP LOCKED` внутри транзакции). Гарантия at-least-once.

**Аналитика.** Consumer накапливает до 100 событий или 2 секунды, пишет пачку в ClickHouse
и только потом коммитит офсеты (at-least-once, при сбое ClickHouse буфер не теряется).

**Аутентификация.** Access JWT (HS256, 15 минут) и одноразовые refresh-токены с ротацией.
Повторное использование токена считается кражей и закрывает все сессии пользователя.
Отзыв токена атомарен (`UPDATE ... WHERE revoked = false`), поэтому параллельные запросы
с одним токеном не могут оба успеть.

**Идемпотентность.** Необязательный заголовок `Idempotency-Key`: повтор получает сохранённый
ответ (заголовок `Idempotent-Replay: true`) без новой саги и без повторного списания квоты.

**Масштабирование.** Ожидание ответа саги хранится в памяти пода, поэтому у каждого пода
shortener своя consumer group и все поды видят все ответы. Лимитер хранит счётчики в Redis,
поэтому лимит не растёт с числом реплик. Оба эффекта были воспроизведены на двух репликах
в Kubernetes и затем исправлены.

## Запуск (Docker Compose)

~~~bash
cd services
docker compose up -d --build
./../scripts/e2e.sh
~~~

Единственный внешний вход: `http://localhost:8082` (gateway).

~~~bash
# регистрация
curl -X POST localhost:8082/api/v1/auth/register -H 'Content-Type: application/json' \
  -d '{"email":"me@example.com","password":"testpass123"}'

# создание ссылки
curl -X POST localhost:8082/api/v1/shorten \
  -H "Authorization: Bearer <access_token>" -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com"}'

# редирект (без токена)
curl -i localhost:8082/r/<short_code>
~~~

Prometheus: `localhost:9090`, Grafana: `localhost:3000`. Пароли в `docker-compose.yaml` это
значения для разработки.

## Kubernetes

> Манифесты проверены на kind до последних правок кода; перед запуском образы нужно пересобрать и загрузить.

Манифесты лежат в `deploy/k8s` (kind): namespace, ConfigMap и Secret, StatefulSet для баз,
Kafka и ZooKeeper, Deployment с пробами `/healthz` и `/readyz`. Секреты в репозиторий не
входят: шаблон в `deploy/k8s/examples/11-secrets.example.yaml`. Перед запуском образы нужно
собрать и загрузить командой `kind load docker-image`.

## Тесты

~~~bash
cd services/auth && go test ./... -race -cover
cd services/billing && go test ./... -race -cover
~~~

Покрыты бизнес-логика auth (регистрация, вход, ротация и гонка refresh-токена)
и billing (резервирование, sweeper). Сквозной сценарий: `scripts/e2e.sh`.

## Известные ограничения

- Ежемесячный сброс квоты не реализован (`reset_at` хранится, но не применяется).
- Ожидание ответа саги внутри HTTP-запроса: в продакшене обычно отвечают `202` и дают опрос статуса.
- Каждый под shortener читает все ответы саги, а старые consumer groups остаются в Kafka.
- В ClickHouse возможны дубли при повторной доставке (решение: `ReplacingMergeTree` по `event_id`).
- Хранилище идемпотентности в notification в памяти: после перезапуска вебхук может уйти повторно.
- Нет dead-letter топиков: событие, которое не обрабатывается, пропускается или блокирует партицию.
- Лимитер использует fixed window (на границе окон возможен всплеск до двух лимитов)
  и доверяет `RemoteAddr`, за балансировщиком нужен `X-Forwarded-For`.
- Подтверждение, пришедшее позже таймаута компенсации, не восстанавливает квоту.
- У gateway нет `/metrics`.
- Просроченные refresh-токены не удаляются, нет logout и отзыва access-токена.
- `Refresh` не атомарен целиком (отзыв и выдача нового токена в разных запросах).
- Время ответа логина зависит от существования email, email не нормализуется.
- Тесты написаны для auth и billing, остальные сервисы проверяются сквозным сценарием.
