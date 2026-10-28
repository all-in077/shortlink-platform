#!/usr/bin/env bash
GW=http://localhost:8082
EMAIL="e2e_${RANDOM}@example.com"
PASS="testpass123"

check() { # имя, ожидалось, получено
  if [ "$2" = "$3" ]; then echo "PASS  $1"; else echo "FAIL  $1 (ожидалось [$2], получено [$3])"; fi
}
json() { sed -nE "s/.*\"$1\":\"([^\"]+)\".*/\1/p"; }
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
pg()   { docker exec url_shortener_postgres psql -U postgres -d url_shortener -tAc "$1"; }
bill() { docker exec billing-db psql -U postgres -d billing -tAc "$1"; }

echo "== доступ"
check "gateway health" 200 "$(code $GW/health)"
check "auth напрямую закрыт" 000 "$(code --max-time 2 http://localhost:8080/healthz)"
check "shortener напрямую закрыт" 000 "$(code --max-time 2 http://localhost:8081/healthz)"

echo "== аутентификация"
REG=$(curl -s -X POST $GW/api/v1/auth/register -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}")
TOKEN=$(echo "$REG" | json access_token)
RT=$(echo "$REG" | json refresh_token)
check "регистрация выдала токены" yes "$([ -n "$TOKEN" ] && [ -n "$RT" ] && echo yes || echo no)"
check "повторная регистрация" 409 "$(code -X POST $GW/api/v1/auth/register -H 'Content-Type: application/json' -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}")"
check "неверный пароль" 401 "$(code -X POST $GW/api/v1/auth/login -H 'Content-Type: application/json' -d "{\"email\":\"$EMAIL\",\"password\":\"wrong-password\"}")"
check "битый токен" 401 "$(code -X POST $GW/api/v1/shorten -H 'Authorization: Bearer broken' -H 'Content-Type: application/json' -d '{"url":"https://example.com"}')"

echo "== создание ссылки и идемпотентность"
KEY="e2e-$RANDOM"
S1=$(curl -s -o /tmp/b1 -D /tmp/h1 -w '%{http_code}' -X POST $GW/api/v1/shorten \
  -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: $KEY" -H 'Content-Type: application/json' -d '{"url":"https://example.com"}')
S2=$(curl -s -o /tmp/b2 -D /tmp/h2 -w '%{http_code}' -X POST $GW/api/v1/shorten \
  -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: $KEY" -H 'Content-Type: application/json' -d '{"url":"https://example.com"}')
CODE=$(json short_code </tmp/b1)
check "создание ссылки" 201 "$S1"
check "повтор с тем же ключом" 201 "$S2"
check "тот же short_code при повторе" "$CODE" "$(json short_code </tmp/b2)"
check "заголовок Idempotent-Replay" yes "$(grep -qi '^idempotent-replay: true' /tmp/h2 && echo yes || echo no)"

echo "== редирект и счётчик переходов"
check "редирект без токена" 302 "$(code $GW/r/$CODE)"
check "Location" "location: https://example.com" "$(curl -s -D - -o /dev/null $GW/r/$CODE | tr -d '\r' | grep -i '^location:' | tr 'A-Z' 'a-z')"
code $GW/r/$CODE >/dev/null
sleep 1
check "visit_count в Postgres" 3 "$(pg "SELECT visit_count FROM urls WHERE short_code='$CODE'")"

echo "== квота"
U=$(docker exec auth-db psql -U postgres -d auth -tAc "SELECT id FROM users WHERE email='$EMAIL'")
check "квота списана один раз" 1 "$(bill "SELECT used FROM quotas WHERE user_id='$U'")"
check "бронь подтверждена" t "$(bill "SELECT confirmed FROM reservations WHERE user_id='$U' LIMIT 1")"
bill "UPDATE quotas SET limit_count = used WHERE user_id='$U'" >/dev/null
BODY=$(curl -s -w ' %{http_code}' -X POST $GW/api/v1/shorten -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"url":"https://example.com"}')
check "исчерпанная квота" "429" "${BODY##* }"
bill "UPDATE quotas SET limit_count = 50 WHERE user_id='$U'" >/dev/null

echo "== аналитика (ждём пачку и коммит)"
sleep 6
echo "events in ClickHouse for $CODE:"
docker exec url_shortener_clickhouse clickhouse-client --password clickhouse123 \
  --query "SELECT event_type, count() FROM analytics.url_events WHERE short_key='$CODE' GROUP BY event_type ORDER BY event_type"
echo "(ожидается: url_created 1, url_visited 3)"

echo "== ротация refresh-токена, гонка"
N200=$(for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w '%{http_code}\n' -X POST $GW/api/v1/auth/refresh \
    -H 'Content-Type: application/json' -d "{\"refresh_token\":\"$RT\"}" &
done | grep -c '^200$')
check "ровно один успешный refresh из пяти" 1 "$N200"

echo "== лимитер"
sleep 2
N429=$(for i in $(seq 1 60); do curl -s -o /dev/null -w '%{http_code}\n' $GW/health & done | grep -c '^429$')
check "лимитер режет всплеск" yes "$([ "$N429" -gt 0 ] && echo yes || echo no)"
echo "429 из 60: $N429"
