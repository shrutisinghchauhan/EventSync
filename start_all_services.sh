# ============================================================
#  Service Initialization Script
#  Starts all microservices in the correct dependency order
# ============================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

HEALTH_CHECK_RETRIES=20
HEALTH_CHECK_INTERVAL=4

log_info()    { echo -e "${BLUE}[INFO]${NC}    $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}      $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}    $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC}   $1"; }
log_step()    { echo -e "\n${CYAN}━━━ $1 ━━━${NC}"; }

port_is_open() {
  local host="$1"
  local port="$2"
  (echo > /dev/tcp/"$host"/"$port") 2>/dev/null && return 0
  nc -z "$host" "$port" 2>/dev/null && return 0
  return 1
}

start_container() {
  local name="$1"
  local wait_seconds="${2:-3}"

  local status
  status=$(docker inspect -f '{{.State.Status}}' "$name" 2>/dev/null || echo "not_found")

  if [ "$status" == "not_found" ]; then
    log_error "Container '$name' not found. Run 'docker ps -a' to check names."
    exit 1
  fi

  if [ "$status" == "running" ]; then
    log_warn "$name is already running — skipping"
    return 0
  fi

  log_info "Starting $name ..."
  docker start "$name"

  local retries=0
  while [ "$(docker inspect -f '{{.State.Status}}' "$name" 2>/dev/null)" != "running" ]; do
    if [ "$retries" -ge "$HEALTH_CHECK_RETRIES" ]; then
      log_error "$name failed to reach 'running' state"
      exit 1
    fi
    sleep 1
    retries=$((retries + 1))
  done

  sleep "$wait_seconds"
  log_success "$name is running"
}

wait_for_port() {
  local name="$1"
  local host="$2"
  local port="$3"
  local retries=0

  log_info "Waiting for $name on $host:$port ..."
  until port_is_open "$host" "$port"; do
    if [ "$retries" -ge "$HEALTH_CHECK_RETRIES" ]; then
      log_error "$name port $port not available after ${HEALTH_CHECK_RETRIES} retries"
      exit 1
    fi
    sleep "$HEALTH_CHECK_INTERVAL"
    retries=$((retries + 1))
    log_info "  still waiting ($retries/${HEALTH_CHECK_RETRIES})..."
  done
  log_success "$name is accepting connections on :$port"
}

wait_for_postgres() {
  local retries=0
  log_info "Waiting for PostgreSQL to be ready (pg_isready) ..."
  until docker exec postgres_outbox pg_isready -q 2>/dev/null; do
    if [ "$retries" -ge "$HEALTH_CHECK_RETRIES" ]; then
      log_error "PostgreSQL not ready after ${HEALTH_CHECK_RETRIES} retries"
      exit 1
    fi
    sleep "$HEALTH_CHECK_INTERVAL"
    retries=$((retries + 1))
    log_info "  still waiting ($retries/${HEALTH_CHECK_RETRIES})..."
  done
  log_success "PostgreSQL is ready"
}

wait_for_consumer_postgres() {
  local retries=0
  log_info "Waiting for Consumer PostgreSQL to be ready (pg_isready) ..."
  until docker exec postgres_consumer pg_isready -q 2>/dev/null; do
    if [ "$retries" -ge "$HEALTH_CHECK_RETRIES" ]; then
      log_error "Consumer PostgreSQL not ready after ${HEALTH_CHECK_RETRIES} retries"
      exit 1
    fi
    sleep "$HEALTH_CHECK_INTERVAL"
    retries=$((retries + 1))
    log_info "  still waiting ($retries/${HEALTH_CHECK_RETRIES})..."
  done
  log_success "Consumer PostgreSQL is ready"
}

wait_for_kafka() {
  local retries=0
  log_info "Waiting for Kafka broker to be ready ..."
  until docker exec kafka kafka-broker-api-versions \
        --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    if [ "$retries" -ge "$HEALTH_CHECK_RETRIES" ]; then
      log_error "Kafka not ready after ${HEALTH_CHECK_RETRIES} retries"
      exit 1
    fi
    sleep "$HEALTH_CHECK_INTERVAL"
    retries=$((retries + 1))
    log_info "  still waiting ($retries/${HEALTH_CHECK_RETRIES})..."
  done
  log_success "Kafka broker is ready"
}

wait_for_debezium() {
  local retries=0
  log_info "Waiting for Debezium REST API (http://localhost:8083) ..."
  until curl -sf http://localhost:8083/connectors > /dev/null 2>&1; do
    if [ "$retries" -ge "$HEALTH_CHECK_RETRIES" ]; then
      log_error "Debezium REST API not reachable after ${HEALTH_CHECK_RETRIES} retries"
      exit 1
    fi
    sleep "$HEALTH_CHECK_INTERVAL"
    retries=$((retries + 1))
    log_info "  still waiting ($retries/${HEALTH_CHECK_RETRIES})..."
  done
  log_success "Debezium REST API is ready"
}


echo ""
echo -e "${CYAN}╔══════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║   Microservices Initialization Script    ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════╝${NC}"
echo ""

# ------ STAGE 1: Infrastructure --------------------------------
log_step "Stage 1 — Infrastructure (Zookeeper → Postgres → Kafka)"

start_container "zookeeper" 5
start_container "postgres_outbox" 5
wait_for_postgres
start_container "postgres_consumer" 5
wait_for_consumer_postgres

start_container "kafka" 8
wait_for_kafka                        

start_container "kafka-ui" 2

# ------ STAGE 2: CDC -------------------------------------------
log_step "Stage 2 — Change Data Capture (Debezium)"

start_container "debezium-connect" 5
wait_for_debezium

# ------ STAGE 3: Business Services ----------------------------
log_step "Stage 3 — Business Services"

start_container "order_service_1-backend-1" 3
start_container "order_service_2-backend-1" 3
start_container "consumer_service-backend-1" 3

# ------ STAGE 4: Pollers --------------------------------------
log_step "Stage 4 — Pollers"

start_container "poller_1_transactional_outbox-backend-1" 2
start_container "poller_2_transactional_outbox-backend-1" 2
start_container "poller_1_listen_to_yourself-backend-1" 2
start_container "poller_2_listen_to_yourself-backend-1" 2

# ------ Summary -----------------------------------------------
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   All services started successfully! ✓   ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════╝${NC}"
echo ""

log_step "Service Status"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" \
  | grep -E "NAME|zookeeper|postgres_outbox|postgres_consumer|kafka|debezium|order_service|consumer_service|poller"
echo ""

echo -e "  ${CYAN}Kafka UI   →${NC}  http://localhost:8080"
echo -e "  ${CYAN}Debezium   →${NC}  http://localhost:8083"
echo -e "  ${CYAN}Order Svc1 →${NC}  http://localhost:3000"
echo -e "  ${CYAN}Order Svc2 →${NC}  http://localhost:3001"
echo -e "  ${CYAN}Consumer   →${NC}  http://localhost:9000"
echo -e "  ${CYAN}Pollers    →${NC}  :8000  :8001  :8002  :8003"
echo ""
