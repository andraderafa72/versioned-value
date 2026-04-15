#!/usr/bin/env bash

set -euo pipefail

usage() {
  echo "Uso: $0 <protocol>"
  echo "Protocolos suportados: http | grpc | tcp | udp"
}

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

PROTOCOL="$1"
case "$PROTOCOL" in
  http|grpc|tcp|udp) ;;
  *)
    echo "Protocolo invalido: $PROTOCOL"
    usage
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
MAVEN_OPTS_LOCAL_REPO="-Dmaven.repo.local=$SCRIPT_DIR/.m2/repository"
LOG_DIR="$SCRIPT_DIR/logs"
GATEWAY_ROUTING_ARGS="${GATEWAY_ROUTING_ARGS:-}"
ACCOUNT_ENDPOINTS="${ACCOUNT_INSTANCES:-127.0.0.1:8081,127.0.0.1:8082}"
TRANSACTION_ENDPOINTS="${TRANSACTION_INSTANCES:-127.0.0.1:8083,127.0.0.1:8084}"
mkdir -p "$LOG_DIR"

PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}

free_port_if_busy() {
  local port="$1"
  if command -v fuser >/dev/null 2>&1; then
    # Kill stale processes from previous runs holding service ports.
    fuser -k "${port}/tcp" >/dev/null 2>&1 || true
    return
  fi

  if command -v ss >/dev/null 2>&1; then
    local pids
    pids="$(ss -ltnp 2>/dev/null | rg ":${port}\\b" | rg -o "pid=[0-9]+" | sed 's/pid=//' | sort -u || true)"
    if [[ -n "${pids:-}" ]]; then
      # shellcheck disable=SC2086
      kill $pids >/dev/null 2>&1 || true
    fi
  fi
}

trap cleanup INT TERM EXIT

echo "[INFO] Subindo apps com protocolo: $PROTOCOL"

free_port_if_busy 8080
free_port_if_busy 8081
free_port_if_busy 8082
free_port_if_busy 8083
free_port_if_busy 8084

echo "[INFO] Compilando e instalando shared-core no repositorio local..."
mvn -q $MAVEN_OPTS_LOCAL_REPO -pl shared-core -am -DskipTests install

echo "[INFO] Iniciando Gateway..."
if [[ -z "${GATEWAY_ROUTING_ARGS// }" ]]; then
  GATEWAY_ROUTING_ARGS="--account.instances=$ACCOUNT_ENDPOINTS --transaction.instances=$TRANSACTION_ENDPOINTS"
fi

mvn -q $MAVEN_OPTS_LOCAL_REPO -pl gateway spring-boot:run -Dspring-boot.run.arguments="--protocol=$PROTOCOL $GATEWAY_ROUTING_ARGS" \
  > "$LOG_DIR/gateway.log" 2>&1 &
PIDS+=("$!")
echo "[INFO] Logs Gateway: $LOG_DIR/gateway.log"

echo "[INFO] Iniciando Account Service (instancia 1 na porta 8081)..."
mvn -q $MAVEN_OPTS_LOCAL_REPO -pl account-service spring-boot:run -Dspring-boot.run.arguments="--protocol=$PROTOCOL --port=8081 --instance-id=account-1" \
  > "$LOG_DIR/account-service-1.log" 2>&1 &
PIDS+=("$!")
echo "[INFO] Logs Account Service 1: $LOG_DIR/account-service-1.log"

echo "[INFO] Iniciando Account Service (instancia 2 na porta 8082)..."
mvn -q $MAVEN_OPTS_LOCAL_REPO -pl account-service spring-boot:run -Dspring-boot.run.arguments="--protocol=$PROTOCOL --port=8082 --instance-id=account-2" \
  > "$LOG_DIR/account-service-2.log" 2>&1 &
PIDS+=("$!")
echo "[INFO] Logs Account Service 2: $LOG_DIR/account-service-2.log"

echo "[INFO] Iniciando Transaction Service (instancia 1 na porta 8083)..."
mvn -q $MAVEN_OPTS_LOCAL_REPO -pl transaction-service spring-boot:run -Dspring-boot.run.arguments="--protocol=$PROTOCOL --port=8083 --instance-id=transaction-1" \
  > "$LOG_DIR/transaction-service-1.log" 2>&1 &
PIDS+=("$!")
echo "[INFO] Logs Transaction Service 1: $LOG_DIR/transaction-service-1.log"

echo "[INFO] Iniciando Transaction Service (instancia 2 na porta 8084)..."
mvn -q $MAVEN_OPTS_LOCAL_REPO -pl transaction-service spring-boot:run -Dspring-boot.run.arguments="--protocol=$PROTOCOL --port=8084 --instance-id=transaction-2" \
  > "$LOG_DIR/transaction-service-2.log" 2>&1 &
PIDS+=("$!")
echo "[INFO] Logs Transaction Service 2: $LOG_DIR/transaction-service-2.log"

echo "[INFO] Processos iniciados. Ctrl+C para parar todos."

wait
