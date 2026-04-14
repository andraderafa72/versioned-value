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

echo "[INFO] Compilando e instalando shared-core no repositorio local..."
mvn -q $MAVEN_OPTS_LOCAL_REPO -pl shared-core -am -DskipTests install

echo "[INFO] Iniciando Gateway..."
mvn -q $MAVEN_OPTS_LOCAL_REPO -pl gateway spring-boot:run -Dspring-boot.run.arguments="--protocol=$PROTOCOL" \
  > "$LOG_DIR/gateway.log" 2>&1 &
PIDS+=("$!")
echo "[INFO] Logs Gateway: $LOG_DIR/gateway.log"

echo "[INFO] Iniciando Account Service..."
mvn -q $MAVEN_OPTS_LOCAL_REPO -pl account-service spring-boot:run -Dspring-boot.run.arguments="--protocol=$PROTOCOL" \
  > "$LOG_DIR/account-service.log" 2>&1 &
PIDS+=("$!")
echo "[INFO] Logs Account Service: $LOG_DIR/account-service.log"

echo "[INFO] Iniciando Transaction Service..."
mvn -q $MAVEN_OPTS_LOCAL_REPO -pl transaction-service spring-boot:run -Dspring-boot.run.arguments="--protocol=$PROTOCOL" \
  > "$LOG_DIR/transaction-service.log" 2>&1 &
PIDS+=("$!")
echo "[INFO] Logs Transaction Service: $LOG_DIR/transaction-service.log"

echo "[INFO] Processos iniciados. Ctrl+C para parar todos."

wait
