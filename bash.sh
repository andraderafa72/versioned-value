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

PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}

trap cleanup INT TERM EXIT

echo "[INFO] Subindo apps com protocolo: $PROTOCOL"

echo "[INFO] Iniciando Gateway..."
mvn -q -pl gateway exec:java -Dexec.args="--protocol=$PROTOCOL" &
PIDS+=("$!")

echo "[INFO] Iniciando Account Service..."
mvn -q -pl account-service exec:java -Dexec.args="--protocol=$PROTOCOL" &
PIDS+=("$!")

echo "[INFO] Iniciando Transaction Service..."
mvn -q -pl transaction-service exec:java -Dexec.args="--protocol=$PROTOCOL" &
PIDS+=("$!")

echo "[INFO] Processos iniciados. Ctrl+C para parar todos."

wait
