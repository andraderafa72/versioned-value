#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAVEN_LOCAL="-Dmaven.repo.local=$ROOT/.m2/repository"

usage() {
  echo "Uso: $0 <protocolo> <porta> <instance-id>" >&2
  echo "Protocolos: http | grpc | tcp | udp" >&2
}

if [[ $# -ne 3 ]]; then
  usage
  exit 1
fi

case "$1" in
  http | grpc | tcp | udp) ;;
  *)
    echo "protocolo invalido: $1" >&2
    usage
    exit 1
    ;;
esac

PROTOCOL="$1"
PORT="$2"
INSTANCE_ID="$3"

# Local Redis (127.0.0.1:6379). Override with REDIS_ARGS="" or custom flags when needed.
REDIS_ARGS="${REDIS_ARGS:---redis.enabled=true --redis.host=127.0.0.1 --redis.port=6379}"

SPRING_ARGS="--protocol=$PROTOCOL --port=$PORT --instance-id=$INSTANCE_ID $REDIS_ARGS"

cd "$ROOT"
exec mvn $MAVEN_LOCAL -pl account-service spring-boot:run -Dspring-boot.run.arguments="$SPRING_ARGS"
