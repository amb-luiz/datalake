#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="/opt/apps/datalake"
DEPLOY_BASE="$APP_DIR/.deploy"
BACKUP_DIR="$DEPLOY_BASE/backups"
AIRFLOW_DIR="$APP_DIR/airflow-pipeline"
PYSPARK_DIR="$APP_DIR/pyspark-library"
AIRFLOW_COMPOSE_FILE="$AIRFLOW_DIR/docker-compose.yaml"
AIRFLOW_SECRET_ENV_FILE="/opt/apps/secrets/airflow.env"

if [[ -f "$AIRFLOW_SECRET_ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$AIRFLOW_SECRET_ENV_FILE"
  set +a
fi

log() {
  printf '%s %s\n' "$(date '+%F %T')" "$*"
}

usage() {
  cat <<'EOF'
Uso:
  rollback.sh --backup /caminho/backup.tar.gz
  rollback.sh --list

Opcoes:
  --backup   Restaura um backup gerado pelo deploy_auto.sh
  --list     Lista os backups disponiveis
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    log "ERRO: comando ausente: $1"
    exit 1
  }
}

require_cmd tar
require_cmd docker

list_backups() {
  ls -1 "$BACKUP_DIR"
}

restore_backup() {
  local backup_file="$1"

  if [[ ! -f "$backup_file" ]]; then
    log "ERRO: backup nao encontrado: $backup_file"
    exit 1
  fi

  log "Restaurando backup $backup_file"
  mkdir -p "$AIRFLOW_DIR" "$PYSPARK_DIR"
  tar -xzf "$backup_file" -C "$APP_DIR"

  log "Reiniciando Airflow"
  docker compose -f "$AIRFLOW_COMPOSE_FILE" up -d --build
  log "Rollback concluido"
}

main() {
  case "${1:-}" in
    --list)
      list_backups
      ;;
    --backup)
      [[ $# -eq 2 ]] || {
        usage
        exit 1
      }
      restore_backup "$2"
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
