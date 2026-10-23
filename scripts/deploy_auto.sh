#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_NAME="Datalakehouse Ambipar"
APP_DIR="/opt/apps/datalake"
AIRFLOW_DIR="$APP_DIR/airflow-pipeline"
PYSPARK_DIR="$APP_DIR/pyspark-library"
SCRIPTS_DIR="$APP_DIR/scripts"

DEPLOY_BASE="$APP_DIR/.deploy"
MIRROR_DIR="$DEPLOY_BASE/repo.git"
STAGE_DIR="$DEPLOY_BASE/stage"
BACKUP_DIR="$DEPLOY_BASE/backups"
ENV_FILE="$DEPLOY_BASE/deploy.env"
STATE_FILE="$DEPLOY_BASE/last_deployed_main.sha"
CURRENT_FILE="$DEPLOY_BASE/current.sha"
PREVIOUS_FILE="$DEPLOY_BASE/previous.sha"
LOCK_FILE="$DEPLOY_BASE/deploy.lock"
LOG_FILE="$DEPLOY_BASE/deploy.log"
SSH_KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/id_ed25519_github_datalakehouse}"
BACKUP_RETENTION="${BACKUP_RETENTION:-5}"

BRANCH="main"
REMOTE_NAME="origin"
AIRFLOW_COMPOSE_FILE="$AIRFLOW_DIR/docker-compose.yaml"
AIRFLOW_SECRET_ENV_FILE="/opt/apps/secrets/airflow.env"

if [[ -f "$AIRFLOW_SECRET_ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$AIRFLOW_SECRET_ENV_FILE"
  set +a
fi

mkdir -p "$DEPLOY_BASE" "$STAGE_DIR" "$BACKUP_DIR" "$SCRIPTS_DIR"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

REPO_URL="${REPO_URL:-}"

exec 9>"$LOCK_FILE"
flock -n 9 || exit 0

log() {
  printf '%s %s\n' "$(date '+%F %T')" "$*" | tee -a "$LOG_FILE"
}

on_error() {
  local exit_code="$1"
  local line_no="$2"
  log "ERRO: deploy interrompido na linha $line_no (exit_code=$exit_code)"
  exit "$exit_code"
}

trap 'on_error $? $LINENO' ERR

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    log "ERRO: comando ausente: $1"
    exit 1
  }
}

require_cmd git
require_cmd rsync
require_cmd tar
require_cmd flock
require_cmd docker

if [[ -z "$REPO_URL" ]]; then
  log "ERRO: REPO_URL nao definido. Configure em $ENV_FILE"
  exit 1
fi

if [[ ! -f "$SSH_KEY_FILE" ]]; then
  log "ERRO: chave SSH nao encontrada em $SSH_KEY_FILE"
  exit 1
fi

export GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=yes -i $SSH_KEY_FILE"

init_mirror() {
  if [[ ! -d "$MIRROR_DIR" ]]; then
    log "Inicializando mirror Git em $MIRROR_DIR"
    git clone --mirror "$REPO_URL" "$MIRROR_DIR"
  fi
}

fetch_remote() {
  git --git-dir="$MIRROR_DIR" remote set-url "$REMOTE_NAME" "$REPO_URL"
  git --git-dir="$MIRROR_DIR" fetch --prune "$REMOTE_NAME"
}

get_target_sha() {
  if git --git-dir="$MIRROR_DIR" show-ref --verify --quiet "refs/heads/$BRANCH"; then
    git --git-dir="$MIRROR_DIR" rev-parse "refs/heads/$BRANCH"
  else
    git --git-dir="$MIRROR_DIR" rev-parse "$REMOTE_NAME/$BRANCH"
  fi
}

get_last_sha() {
  if [[ -f "$STATE_FILE" ]]; then
    cat "$STATE_FILE"
  else
    echo ""
  fi
}

list_changed_files() {
  local from_sha="$1"
  local to_sha="$2"

  if [[ -z "$from_sha" ]]; then
    git --git-dir="$MIRROR_DIR" ls-tree -r --name-only "$to_sha" -- airflow-pipeline pyspark-library
  else
    git --git-dir="$MIRROR_DIR" diff --name-only "$from_sha" "$to_sha" -- airflow-pipeline pyspark-library
  fi
}

backup_live_dirs() {
  local ref="$1"
  local existing_backup
  local backup_file="$BACKUP_DIR/backup_${ref}_$(date '+%Y%m%d_%H%M%S').tar.gz"

  existing_backup="$(find "$BACKUP_DIR" -maxdepth 1 -type f -name "backup_${ref}_*.tar.gz" | sort | tail -n 1 || true)"
  if [[ -n "$existing_backup" ]]; then
    log "Backup existente para o commit $ref: $existing_backup"
    return 0
  fi

  tar -czf "$backup_file" \
    -C "$APP_DIR" \
    airflow-pipeline \
    pyspark-library

  log "Backup criado: $backup_file"
}

cleanup_old_backups() {
  local old_backups

  if ! [[ "$BACKUP_RETENTION" =~ ^[0-9]+$ ]] || [[ "$BACKUP_RETENTION" -lt 1 ]]; then
    log "BACKUP_RETENTION invalido ($BACKUP_RETENTION). Mantendo 5 backups."
    BACKUP_RETENTION=5
  fi

  old_backups="$(find "$BACKUP_DIR" -maxdepth 1 -type f -name 'backup_*.tar.gz' -printf '%T@ %p\n' \
    | sort -nr \
    | awk -v keep="$BACKUP_RETENTION" 'NR > keep { sub(/^[^ ]+ /, ""); print }')"

  if [[ -z "$old_backups" ]]; then
    return 0
  fi

  while IFS= read -r backup_path; do
    [[ -z "$backup_path" ]] && continue
    rm -f "$backup_path"
    log "Backup removido por retencao: $backup_path"
  done <<< "$old_backups"
}

stage_path_from_git() {
  local sha="$1"
  local topdir="$2"

  rm -rf "$STAGE_DIR/$topdir"
  mkdir -p "$STAGE_DIR/$topdir"

  git --git-dir="$MIRROR_DIR" archive "$sha" "$topdir" | tar -x -C "$STAGE_DIR"
}

sync_dir() {
  local src="$1"
  local dst="$2"
  shift 2

  mkdir -p "$dst"
  rsync -a --delete "$@" "$src/" "$dst/"
}

restart_airflow() {
  log "Reiniciando Airflow"
  docker compose -f "$AIRFLOW_COMPOSE_FILE" up -d --build >>"$LOG_FILE" 2>&1
}

persist_state() {
  local last_sha="$1"
  local target_sha="$2"

  if [[ -n "$last_sha" ]]; then
    echo "$last_sha" > "$PREVIOUS_FILE"
  fi
  echo "$target_sha" > "$CURRENT_FILE"
  echo "$target_sha" > "$STATE_FILE"
}

main() {
  log "==== Inicio deploy automatico: $PROJECT_NAME ===="

  init_mirror
  fetch_remote

  local last_sha target_sha changed_files changed_count
  last_sha="$(get_last_sha)"
  target_sha="$(get_target_sha)"

  if [[ -n "$last_sha" && "$last_sha" == "$target_sha" ]]; then
    log "Sem novo commit em $BRANCH"
    exit 0
  fi

  changed_files="$(list_changed_files "$last_sha" "$target_sha" || true)"
  changed_count="$(printf '%s\n' "$changed_files" | sed '/^$/d' | wc -l)"

  if [[ "$changed_count" -eq 0 ]]; then
    log "Novo commit detectado, mas sem mudancas em airflow-pipeline/ ou pyspark-library/"
    persist_state "$last_sha" "$target_sha"
    exit 0
  fi

  log "Arquivos alterados:"
  printf '%s\n' "$changed_files" | tee -a "$LOG_FILE"

  backup_live_dirs "$target_sha"

  local restart_needed="false"

  if printf '%s\n' "$changed_files" | grep -q '^airflow-pipeline/'; then
    log "Sincronizando airflow-pipeline/"
    stage_path_from_git "$target_sha" "airflow-pipeline"
    sync_dir "$STAGE_DIR/airflow-pipeline" "$AIRFLOW_DIR" \
      --exclude 'logs/' \
      --exclude '__pycache__/' \
      --exclude '*.pyc'
    restart_needed="true"
  fi

  if printf '%s\n' "$changed_files" | grep -q '^pyspark-library/'; then
    log "Sincronizando pyspark-library/"
    stage_path_from_git "$target_sha" "pyspark-library"
    sync_dir "$STAGE_DIR/pyspark-library" "$PYSPARK_DIR"
    restart_needed="true"
  fi

  if [[ "$restart_needed" == "true" ]]; then
    restart_airflow
  else
    log "Sem necessidade de reiniciar Airflow"
  fi

  persist_state "$last_sha" "$target_sha"
  cleanup_old_backups
  log "Deploy concluido no commit $target_sha"
}

main "$@"
