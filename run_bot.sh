#!/bin/zsh
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PYTHON="$PROJECT_DIR/.venv/bin/python"
ENV_FILE="$PROJECT_DIR/.env"

cd "$PROJECT_DIR"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

if [[ -z "${BOT_TOKEN:-}" ]]; then
  echo "BOT_TOKEN не задан. Добавьте его в $ENV_FILE"
  exit 1
fi

exec "$VENV_PYTHON" "$PROJECT_DIR/bot.py"
