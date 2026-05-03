# Helper to load a .env file into the shell environment for testing/run purposes
# Usage: source scripts/load_env.sh spark_streaming/.env

ENV_FILE="$1"
if [ -z "$ENV_FILE" ]; then
  echo "Usage: source scripts/load_env.sh <path-to-env-file>"
  return 1
fi

if [ ! -f "$ENV_FILE" ]; then
  echo "Env file not found: $ENV_FILE"
  return 1
fi

# Export each non-comment non-empty KEY=VALUE
while IFS='=' read -r key val; do
  # trim whitespace
  key="$(echo "$key" | xargs)"
  val="$(echo "$val" | xargs)"
  if [ -z "$key" ]; then
    continue
  fi
  case "$key" in
    \#*) continue ;;
  esac
  # Do not export if already set
  if [ -z "${!key}" ]; then
    export "$key=$val"
  fi
done < <(grep -v '^\s*#' "$ENV_FILE" | grep -v '^\s*$')

echo "Loaded env from $ENV_FILE"
