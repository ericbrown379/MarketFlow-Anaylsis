#!/usr/bin/env bash
set -euo pipefail

# Downloads PostgreSQL JDBC driver into spark_streaming/ directory.
# Usage: ./scripts/download_postgres_jdbc.sh [version]
# Example: ./scripts/download_postgres_jdbc.sh 42.7.11

VERSION=${1:-42.7.11}
TARGET_DIR="$(pwd)/spark_streaming"
mkdir -p "$TARGET_DIR"
FILENAME="postgresql-${VERSION}.jar"
URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/${VERSION}/${FILENAME}"

echo "Downloading ${FILENAME} to ${TARGET_DIR}..."

if command -v curl >/dev/null 2>&1; then
  curl -fSL "$URL" -o "$TARGET_DIR/$FILENAME"
elif command -v wget >/dev/null 2>&1; then
  wget -O "$TARGET_DIR/$FILENAME" "$URL"
else
  echo "Error: neither curl nor wget found on PATH." >&2
  exit 2
fi

echo "Downloaded $TARGET_DIR/$FILENAME"
