#!/usr/bin/env bash
# gosk dev launcher - Git Bash entry. Thin wrapper around dev-up.ps1.
#   ./scripts/dev-up.sh            start everything
#   ./scripts/dev-up.sh -Build     rebuild images before up
#   ./scripts/dev-up.sh -Seed      load test data after services are healthy
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PS1_FILE="$(cygpath -w "$DIR/dev-up.ps1")"

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$PS1_FILE" "$@"
