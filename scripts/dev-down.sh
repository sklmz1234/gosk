#!/usr/bin/env bash
# gosk dev stopper - Git Bash entry. Thin wrapper around dev-down.ps1.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PS1_FILE="$(cygpath -w "$DIR/dev-down.ps1")"

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$PS1_FILE" "$@"
