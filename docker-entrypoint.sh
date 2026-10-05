#!/bin/sh
set -e

DATA_DIR="${NIODB_DATA:-/data}"
mkdir -p "$DATA_DIR"

AUTH_FILE="$DATA_DIR/auth.json"

# Auto-initialize authentication credentials if missing
if [ ! -f "$AUTH_FILE" ]; then
    echo "=========================================================="
    echo "  Welcome to NioDB! Initializing authentication..."
    echo "=========================================================="
    niodb init-auth --data "$DATA_DIR" --name "admin"
    echo ""
    echo "  Credentials saved to $AUTH_FILE."
    echo "  Save your API keys above securely!"
    echo "=========================================================="
fi

echo "==> Starting NioDB on 0.0.0.0:${PORT:-7432}..."
exec niodb --listen "0.0.0.0:${PORT:-7432}" --data "$DATA_DIR" "$@"
