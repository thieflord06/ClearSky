@pip-install *args:
    echo "🐍 Installing Python packages"
    pip install -q -r requirements.txt -r requirements-dev.txt

@pip-update:
    pip-compile requirements.in
    pip-compile requirements-dev.in
    just pip-install

first-time-setup:
    #!/usr/bin/env bash

    set -euo pipefail

    echo "🌎 Setting up environment"
    [[ -e ".env" ]] || ln -s .env.local .env
    [[ -n "$DIRENV_DIR" ]] || direnv allow
    [[ -e "venv" ]] || ln -s .direnv/python3.12 venv
    just pip-install -q
    echo "🪝 Installing pre-commit hooks"
    pre-commit install &> /dev/null
