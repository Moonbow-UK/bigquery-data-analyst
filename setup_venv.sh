#!/usr/bin/env bash
# Helper to create a Python venv and install dependencies
set -euo pipefail

VENV_DIR=".venv"
PYTHON=${PYTHON:-python3}

if [ -d "$VENV_DIR" ]; then
  echo "Virtualenv $VENV_DIR already exists. Activate it with: source $VENV_DIR/bin/activate"
  exit 0
fi

echo "Creating virtualenv in $VENV_DIR using $PYTHON"
$PYTHON -m venv "$VENV_DIR"
echo "Activating venv and upgrading pip"
source "$VENV_DIR/bin/activate"
python -m pip install --upgrade pip
if [ -f requirements.txt ]; then
  echo "Installing from requirements.txt"
  pip install -r requirements.txt
else
  echo "No requirements.txt found."
fi

echo "Setup complete. Activate with: source $VENV_DIR/bin/activate"
