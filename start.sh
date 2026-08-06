#!/bin/bash

PATH="$PATH:$HOME/.local/bin"
poetry install
poetry run python main.py
