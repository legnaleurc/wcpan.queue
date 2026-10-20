RM := rm -rf
PYTHON := uv run python3
RUFF := uv run ruff

PKG_FILES := pyproject.toml
PKG_LOCK := uv.lock
ENV_DIR := .venv
ENV_LOCK := $(ENV_DIR)/pyvenv.cfg

.PHONY: all format lint clean purge test build publish venv

all: venv

format: venv
	$(RUFF) format tests src/wcpan
	$(RUFF) check --fix tests src/wcpan

lint: venv
	$(RUFF) format --check tests src/wcpan
	$(RUFF) check tests src/wcpan

clean:
	$(RM) ./dist ./build ./*.egg-info

purge: clean
	$(RM) $(ENV_DIR)

test: venv
	$(PYTHON) -m compileall src/wcpan
	$(PYTHON) -m unittest

build: clean venv
	uv build

publish: venv
	uv publish

venv: $(ENV_LOCK)

$(ENV_LOCK): $(PKG_LOCK)
	uv sync
	touch $@

$(PKG_LOCK): $(PKG_FILES)
	uv lock
	touch $@
