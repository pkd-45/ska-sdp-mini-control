.PHONY: install test probe benchmark

install:
	python -m pip install -e '.[dev]'

test:
	PYTHONPATH=src pytest -q

probe:
	bash scripts/manual_docker_probe.sh

benchmark:
	PYTHONPATH=src python scripts/benchmark_tick_history.py
