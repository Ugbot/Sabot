# Makefile for SabotQL Integration Examples
#
# Quick commands for building and testing SabotQL integration

.PHONY: help build install test benchmark examples clean

help:
	@echo "SabotQL Integration - Makefile"
	@echo ""
	@echo "Commands:"
	@echo "  make build      - Build C++ library and Python bindings"
	@echo "  make install    - Install Python package"
	@echo "  make test       - Run integration tests"
	@echo "  make benchmark  - Run performance benchmarks"
	@echo "  make examples   - Run all examples"
	@echo "  make quickstart - Run quickstart example"
	@echo "  make clean      - Clean build artifacts"
	@echo ""

build:
	@echo "Building SabotQL C++ library..."
	cd ../../sabot_ql/bindings/python && ./build.sh

install:
	@echo "Installing Python package..."
	cd ../../sabot_ql/bindings/python && pip install -e .

test:
	@echo "Running integration tests..."
	python test_triple_enrichment.py

benchmark:
	@echo "Running benchmarks..."
	python benchmark_triple_lookup.py

quickstart:
	@echo "Running quickstart..."
	python quickstart.py

examples: quickstart
	@echo "Running example 1..."
	python example1_company_enrichment.py &
	sleep 5
	@echo "Running example 2..."
	python example2_fraud_detection.py &
	sleep 5
	@echo "Running example 3..."
	python example3_recommendation.py &

clean:
	@echo "Cleaning build artifacts..."
	rm -rf ../../sabot_ql/build/*
	rm -rf ../../sabot_ql/bindings/python/build/
	rm -rf ../../sabot_ql/bindings/python/*.so
	rm -rf ../../sabot_ql/bindings/python/sabot_ql.egg-info/
	rm -rf ./*.db
	rm -rf ./quickstart_kg.db
	@echo "✅ Clean complete"


