#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_NAME = e-commerce-analyzer
PYTHON_VERSION = 3.13
PYTHON_INTERPRETER = python

#################################################################################
# COMMANDS                                                                      #
#################################################################################


## Install Python dependencies
.PHONY: requirements
requirements:
	uv sync
	



## Delete all compiled Python files
.PHONY: clean
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete


## Lint using ruff (use `make format` to do formatting)
.PHONY: lint
lint:
	ruff format --check
	ruff check

## Format source code with ruff
.PHONY: format
format:
	ruff check --fix
	ruff format



## Run tests
.PHONY: test
test:
	python -m pytest tests
## Download Data from storage system
# not set up yet
.PHONY: sync_data_down
sync_data_down:
	az storage blob download-batch -s container-name/data/ \
		-d data/
	

## Upload Data to storage system
# not set up yet
.PHONY: sync_data_up
sync_data_up:
	az storage blob upload-batch -d container-name/data/ \
		-s data/
	



## Set up Python interpreter environment
.PHONY: create_environment
create_environment:
	uv venv --python $(PYTHON_VERSION)
	@echo ">>> New uv virtual environment created. Activate with:"
	@echo ">>> Windows: .\\\\.venv\\\\Scripts\\\\activate"
	@echo ">>> Unix/macOS: source ./.venv/bin/activate"
	


## Make dataset
.PHONY: data
data: requirements
	uv run python -m e_commerce_analyzer/dataset.py

## Build silver/gold
.PHONY: features
features: requirements
	uv run python -m e_commerce_analyzer.features

## Train ML models
.PHONY: train
train: requirements
	uv run python -m e_commerce_analyzer.modeling.train

## Generate reporting figures
.PHONY: plots
plots: requirements
	uv run python -m e_commerce_analyzer.plots

## Run batch inference
.PHONY: predict
predict: requirements
	uv run python -m e_commerce_analyzer.modeling.predict

## Full local pipeline (learning mode)
.PHONY: pipeline_local
pipeline_local: data features train plots predict
	@echo "Local pipeline finished."
	@echo "Next: review PROJECT_GUIDE.md and fill TODO blocks in each phase."


#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys; \
lines = '\n'.join([line for line in sys.stdin]); \
matches = re.findall(r'\n## (.*)\n[\s\S]+?\n([a-zA-Z_-]+):', lines); \
print('Available rules:\n'); \
print('\n'.join(['{:25}{}'.format(*reversed(match)) for match in matches]))
endef
export PRINT_HELP_PYSCRIPT

help:
	@$(PYTHON_INTERPRETER) -c "${PRINT_HELP_PYSCRIPT}" < $(MAKEFILE_LIST)
