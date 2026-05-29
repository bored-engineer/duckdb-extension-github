PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=github
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Install system dependencies required for the tidy check (curl dev headers not pre-installed on CI)
install-dev-deps:
ifeq ($(shell uname -s),Linux)
	sudo apt-get install -y -qq libcurl4-openssl-dev 2>/dev/null || true
endif

tidy-check: install-dev-deps
