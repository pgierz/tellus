# Minimal makefile for Sphinx documentation with Jupyter Book
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD  ?= sphinx-build
SOURCEDIR    = .
BUILDDIR     = _build

# Default target
.PHONY: help
help:
	@echo "Tellus Documentation Build Commands"
	@echo ""
	@echo "Available targets:"
	@echo "  build     Build the documentation using Jupyter Book"
	@echo "  clean     Clean the build directory"
	@echo "  serve     Serve the documentation locally"
	@echo "  install   Install documentation dependencies"
	@echo ""
	@echo "Using Jupyter Book commands:"
	@echo "  jupyter-book build docs"
	@echo "  jupyter-book clean docs"

.PHONY: build
build:
	jupyter-book build .

.PHONY: clean
clean:
	jupyter-book clean .

.PHONY: serve
serve: build
	@echo "Serving documentation at http://localhost:8000"
	@cd _build/html && python -m http.server 8000

.PHONY: install
install:
	pip install -r requirements.txt

# Traditional Sphinx targets for compatibility
.PHONY: html
html: build

.PHONY: livehtml
livehtml:
	sphinx-autobuild $(SOURCEDIR) $(BUILDDIR)/html $(SPHINXOPTS)

# Catch-all target: route all unknown targets to Sphinx
%: Makefile
	@echo "Unknown target: $@"
	@echo "Run 'make help' for available targets."