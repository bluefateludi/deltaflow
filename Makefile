.PHONY: install test compile check demo clean

PYTHON ?= python3
PIP ?= $(PYTHON) -m pip

install:
	$(PIP) install .

test: install
	$(PYTHON) -m unittest discover -s tests -v

compile:
	$(PYTHON) -m compileall -q src tests

check: compile test

demo: install
	$(PYTHON) -m qsync init-demo demo-source.db --count 1000
	$(PYTHON) -m qsync sync demo-source.db demo-target.db --batch-size 500
	$(PYTHON) -m qsync status demo-target.db

clean:
	rm -f demo-source.db demo-source.db-shm demo-source.db-wal
	rm -f demo-target.db demo-target.db-shm demo-target.db-wal
