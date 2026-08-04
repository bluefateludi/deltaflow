.PHONY: test demo clean

test:
	PYTHONPATH=src python3 -m unittest discover -s tests -v

demo:
	PYTHONPATH=src python3 -m qsync generate demo.jsonl --count 100000
	PYTHONPATH=src python3 -m qsync sync demo.jsonl demo.db --batch-size 5000

clean:
	rm -f demo.jsonl demo.db demo.db-shm demo.db-wal

