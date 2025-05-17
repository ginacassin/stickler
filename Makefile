.PHONY: code-check run-tests

code-check:
	isort .
	black .
	pylint src tests

run-tests:
	pytest