.PHONY: install run test lint clean

install:
	poetry install

run:
	poetry run uvicorn deltalink.main:app --reload

test:
	poetry run pytest

lint:
	poetry run flake8 app

clean:
	poetry cache clear pypi --all
