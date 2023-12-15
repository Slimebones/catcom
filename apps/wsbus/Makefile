export show=all
export show=all
export t
export m
export args
export host=localhost
export port=3000

test:
	ORWYNN_MODE=test poetry run coverage run -m pytest -m "not off" -x --ignore=tests/app --ignore=src/insert -p no:warnings --show-capture=$(show) --failed-first $(args) src

test.target:
	ORWYNN_MODE=test poetry run coverage run -m pytest -m "not off" -x --ignore=tests/app -p no:warnings --show-capture=$(show) --failed-first $(args) src/$(t)

lint:
	poetry run ruff .

check: lint test

coverage:
	poetry run coverage report -m

coverage.html:
	poetry run coverage html --show-contexts && python -m http.server -d htmlcov 8000
