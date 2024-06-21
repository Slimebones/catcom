test target="" show="all" *flags="":
	poetry run coverage run -m pytest -x --ignore=tests/app -p no:warnings --show-capture={{show}} --failed-first {{flags}} tests/{{target}}

lint target="" *flags="":
	poetry run ruff {{flags}} {{target}}

check: lint test
