format:
	isort --profile=black .
	black .

lint:
	black --check .
	isort --check --profile=black .
	flake8

setup:
	dephell deps convert --from=pyproject.toml --to=setup.py
	dephell deps convert --from=pyproject.toml --to=requirements.txt

build_wheels:
	python setup.py sdist
	twine check dist/*
