format:
	isort -y
	black .

setup:
	dephell deps convert --from=pyproject.toml --to=setup.py
	dephell deps convert --from=pyproject.toml --to=requirements.txt

build_wheels:
	python setup.py sdist
	twine check dist/*
