.PHONY: tests

# Clean
clean:
	@find . -name '__pycache__' | xargs rm -fr {} \;
	@find . -name '.coverage' | xargs rm -fr {} \;
	@find . -name 'build' | xargs rm -fr {} \;
	@find . -name 'dist' | xargs rm -fr {} \;
	@find . -name '.pytest_cache' | xargs rm -fr {} \;
	@find . -type d -name '*.egg-info' | xargs rm -fr {} \;
	@find . -type d -name '*.eggs' | xargs rm -fr {} \;

# Install
install:
	@pip install .
dev_install:
	@pip install -e .

# Tests
qa_check_code:
	@find ./src -name "*.py" | xargs flake8 --exclude *.eggs*
unittest:
	@pytest -v --cov=src --cov-report term-missing src/**/tests/unit
typetest:
	@find ./src -name "*.py" | xargs mypy
tests:
	@make qa_check_code
	@make unittest
	@make typetest
	@make clean

# Deploy to PyPi
build:
	@make clean
	@python3 -m pip install --upgrade build
	@python3 -m pip install --upgrade twine
	@python3 -m build
build_and_deploy_to_pypi:
	@make build
	@python3 -m twine upload --verbose --repository pypi dist/*
