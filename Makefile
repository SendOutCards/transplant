env:
	virtualenv -p python3 env

dev: env
	./env/bin/pip install -e .
	./env/bin/pip install -r requirements-dev.txt ipython

test: dev
	./env/bin/mypy transplant/
	./env/bin/black transplant/ --check

black: dev
	./env/bin/black transplant/

deploy: test
	./env/bin/python setup.py sdist bdist_wheel upload -r soc