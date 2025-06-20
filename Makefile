.PHONY:	black isort flake8 build 

DIR=mcstools/

isort:
	isort $(DIR)

black:
	black $(DIR)

flake8:
	flake8 $(DIR)

lint:	isort black flake8

test-isort:
	isort $(DIR) -c

test-black:
	black $(DIR) --check

test-flake8:
	flake8 $(DIR)

test-pytest:
	pytest $(DIR)

test:	test-isort test-black test-flake8 test-pytest