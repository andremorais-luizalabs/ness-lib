.PHONY: clean build
.DEFAULT_GOAL := build

clean:
	rm -rf build
	rm -rf dist
	rm -rf *.egg-info

build: clean
	# python setup.py build
	# python setup.py sdist
	# python setup.py bdist_egg
	python setup.py bdist_wheel

# deploy: clean build
	# cp ./dist/superness-0.0.1-py2-none-any.whl gs://prd-cluster-config/wheels/superness-0.0.1-py2-none-any.whl
