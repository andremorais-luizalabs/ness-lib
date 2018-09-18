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


deploy: build
    gsutil -m cp -r sness/ gs://us-east1-ness-maestro-782d5135-bucket/dags/

