clean:
	rm -rf build dist *.egg-info

uninstall:
	pip uninstall hao -y

install:
	make clean
	make uninstall
	pip install .
	make clean

build:
	make clean
	python setup.py sdist bdist_wheel

upload:
	twine upload -r pypi dist/*

release:
	git tag `cat VERSION`
	git push origin --tags
