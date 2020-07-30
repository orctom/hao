clean:
	rm -rf build dist *.egg-info

uninstall:
	pip uninstall hao -y

install:
	pip install .

reinstall:
	make clean && make uninstall && make install

build:
	python setup.py sdist bdist_wheel

upload:
	twine upload -r pypi dist/*

release:
	git tag `cat VERSION`
	git push origin --tags
