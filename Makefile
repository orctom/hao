deps:
	poetry install --no-root -vvv

clean:
	rm -rf build dist *.egg-info

build:
	make clean
	poetry build

publish:
	poetry publish -r pypi

release:
	git tag `poetry version -s`
	git push origin `poetry version -s`

release-again:
	git tag -d `poetry version -s`
	git push -d origin `poetry version -s`
	git tag `poetry version -s`
	git push origin `poetry version -s`
