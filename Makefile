
help:
	@grep -B1 -E "^[a-zA-Z0-9_-]+\:([^\=]|$$)" Makefile \
	 | grep -v -- -- \
	 | sed 'N;s/\n/###/' \
	 | sed -n 's/^#: \(.*\)###\(.*\):.*/\2###\1/p' \
	 | column -t  -s '###'

#: install python dependencies
deps:
	pi install

#: remove build dists
clean:
	rm -rf build dist *.egg-info

#: create git tag with the version number in poetry
release:
	git tag `pi version -s`
	git push origin `pi version -s`

#: re-create git tag with the version number in poetry
release-again:
	git tag -d `pi version -s`
	git push -d origin `pi version -s`
	git tag `pi version -s`
	git push origin `pi version -s`

#: build dists
build:
	make clean
	pi build

#: publish to pypi
publish:
	pi publish
