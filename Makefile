# iiimets installation makefile

help:
	@echo "Installing iiimets:"
	@echo
	@echo "  Usage:"
	@echo "  make [OPTIONS] TARGET"
	@echo
	@echo "  Targets:"
	@echo "  * help        (this message)"
	@echo "  * deps-ubuntu (install system dependencies)"
	@echo "  * deps        (download and unpack Saxon library)"
	@echo "  * install     (run pip install)"
	@echo "  * uninstall   (run pip uninstall)"
	@echo "  * dist        (build sdist and bdist pkgs)"
	@echo "  * pypi        (publish pkg)"

.PHONY: help

deps-ubuntu:
	apt-get -y install wget curl unzip openjdk-8-jre-headless

SAXON = https://sourceforge.net/projects/saxon/files/Saxon-HE/10/Java/SaxonHE10-6J.zip
SAXONZIP = $(notdir $(SAXON))
SAXONLIB = saxon-he-10.6.jar
$(SAXONLIB): $(SAXONZIP)
	unzip -n $< $(SAXONLIB) -d iiimets/res
$(SAXONZIP):
	wget -O $@ $(SAXON) || curl -L -o $@ $(SAXON)

deps: $(SAXONLIB)

install: | $(SAXONLIB)
	pip install iiimets

uninstall:
	pip uninstall iiimets

dist: deps always
	pip install wheel
	python setup.py sdist bdist_wheel --universal

pypi: deps always
	pip install twine
	twine check dist dist/*
	twine upload --repository-url https://upload.pypi.org/legacy/ dist/*

.PHONY: deps-ubuntu deps install uninstall dist pypi always

# do not search for implicit rules here:
Makefile: ;
