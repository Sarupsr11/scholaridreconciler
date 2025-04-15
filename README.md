# Knowledge_graph
This project is based on working with knowledge graph for the reconciliation of scholar identity. Given a set of scholars details, we retrieve the corresponding wikidata id with a confidence score.


# ScholarIDReconciler



## Getting started

* install all dependencies with `pip install .[test]`
* to format anf lint the code use `ruff check` and `ruff format`
* for testing use `tox` or to only test specific environments e.g. `tox -e py310`

## RESTful Endpoint

The RESTful endpoint can be either started with 

```shell
$ scholar-id-reconciler start
```
after the installation of the project. Or for development purposes with the following command:

```shell
$ fastapi dev ./src/scholaridreconciler/endpoint.py
```
