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

## Commits
### What Makes a Good Commit Message?

In line with best practices and to ensure meaningful and easy-to-review commits, we're implementing strict rules for commit messages and branch names.

#### Branch Naming Convention

Branch names will adhere to the following format: 

type-of-branch/issue-number-meaningful-short-description

- `type-of-branch` is one of the fixed words from the table provided.
- The branch name format must not exceed 150 characters, including the `remotes/origin/` string.

#### Commit Message Format

All commit messages will follow this structure:

type-of-commit: meaningful short description

- `type-of-commit` is one of the fixed words from the table provided.
- The commit message must not exceed 100 characters.

#### Commit Message Guidelines

When crafting commit messages, follow these guidelines:

- Do not end the subject line with a period.
- Use the imperative present in the subject line.

#### Commit Type Definitions

- **build**: Changes that affect the build system or external dependencies (example scopes: pip, cmake)
- **ci**: Changes to our CI configuration files and scripts (example scopes: GitLab Actions)
- **docs**: Documentation-only changes
- **feat**: A new feature
- **fix**: A bug fix
- **perf**: A code change that improves performance
- **refactor**: A code change that neither fixes a bug nor adds a feature
- **style**: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- **test**: Changes of tests that are missing or need to be corrected

We maintain consistency and clarity in our commits for our development process.
