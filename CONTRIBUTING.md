# Contributing to Elastic-Datashader

We welcome any contributions!

## Bug Reports

For logging bugs and other issues, please file
an issue on [GitHub](https://github.com/spectriclabs/elastic_datashader/issues).

Try to provide as much detail as possible in the issue,
including, but not limited to
- the environment in which you're running
Elastic-Datashader (e.g., Docker, macOS, RHEL, etc.),
- any error messages,
- the situation in which the error message arose,
- etc.

## Bug Fixes and Other contributions

To help contribute, make sure to set up your local
environment as described in README.md.

We recommend developing on a new branch,
where the branch name is descriptive enough to glean
what is contained in it, e.g., `fix-ellipse-edge-case`
is better than `patch-1`.

For both bug fixes and new features, we strongly
suggest adding a unit test verifying the behavior.

## Running Tests

Once you have added the bug fix/new functionality and
relevant unit tests, follow the testing instructions
described in README.md.

We also encourage you to ensure the Docker image will
still build via `make`.

## Submitting a Pull Request

Before pushing, make sure to run `black` on your files
to lint and format them.

Once you've verified functionality and linted, submit a
pull request through [GitHub](https://github.com/spectriclabs/elastic_datashader/pulls).
