# migration-testing-tool

Data Driven Projects: Migration Testing Tool

## development

### setup virtual env

```bash

python3 -m venv .venv

source .venv/bin/activate
```

### install dependencies

This required to activate virtual env first.

```bash
pip3 install --upgrade datadriven-core@git+https://ro:rW-cvXTECzHxFYZLKvxM@git.matador.ais.co.th/cronus/data-driven/data-driven-lib.git@0.5.3
```

> Note: current version to use is `0.5.3` (required python 3.11)

## extras

- this project already convert all files ending to `CRLF` with this command `find . -type d -name .git -prune -o -type f -print0 | xargs -0 unix2dos`
