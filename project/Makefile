MODEL_NAME=kaggle-otto-serving

serve-uvicorn:
	python -m app.uvicorn

.PHONY: setup
setup:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

.PHONY: test-cov
test-cov:
	PYTHONPATH=src:$(PYTHONPATH) pytest -s -vv --cov=app --cov-fail-under=85 --cov-report term-missing --cov-config=.coveragerc --disable-pytest-warnings

.PHONY: format-linting
format-linting:
	black --color --line-length 88 -v app/ tests/

.PHONY: check-linting
check-linting:
	black --check -v --diff --quiet app/

.PHONY: check-non-linting
check-non-linting:
	flake8 --max-line-length 119 app/

run-test: check-linting check-non-linting test-cov check-static-analysis

localbuild:
	docker build -t ${MODEL_NAME} .

localrun: localbuild
	docker run --rm -it -p 5000:80 \
		-v $(shell pwd)/secrets:/serving/secrets \
		-v ~/.config/gcloud/:/creds \
		--env GOOGLE_APPLICATION_CREDENTIALS=/creds/application_default_credentials.json \
		--env-file .envrc \
		$(MODEL_NAME)
