FROM python:3.8

RUN pip install poetry

WORKDIR /app

COPY poetry.lock pyproject.toml /app/
RUN poetry config virtualenvs.create false
RUN poetry install

COPY web3indexer /app/web3indexer
COPY abi /app/abi
