# Simple blockchain indexer

This is very much a prototype.

To start local mongo DB
```
docker-compose up -d
```

To use
```
export MONGODB_URI='mongodb://web3indexer:password@localhost:27017'
export ENDPOINT_URL='http://endpoint/rpc'
poetry install
poetry run index
```
