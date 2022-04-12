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

## Kafka Setup

Download and install **kafka** and **zookeeper**. You can do this using Homebrew via `brew install kafka` and `brew install zookeeper`.

1. Start **zookeeper** via: `zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties`
2. Start **kafka** via: `kafka-server-start /usr/local/etc/kafka/server.properties`
3. To publish some messages, run `poetry run producer`.
4. To consume messages, run `poetry run consumer <topic> <group-id>`.

   i. Run `poetry run consumer fetch-block test-group-id` to fetch blocks

   ii. Run `poetry run consumer process-log test-group-id` to process logs
