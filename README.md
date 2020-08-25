# fio.etl

**This project is in the early phases of development, is intended for use for getting general statistical information
about the FIO chain, and should not be considered a valid source-of-truth for wallets or exchange accounts**

This is a small group of services that uses a websocket to consume the output of
[eos-chronicle](https://github.com/EOSChronicleProject/eos-chronicle) and output into an elasticsearch cluster,
publishing each message type into a different index, though it's not strictly necessary to use elasticsearch, logstash
can output to many types of data stores.

It attempts to re-use as many existing open-source tools as possible to minimize the complexity.

- chronicle sends state-history-plugin output as json to a websocket
- fioetl listens on a websocket, and pushes data into durable rabbitmq exchanges
- logstash consumes the data and pushes into elasticsearch
- this project does not include any presentation layer, or lookup APIs, it is solely focused on populating a data store

### fioetl

fioetl is the program handling normalization of data from chronicle that logstash is not capable of transforming, such as handling
casts in deeply nested structs, calculating the block id, and splitting some types of records out for storage in different
indexes. This is the only truly custom part of this project, weighing in under 5,000 loc.


## Data flow

```

  state-history-plugin <--- chronicle ---> fioetl ---> rabbitmq ---> logstash ---> elasticsearch
         external           [ .............. docker containers ............ ]        external

```

This project contains a docker-compose file that handles building and running the etl portion, but not the FIO node or elasticsearch.

## Running

* needs access to a node running the state-history-plugin, it is not recommended to run an elasticsearch server and nodeos on the same system because of memory locking
* needs access to an elasticsearch cluster
* copy `example-docker-compose.yml` to `docker-compose.yml` and edit.
  - change "chronicle.environment.HOST" to the URL for the state-history-plugin
  - change "fioetl.environment.HOST" to a nodeos chain api endpoint (this is only used if deriving block ID fails for some reason)
* copy `logstash/example-logstash.conf` to `logstash/logstash.conf` and update the outputs section with information for the elasticsearch cluster
* build the containers `docker-compose build` -- note: this takes a _very_ long time
* run `docker-compose up -d` and indexing should begin

The ingest process is not very optimized yet, only achieving around two to four thousand blocks per-second. Future
plans may involve allowing for concurrent indexers if needed. Presently takes about two days to index the FIO mainnet.

## Default indices:

Each index is split by month. No effort is made by this tool to define sharding or replication settings. See [examples here](doc/data/)

- `[logstash-abi-]YYYY.MM`: contains ABI changes
- `[logstash-acc_metadata-]YYYY.MM`: account metadata updates
- `[logstash-block-]YYYY.MM`: blocks, transactions are not unpacked
- `[logstash-permission-]YYYY.MM`: account permission changes
- `[logstash-permission_link-]YYYY.MM`: linked permission changes
- `[logstash-schedule-]YYYY.MM`: schedule updates, extracted from blocks to make searching efficient
- `[logstash-table_row-]YYYY.MM`: table row updates, contains many millions of records
- `[logstash-trace-]YYYY.MM`: action traces
- `[logstash-transfer-]YYYY.MM`: transfers (not trnsfiopubky actions). These are split out of traces to make it easier to find fees and reward payouts.


