# fio.etl

This is a small service that uses a websocket to consume the output of
[eos-chronicle](https://github.com/EOSChronicleProject/eos-chronicle) and output into a Kafka cluster, publishing each
message type into a different topic. This allows pulling data into various backends for analysis.

The Kafka cluster is expected to have the following topics:

 * block
 * tx
 * abi
 * table
 * permission
 * acount
 * test (optional, only used by go test)

