# Cassandra client for tornado

Made as simple and straightforward as possible.
This implementation only supports cql v3 and simple queries, yet provide some important features:

* Fail-over behavior on the client side.
* Automatic retry in case of downed hosts.
