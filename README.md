# JDBC streaming source connector for Apache Flink

This connector for Apache Flink provides a streaming JDBC source.
The connector implements a source function for Flink that queries the database on a regular interval and pushes all the results to the output stream.

Features:

- Accept custom JDBC connection parameters and custom SQL SELECT query to be executed

- Extendable row parsers

- Implemented in Java, but with additional Python binding for the source function

## Usage

For JVM version see:
[Java](flink-java/README.md) docs.
For PyFlink version see [Python](flink-python/README.md) docs.

## Compatibility Matrix

The main branch will always have the most recent supported versions of Flink.

| Connector Version | Flink Version | Status            |
|-------------------|---------------|-------------------|
| 0.1               | 1.15          | Under Development |
