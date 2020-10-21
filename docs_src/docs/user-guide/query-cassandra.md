# Query Cassandra

**primeight** is designed to abstract the querying language used by Cassandra and to ease the creation of queries programmatically.
It implements a set of methods that can be chained to construct queries (these methods return `#!python self`).

## Methods

- [id](/reference/cassandra-table#id)
- [time](/reference/cassandra-table#time)
- [space](/reference/cassandra-table#space)
- [equals](/reference/cassandra-table#equals)
- [among](/reference/cassandra-table#among)
- [between](/reference/cassandra-table#between)
- [between_including](/reference/cassandra-table#between_including)
- [lower_than](/reference/cassandra-table#lower_than)
- [lower_or_equal_than](/reference/cassandra-table#lower_or_equal_than)
- [higher_than](/reference/cassandra-table#higher_than)
- [higher_or_equal_than](/reference/cassandra-table#higher_or_equal_than)

## Required Columns

The `id`, `time`, and `space` methods identify the required attributes of the query.

When a query have more that one required column, you can not only specify one of them, you need to specify all. 

## Example

```python
from primeight.parser import YamlParser
from primeight import CassandraManager, CassandraKeyspace, CassandraTable

# Load configuration.
config = YamlParser.parse('devices.yaml')

# Connect to Cassandra cluster.
manager = CassandraManager(['127.0.0.1'])
manager.connect()

# Create Keyspace and table.
keyspace = CassandraKeyspace(config, cassandra_manager=manager)
table = CassandraTable(config, cassandra_manager=manager)

# Query table.
rows = \
    table \
    .query('base') \
    .id('659b6222-19fb-416e-9aa5-9df8cf679247') \
    .execute()
n_rows = len(rows)
print(f"Queried {len(rows)} rows")
```
