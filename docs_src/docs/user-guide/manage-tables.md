# Managing Tables

**primeight** allows the management of Cassandra tables, 
namely, the __creation__ and __drop__ of tables.

**primeight** tries to make these tasks the easiest experience possible.

## Creating tables

```python
from primeight import CassandraManager, CassandraTable
from primeight.parser import YamlParser

config = YamlParser.parse("devices.yaml")

manager = CassandraManager(["127.0.0.1"])
manager.connect()

table = CassandraTable(config, cassandra_manager=manager)
table.create().execute()
```

or using a __dynamic keyspace name__:

```python
from primeight import CassandraManager, CassandraTable
from primeight.parser import YamlParser

config = YamlParser.parse("devices.yaml")

manager = CassandraManager(["127.0.0.1"])
manager.connect()

table = CassandraTable(config, cassandra_manager=manager)

client_name = "Client ABC"
table.create(keyspace=client_name.replace(' ', '-')).execute()
```

You can read more on the parameters of the `create` method [here](/reference/cassandra-table/#create).

## Dropping tables

**primeight** tries to make the creation of tables the easiest experience possible.

!!! warning
    You cannot drop a table without first dropping all of its materialized views.
    For that reason the `drop` method includes a `drop_materialized_views` parameter that allows you to drop everything at once.

```python
from primeight import CassandraManager, CassandraTable
from primeight.parser import YamlParser

config = YamlParser.parse("devices.yaml")

manager = CassandraManager(["127.0.0.1"])
manager.connect()

table = CassandraTable(config, cassandra_manager=manager)
table.drop().execute()
```

You can read more on the parameters of the `drop` method [here](/reference/cassandra-table/#drop).