# Managing Materialized Views

!!! warning
    Materialized Views are experimental and are not recommended for production use.

**primeight** allows the management of Cassandra materialized views, 
namely, the __creation__ and __drop__ of materialized views.

**primeight** tries to make these tasks the easiest experience possible.

## Creating materialized views

```python
from primeight import CassandraManager, CassandraMaterializedView
from primeight.parser import YamlParser

config = YamlParser.parse("devices.yaml")

manager = CassandraManager(["127.0.0.1"])
manager.connect()

view = CassandraMaterializedView(config, 'devices_by_time', cassandra_manager=manager)
view.create().execute()
```

or using a __dynamic keyspace name__:

```python
from primeight import CassandraManager, CassandraMaterializedView
from primeight.parser import YamlParser

config = YamlParser.parse("devices.yaml")

manager = CassandraManager(["127.0.0.1"])
manager.connect()

view = CassandraMaterializedView(config, 'devices_by_time', cassandra_manager=manager)

client_name = "Client ABC"
view.create(keyspace=client_name.replace(' ', '-')).execute()
```

You can read more on the parameters of the `create` method [here](/reference/cassandra-materialized-view/#create).

## Dropping materialized views

**primeight** tries to make the creation of materialized views the easiest experience possible.

```python
from primeight import CassandraManager, CassandraMaterializedView
from primeight.parser import YamlParser

config = YamlParser.parse("devices.yaml")

manager = CassandraManager(["127.0.0.1"])
manager.connect()

view = CassandraMaterializedView(config, 'devices_by_time', cassandra_manager=manager)
view.drop().execute()
```

You can read more on the parameters of the `drop` method [here](/reference/cassandra-materialized-view/#drop).
