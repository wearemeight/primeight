# Managing Keyspaces

**primeight** allows for some management tasks of Cassandra keyspaces, 
namely, the __creation__, __alter__ and __drop__ of keyspaces.

**primeight** tries to make these tasks the easiest experience possible.

## Keyspace names

**primeight** allows for both __static__ or __dynamic__ keyspace names.
The first are specified in the table configuration, 
while the latter are overwritten in an action by action manner.

The dynamic names allow for a more costumizable table configuration, 
enabling, for instance, to have a keyspace per client.

## Creating keyspaces

```python
from primeight import CassandraManager, CassandraKeyspace
from primeight.parser import YamlParser

config = YamlParser.parse("devices.yaml")

manager = CassandraManager(["127.0.0.1"])
manager.connect()

keyspace = CassandraKeyspace(config, cassandra_manager=manager)
keyspace.create().execute()
```

or using a __dynamic keyspace name__:

```python
from primeight import CassandraManager, CassandraKeyspace
from primeight.parser import YamlParser

config = YamlParser.parse("devices.yaml")

manager = CassandraManager(["127.0.0.1"])
manager.connect()

keyspace = CassandraKeyspace(config, cassandra_manager=manager)

client_name = "Client ABC"
keyspace.create(name=client_name.replace(' ', '')).execute()
```

You can read more on the parameters of the `create` method [here](/reference/cassandra-keyspace/#create).

## Altering keyspaces

```python
from primeight import CassandraManager, CassandraKeyspace
from primeight.parser import YamlParser

config = YamlParser.parse("devices.yaml")

manager = CassandraManager(["127.0.0.1"])
manager.connect()

keyspace = CassandraKeyspace(config, cassandra_manager=manager)
keyspace.alter(replication_strategy="SimpleStrategy", replication_factor=3).execute()
```

You can read more on the parameters of the `alter` method [here](/reference/cassandra-keyspace/#alter).

## Dropping keyspaces

**primeight** tries to make the creation of keyspaces the easiest experience possible.

```python
from primeight import CassandraManager, CassandraKeyspace
from primeight.parser import YamlParser

config = YamlParser.parse("devices.yaml")

manager = CassandraManager(["127.0.0.1"])
manager.connect()

keyspace = CassandraKeyspace(config, cassandra_manager=manager)
keyspace.drop().execute()
```

You can read more on the parameters of the `drop` method [here](/reference/cassandra-keyspace/#drop).