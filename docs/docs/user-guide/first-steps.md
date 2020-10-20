# First Steps

## Basic usage

### Connecting to the cluster

The `CassandraManager` is responsible for managing the connection to the Cassandra cluster 
and executing all your actions.

The first step is to create an instance and connect to the cluster.

```python
from primeight import CassandraManager

cassandra_seed = ["127.0.0.1"]
manager = CassandraManager(cassandra_seed)
manager.connect()
```

### Loading table configuration

Next, you need to load the configuration of the table that you wish to operate over.

```python
from primeight.parser import YamlParser

parser = YamlParser('devices.yaml')
config = parser.load()
```

### Create the Table object

Finally, you are ready to create the Table object and make queries.

```python
for primeight import CassandraTable

table = CassandraTable(config, cassandra_manager=manager)
table = \
    table \
    .query('base') \
    .select(['device_id'])
```

### The execute order

**primeight** does not take any actions unless when explicitly told so.
To make sure no undesired results happen, statements are only executed when an `execute` method is called.

And if you want to examine the statements before execute, the statement list is available through the `statements` attribute.

```python
print(table.statements)

rows = table.execute()
```

### Recap

Putting it all together:

```python
from primeight import CassandraManager, CassandraTable
from primeight.parser import YamlParser

cassandra_seed = ["127.0.0.1"]
manager = CassandraManager(cassandra_seed)
manager.connect()

parser = YamlParser('devices.yaml')
config = parser.load()

table = CassandraTable(config, cassandra_manager=manager)
table = \
    table \
    .query('base') \
    .select(['device_id'])
print(table.statements)

rows = table.execute()
```