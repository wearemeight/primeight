<p align="center">
    <a href="https://pri.meight.com"><img width=256 height=256 src="https://raw.githubusercontent.com/wearemeight/primeight/initial-version/docs_src/docs/images/logo.svg" alt="Primeight"></a>
</p>
***

**primeight** is a Python package designed to make the powerful NoSQL [Cassandra](https://cassandra.apache.org/) database system easily available to everyone. 
We had two major objectives in mind: encapsulate its partition system with a very easy interface, and enforce Cassandra best practices by defining queries for tables

Additionally we added other proxies to convenient operations such as creation, management, and drop of keyspaces, tables and/or materialized views.

**primeight** came to life from the necessity to create a standard at Meight Engineering when interacting with Cassandra. 
We recognised that when dealing with IOT Time Series data there are typically 3 dimensions to partition it: by __time__, __space__, and/or __identifier__. 

**primeight** tries to make the most of Cassandra core ideas, like Query-driven modelling and partitions to optimise for speed, 
while making available one of the most powerful database technologies to the Python community!

!!! info
    **primeight** is still in beta, as well as this documentation.
    If you find anything that you believe is incorrect please feel free to open an issue or open a pull request.

## Installation

```
pip install primeight
```

## Example

This example demonstrates the most important tasks.
We will be using the following `devices.yaml` configuration file.

```yaml
version: '1.0.0'

name: 'devices'
keyspace: 'meight'

columns:
  device_id:
    type: uuid
  ts:
    type: timestamp
generated_columns:
  day: ts

query:
  base:
    required:
      id: device_id
    optional:
      - day
    order:
      day: desc
```

### Load table configuration

```python
from primeight.parser import YamlParser

config = YamlParser.parse('devices.yaml')
```

### Create connection

```python
from primeight import CassandraManager

manager = CassandraManager(['127.0.0.1'])
manager.connect()
```

### Create keyspace

```python
from primeight import CassandraKeyspace

keyspace = CassandraKeyspace(config, cassandra_manager=manager)
keyspace.create()
```

### Create table

```python
from primeight import CassandraTable

table = CassandraTable(config, cassandra_manager=manager)
table.create()
```

### Insert data

```python
table.insert({'device_id': '659b6222-19fb-416e-9aa5-9df8cf679247', 'ts': 1592491059984}).execute()
```

### Query data

```python
rows = \
    table \
    .query('base') \
    .id('659b6222-19fb-416e-9aa5-9df8cf679247') \
    .execute()
```

### Recap

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
keyspace.create().execute()
table.create().execute()

# Insert line into table.
table.insert({'device_id': '659b6222-19fb-416e-9aa5-9df8cf679247', 'ts': 1592491059984}).execute()

# Query table.
rows = \
    table \
    .query('base') \
    .id('659b6222-19fb-416e-9aa5-9df8cf679247') \
    .execute()
n_rows = len(rows)
print(f"Queried {len(rows)} rows")
```

## License

This project is licensed under the terms of the __Apache License 2.0__ license.
