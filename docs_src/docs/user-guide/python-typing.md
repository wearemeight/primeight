# Python Typing

Since __Python 3.6__, Python included support for type hints.
This enables editors and tools to help you be more proeficient when coding.

**primeight** also allows this by incorporating methods that allow you to 
dynamically generate models for your tables and types for columns.

## Table model

Using the `model` attribute of the `CassandraTable` or `CassandraMaterializedView`, 
you have access to a [Pydantic](https://pydantic-docs.helpmanual.io/) model of your table.

```python
from primeight import CassandraTable
from primeight.parser import YamlParser

config = YamlParser.parse('devices.yaml')
table = CassandraTable(config)

table.model
```

## Column type

Using the `pydantic_type` attribute of the `CassandraColumn`, you have access to type of your column.

```python
from primeight import CassandraTable
from primeight.parser import YamlParser

config = YamlParser.parse('clients.yaml')
table = CassandraTable(config)

table.col['device_id'].pydantic_type
```
