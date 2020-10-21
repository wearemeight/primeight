# CassandraMaterializedView

__Inherits:__ `#!python primeight.CassandraTable`

The `#!python CassandraMaterializedView` manages materialized view interactions.

## Import

```python
from primeight import CassandraMaterializedView
```

## Constructor

- _config_ `#!python dict` __[Required]__: Table configuration, as returned by one of the parsers.
- _query_name_ `#!python str` __[Required]__: Query name.
- _keyspace_ `#!python primeight.keyspace.CassandraKeyspace` __[Required]__: Keyspace.
- _cassandra_manager_ `#!python primeight.manager.CassandraManager` __(Default:__ `#!python None`__)__: Cassandra manager.

## Attributes

### query_name
__Type__: `str`

Query name.

## Methods

### create

Build materialized view `CREATE` statement(s).

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraMaterializedView.execute`).

__Parameters:__

- _keyspace_ `str or List[str]` __(Default:__ `#!python None`__)__: Keyspace name(s).
    If a `#!python List[str]` is passed it will create multiple statements, one for each name.
    If set to `#!python None`, `#!python CassandraMaterializedView.keyspace` is used instead. 
- _gc_grace_seconds_ `#!python int` __(Default:__ `#!python 86400`__)__: Grace period in seconds.
    Time to wait before garbage collecting tombstones (deletion markers).
- _if_not_exists_ `#!python bool` __(Default:__ `#!python False`__)__: 
    If `#!python True` adds `IF NOT EXISTS` option to the statement(s).
    Attempting to create an already existing materialized view will return an error unless the `IF NOT EXISTS` option is used. If it is used, the statement will be a no-op if the materialized view already exists.

__Return:__ `self`

### drop

Build materialized view `DROP` statement(s).

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraMaterializedView.execute`).

__Parameters:__

- _keyspace_ `str or List[str]` __(Default:__ `#!python None`__)__: Keyspace name(s).
    If a `#!python List[str]` is passed it will create multiple statements, one for each name.
    If set to `#!python None`, `#!python CassandraMaterializedView.keyspace` is used instead.
- _if_exists_ `#!python bool` __(Default:__ `#!python False`__)__: 
    If `#!python True` adds `IF EXISTS` option to the statement(s).
    If the materialized view(s) do not exist, the statement will return an error, 
    unless `IF EXISTS` is used in which case the operation is a no-op.

__Return:__ `self`

### insert

Materialize views do not support inserts.

!!! danger
    Calling this method will generate an `#!python Exception`.

### query

Build materialized view `SELECT` statement(s).

This method can be chained with the methods required to define restrictions, 
and ending with the `CassandraMaterializedView.execute` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraMaterializedView.execute`).

__Parameters:__

- _keyspace_ `str or List[str]` __(Default:__ `#!python None`__)__: Keyspace name(s).
    If a `#!python List[str]` is passed it will create multiple statements, one for each name.
    If set to `#!python None`, `#!python CassandraMaterializedView.keyspace` is used instead.

__Return:__ `self`