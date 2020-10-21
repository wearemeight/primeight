# CassandraKeyspace

__Inherits:__ `#!python primeight.base.CassandraBase`

The `#!python CassandraKeyspace` manages keyspace interactions.

## Import

```python
from primeight import CassandraKeyspace
```

## Constructor

- _config_ `#!python dict` __[Required]__: Table configuration, as returned by one of the parsers
- _cassandra_manager_ `#!python primeight.manager.CassandraManager` __(Default:__ `#!python None`__)__: Cassandra manager

## Attributes

### cassandra_manager
__Type__: `#!python primeight.manager.CassandraManager`

Cassandra manager where to run the statements.

### config
__Type__: `#!python dict`

Table configuration, as returned by one of the parsers
from the module `#!python primeight.parser`.

### statements
__Type__: `#!python List[str]`

Current statements in object.

### name
__Type__: `#!python str`

Keyspace name.

## Methods

### create

Build keyspace `CREATE` statement(s).

!!! note
    The statement is only executed using the execute methods (e.g `#!python  CassandraKeyspace.execute`).

__Parameters:__

- _name_ `#!python str or List[str]` __(Default:__ `#!python None`__)__: Keyspace name.
    If a `#!python List[str]` is passed it will create multiple statements, one for each name.
    If set to `None`, the `#!python CassandraKeyspace.name` is used instead. 
- _replication_strategy_ `#!python str` __(Default:__ `#!python SimpleStrategy`__)__: Keyspace replication strategy.
- _replication_factor_ `#!python int or Dict[str, int]` __(Default:__ `#!python 3`__)__: Keyspace replication factor.
    When using the `SimpleStrategy` replication strategy,
    this parameter should be an integer.
    When using the `NetworkTopologyStrategy` replication strategy,
    this should be a dictionary mapping data center names
    to an integer representing the replication factor in that data center.
- _if_not_exists_ `#!python bool` __(Default:__ `#!python False`__)__: 
    If `#!python True` adds `IF NOT EXISTS` option to the statement(s).
    Attempting to create a keyspace that already exists will return an error unless the `IF NOT EXISTS` option is used. 
    If it is used, the statement will be a no-op if the keyspace already exists.

__Return:__ `#!python self`

### drop

Build keyspace `DROP` statement(s).

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraKeyspace.execute`).

__Parameters:__

- _name_ `#!python str or List[str]` __(Default:__ `#!python None`__)__: Keyspace name.
    If a `#!python List[str]` is passed it will create multiple statements, one for each name.
    If set to `#!python None`, the `#!python CassandraKeyspace.name` is used instead. 
- _if_exists_ `#!python bool` __(Default:__ `#!python False`__)__: 
    If `#!python True` adds `IF EXISTS` option to the statement(s).
    If the keyspace does not exists, the statement will return an error, 
    unless `IF EXISTS` is used in which case the operation is a no-op.

__Return:__ `#!python self`


### alter

Build keyspace `ALTER` statement(s).

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraKeyspace.execute`).

__Parameters:__

- _name_ `#!python str or List[str]` __(Default:__ `#!python None`__)__: Keyspace name.
    If a `#!python List[str]` is passed it will create multiple statements, one for each name.
    If set to `None`, the `#!python CassandraKeyspace.name` is used instead. 
- _replication_strategy_ `#!python str` __(Default:__ `#!python SimpleStrategy`__)__: Keyspace replication strategy.
- _replication_factor_ `#!python int or Dict[str, int]` __(Default:__ `#!python 3`__)__: Keyspace replication factor.
    When using the `SimpleStrategy` replication strategy,
    this parameter should be an integer.
    When using the `NetworkTopologyStrategy` replication strategy,
    this should be a dictionary mapping data center names
    to an integer representing the replication factor in that data center.

__Return:__ `#!python self`

