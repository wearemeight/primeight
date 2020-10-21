# CassandraBase

The `#!python CassandraBase` class serves as a common base.

## Import

```python
from primeight.base import CassandraColumn
```

## Constructor

- _config_ `#!python dict` __[Required]__: table configuration, as returned by one of the parsers
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

## Methods

### execute

Execute `#!python CassandraBase.statements` sequentially using the `#!python CassandraBase.cassandra_manager`.
The return type depends on the `#!python row_factory` defined in the execution profile.

__Parameters:__

- _execution_profile_ `#!python str or cassandra.cluster.ExecutionProfile` __(Default:__ `#!python None`__)__: 
    Execution profile name or ExecutionProfile object

__Return:__ `#!python List[tuple] or List[dict]`

### execute_concurrent

Execute `#!python CassandraBase.statements` concurrently using the `#!python CassandraBase.cassandra_manager`.
The return type depends on the `#!python row_factory` defined in the execution profile.

__Parameters:__

- _raise_on_first_error_ `#!python bool` __(Default:__ `#!python False`__)__:
  Whether to stop after the first failed statement

__Return:__ `#!python List[tuple] or List[dict]`