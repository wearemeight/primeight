# CassandraColumn

The `CassandraColumn` represents the columns of Cassandra tables.

## Import

```python
from primeight import CassandraColumn
```

## Constructor [github]()

- _name_ `#!python str` __[Required]__: Column name.
- _type_handle_ `#!python str` __[Required]__: Column type.
- _alias_ `#!python str` __(Default:__ `#!python None`__)__: A different name to identify the column.
- _description_ `#!python str` __(Default:__ `#!python None`__)__: Column description.
- _min_value_ `#!python int or float` __(Default:__ `#!python None`__)__: Minimum value that column can have.
- _max_value_ `#!python int or float` __(Default:__ `#!python None`__)__: Maximum value that column can have.

## Attributes

### name
__Type__: `#!python str`

Column name.

### type
__Type__: `#!python str`

Column type.

### alias
__Type__: `#!python str`

A different name to identify the column.

### description
__Type__: `#!python str`

Column description.

### min_value
__Type__: `#!python int or float`

Minimum value that column can have.

### max_value
__Type__: `#!python int or float`

Minimum value that column can have.

## Methods

### pydantic_type

Column [Pydantic](https://pydantic-docs.helpmanual.io/) type.

__Parameters:__

- _handle_ `#!python str` __(Default:__ `#!python None`__)__ Column type handler. 
    If set to `#!python None`, `#!python CassandraColumn.type` is used. 
    This parameter is mostly used internally.

__Return:__ `#!python Any`

### cassandra_type

Cassandra column type.

__Parameters:__

- _handle_ `#!python str` __(Default:__ `#!python None`__)__ Column type handler. 
    If set to `#!python None`, `#!python CassandraColumn.type` is used. 
    This parameter is mostly used internally.
- _frozen_ `#!python bool` __(Default:__ `#!python True`__)__ Whether the column should 
    be frozen or not.

__Return:__ `#!python str`

### is_valid

Validate value according to the maximum and minimum values of the column.

__Parameters:__

- _value_ `#!python int or float` __[Required]__: Value to validate
__Return:__ `#!python bool`
