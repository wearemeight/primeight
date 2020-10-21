# CassandraTable

__Inherits:__ `#!python primeight.base.CassandraBase`

The `#!python CassandraTable` manages table interactions.

## Import

```python
from primeight import CassandraTable
```

## Constructor

- _config_ `#!python dict` __[Required]__: Table configuration, as returned by one of the parsers.
- _query_name_ `#!python primeight.keyspace.CassandraKeyspace` __[Required]__: Keyspace.
- _cassandra_manager_ `#!python primeight.manager.CassandraManager` __(Default:__ `#!python None`__)__: Cassandra manager.

## Attributes

### name
__Type__: `#!python str`

Table name.

### keyspace
__Type__: `#!python primeight.CassandraKeyspace`

Keyspace passed during object initialization.

### columns
__Type__: `#!python List[primeight.CassandraColumn]`

List of columns.

### col
__Type__: `#!python Dict[str, primeight.CassandraColumn]`

Dictionary of columns, maping column name to the column itself.

### model
__Type__: `#!python pydantic.BaseModel`

Pydantic model representing the table.

## Methods

### get_columns

Filter list of columns by list of names and/or alias, 
working as a white list.

If `#!python names` is `#!python None` 
and `#!python alias` is `#!python None`, all columns are returned.

__Parameters:__

- _names_ `List[str]` __(Default:__ `None`__)__: List of names.
- _alias_ `List[str]` __(Default:__ `None`__)__: List of alias.

__Return:__ `#!python List[primeight.CassandraColumn]`

### get

Get column by name.
If there is no column with the specified name, `None` is returned.

__Parameters:__

- _name_ `str` __[Required]__: Column name.

__Return:__ `#!python Optional[CassandraColumn]`

### has_split

Whether a table has a temporal split or not.

__Return:__ `#!python bool`

### create

Build table `CREATE` statement(s).

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _keyspace_ `str or List[str]` __(Default:__ `#!python None`__)__: Keyspace name(s).
    If a `#!python List[str]` is passed it will create multiple statements, one for each name.
    If set to `#!python None`, `#!python CassandraMaterializedView.keyspace` is used instead. 
- _gc_grace_seconds_ `#!python int` __(Default:__ `#!python 86400`__)__: Grace period in seconds.
    Time to wait before garbage collecting tombstones (deletion markers).
- _if_not_exists_ `#!python bool` __(Default:__ `#!python False`__)__: 
    If `#!python True` adds `IF NOT EXISTS` option to the statement(s).
    Attempting to create an already existing materialized view will return an error unless the `IF NOT EXISTS` option is used. If it is used, the statement will be a no-op if the materialized view already exists.
- _create_materialized_views_ `#!python bool` __(Default:__ `#!python True`__)__: 
    Whether to create the materialized view `CREATE` statement(s) after the table statement(s).

__Return:__ `self`

### drop

Build table `DROP` statement(s).

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _keyspace_ `str or List[str]` __(Default:__ `#!python None`__)__: Keyspace name(s).
    If a `#!python List[str]` is passed it will create multiple statements, one for each name.
    If set to `#!python None`, `#!python CassandraMaterializedView.keyspace` is used instead.
- _if_exists_ `#!python bool` __(Default:__ `#!python False`__)__: 
    If `#!python True` adds `IF EXISTS` option to the statement(s).
    If the materialized view(s) do not exist, the statement will return an error, 
    unless `IF EXISTS` is used in which case the operation is a no-op.
- _drop_materialized_views_ `#!python bool` __(Default:__ `#!python True`__)__: 
    Whether to create the materialized view `DROP` statement(s) before the table statement(s).

__Return:__ `self`

### insert

Build table `INSERT` statement.

When Cassandra does an insert and the row already exists,
it overwrites the current column values.

Also, for Cassandra, `#!python None` row values are considered as empty,
meaning that if you overwrite a column with a `#!python None` value
it will delete that row column.
This means that you need to be careful whenever making inserts,
so that you do not delete any undesired columns. 

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _row_ `#!python Dict[str, Any]` __[Required]__: Row.
- _keyspace_ `str or List[str]` __(Default:__ `#!python None`__)__: Keyspace name(s).
    If a `#!python List[str]` is passed it will create multiple statements, one for each name.
    If set to `#!python None`, `#!python CassandraMaterializedView.keyspace` is used instead. 
- _ttl_ `#!python bool` __(Default:__ `#!python False`__)__: 
    Specifies an optional __Time To Live__ in seconds for the inserted values. 
    If set, the inserted values are automatically removed from the database after the specified time. Note that the TTL concerns the inserted values, not the columns themselves. This means that any subsequent update of the column will also reset the TTL (to whatever TTL is specified in that update). By default, values never expire. A TTL of 0 is equivalent to no TTL. If the table has a default_time_to_live, a TTL of 0 will remove the TTL for the inserted or updated values. A TTL of null is equivalent to inserting with a TTL of 0.

__Return:__ `self`

### query

Build table or materialized view `SELECT` statement(s), depending on the query `#!python name` selected.

This method can be chained with the methods required to define restrictions, 
and ending with the `CassandraTable.execute` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _name_ `#!python str` __(Default:__ `#!python 'base'`__)__: Query name.
- _keyspace_ `#!python str or List[str]` __(Default:__ `#!python None`__)__: Keyspace name(s).
    If a `#!python List[str]` is passed it will create multiple statements, one for each name.
    If set to `#!python None`, `#!python CassandraTable.keyspace` is used instead.

__Return:__ `self`

### select

Filter columns of `SELECT` statement(s), works as a white list.
If not called, all columns will be returned.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _columns_ `#!python List[str]` __(Default:__ `#!python 'base'`__)__: List of column names.

__Return:__ `self`

### time

Complement `SELECT` statement(s), with time information.
This method complements the table `#!yaml split` and , 
if `#!python split_only` is `#!python False`, adds the where clause 
for the `#!yaml time` column specified in the table configuration.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _start_ `#!python datetime.datetime` __[Required]__: Start date.
- _end_ `#!python datetime.datetime` __[Required]__: End date.
- _prepare_ `#!python bool` __(Default:__ `#!python False`__)__: 
    Whether to create statement ready to be a prepared statement.
    If set to `#!python True`, will only define table `#!yaml split` 
    and will leave the required key as a prepared statement parameter.
- _split_only_ `#!python bool` __(Default:__ `#!python False`__)__: 
    Whether to only complement the table `#!yaml split` as specified in the table configuration.

__Return:__ `self`

### space

Complement `SELECT` statement(s), with space information.
This method adds the where clause for the `#!yaml space` column specified in the table configuration.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _identifier_ `#!python str or List[str]` __(Default:__ `#!python None`__)__: 
    Space identifier or list of identifiers.
    If set to `#!python None`, will leave the required key as a prepared statement parameter.


__Return:__ `self`

### id

Complement `SELECT` statement(s), with identifier information.
This method adds the where clause for the `#!yaml id` column specified in the table configuration.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _identifier_ `#!python str or List[str]` __(Default:__ `#!python None`__)__: 
    Identifier or list of identifiers.
    If set to `#!python None`, will leave the required key as a prepared statement parameter.

__Return:__ `self`

### equals

Complement `SELECT` statement(s), with an equality where clause.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _column_ `#!python str` __[Required]__: Column name.
- _value_ `#!python Any` __(Default:__ `#!python None`__)__: Value.
    If set to `#!python None`, will leave the column as a prepared statement parameter.

__Return:__ `self`

### among

Complement `SELECT` statement(s), with an `IN` where clause.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _column_ `#!python str` __[Required]__: Column name.
- _value_ `#!python List[Any]` __(Default:__ `#!python None`__)__: List of values.
    If set to `#!python None`, will leave the column as a prepared statement parameter.

__Return:__ `self`

### between

Complement `SELECT` statement(s), with a range where clause.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _column_ `#!python str` __[Required]__: Column name.
- _lower_ `#!python Any` __(Default:__ `#!python None`__)__: Lower value.
    If set to `#!python None`, will leave the column as a prepared statement parameter.
- _higher_ `#!python Any` __(Default:__ `#!python None`__)__: Higher value.
    If set to `#!python None`, will leave the column as a prepared statement parameter.

__Return:__ `self`

### between_including

Complement `SELECT` statement(s), with an inclusive range where clause.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _column_ `#!python str` __[Required]__: Column name.
- _lower_ `#!python Any` __(Default:__ `#!python None`__)__: Lower value.
    If set to `#!python None`, will leave the column as a prepared statement parameter.
- _higher_ `#!python Any` __(Default:__ `#!python None`__)__: Higher value.
    If set to `#!python None`, will leave the column as a prepared statement parameter.

__Return:__ `self`

### lower_than

Complement `SELECT` statement(s), with a lower than inequality where clause.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _column_ `#!python str` __[Required]__: Column name.
- _boundary_ `#!python Any` __(Default:__ `#!python None`__)__: Value.
    If set to `#!python None`, will leave the column as a prepared statement parameter.

__Return:__ `self`

### lower_or_equal_than

Complement `SELECT` statement(s), with a lower or equal than inequality where clause.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _column_ `#!python str` __[Required]__: Column name.
- _boundary_ `#!python Any` __(Default:__ `#!python None`__)__: Value.
    If set to `#!python None`, will leave the column as a prepared statement parameter.

__Return:__ `self`

### higher_than

Complement `SELECT` statement(s), with a higher than inequality where clause.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _column_ `#!python str` __[Required]__: Column name.
- _boundary_ `#!python Any` __(Default:__ `#!python None`__)__: Value.
    If set to `#!python None`, will leave the column as a prepared statement parameter.

__Return:__ `self`

### higher_or_equal_than

Complement `SELECT` statement(s), with a higher or equal than inequality where clause.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _column_ `#!python str` __[Required]__: Column name.
- _boundary_ `#!python Any` __(Default:__ `#!python None`__)__: Value.
    If set to `#!python None`, will leave the column as a prepared statement parameter.

__Return:__ `self`

### limit

Complement `SELECT` statement(s), with a `LIMIT` clause.
The `LIMIT` option to a `SELECT` statement limits the number of rows returned by a query.

This method must be chained after the `#!python CassandraTable.query` method.

!!! note
    The statement is only executed using the execute methods (e.g `#!python CassandraTable.execute`).

__Parameters:__

- _value_ `#!python int` __[Required]__: Limiting value.

__Return:__ `self`