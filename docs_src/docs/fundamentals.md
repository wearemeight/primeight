# Fundamentals

## Configuration
The **primeight** Python library is a unified interface to Cassandra.

Each Cassandra table is defined by a __Yaml__ configuration file.
This Yaml file has a specific structure that is enforced by the parser.
If any thing is not as expected the parser will throw a `SyntaxError`
and provide an explanation why.

### Yaml structure

#### Required Fields
The yaml has 5 required fields: `version`, `keyspace`, `name`, `columns`, and `query`.

##### Version
The `version` field represents the version of the template.
This field is used to keep track of the updates done to a table.

##### Keyspace
The `keyspace` field specifies the keyspace a table belongs.

Since there are cases where the data may be associated with a dynamic keyspace,
we the keyspace name can be overwritten for each action (e.g. while querying or inserting data).

##### Name
The `name` field is the name of the table.
[//]: <> (Keep in mind that this may not directly correspond to the Cassandra table name, as **primeight** has a way of splitting tables by timeframe (check **Split**).)

##### Columns
The `columns` field is where the table columns are defined.
It is a list of objects with a `type` required field.
The object can be complemented with an `alias` name, a `description`,
a `min`, and `max` values.

The columns can have all the
[Cassandra Data Types](https://cassandra.apache.org/doc/latest/cql/types.html),
like `int`, `float`, `text`, etc..., or `h3hex`,
a custom type used to partition the that geo-spatially.
[Cassandra Collections](https://cassandra.apache.org/doc/latest/cql/types.html#collections)
data types are also supported.
For example, you can define a `set` of `text` attributes with `set<text>`, or
a more complex example may be to define a list of GPS points with
`list<tuple<float,float,float>>`.

If you require the definition of minimum (`min`) and maximum (`max`) values,
both values must be numbers (i.e. `int` or `float`).

Finally, since we also allow segmentation by keyspace,
a table can define its keyspace in the yaml configuration using the `keyspace` field,
or in an operation basis.

The Cassandra table columns may also be complemented
with `generated_columns` (see **Generated Columns**).

##### Query
The `query` field is used to optimize queries.
Here you can define the queries performed on the Cassandra table,
and internally the system will optimize for speed.

The `base` query is always required, ideally it should be the most used query.
It reflects the Cassandra table, while the remaining queries are
[Materialized Views](https://cassandra.apache.org/doc/latest/cql/mvs.html) derived from this table.

Every query has three types of columns:
the `required`, the `optional`, and all the remaining columns.

The `required` columns are the columns that should be specified each time you do a query.
It can be segmented by `time`, `space`, and/or `id`.

When using `time` and `space` as `required` columns you need to use generated columns.
This is enforces a consistent search pattern.
On the other hand, when using an `id` you can use any column.

The `optional` field is used to enumerate columns that may be used for
filtering with methods like `between`, or `higher_then`, or any of the available methods.
Be aware that the order in the `optional` field is crucial,
as you can not restrict a column without also defining the attributes that precede it.

Additionally, you can specify an `order` to sort how the data is saved.
There are two options, `asc` for ascending order, and `desc` for descending order.

**Note:** You can only specify the order for `required` or `optional` attributes.

#### Optional Fields

##### Generated Columns
Generated columns are columns that are created automatically using
other attributes through the predefined generators.
It is defined using the `generated_columns` field,
that receives an object of mappings of generator to input attribute(s).

When specifying more than one attribute, separate the attributes using **a comma**.

For example:

```yaml
...
generated_columns:
  month: tsin
  h3: lat,lon
...
```

##### Generators
* **day**: receives a timestamp attribute
and produces a timestamp for the same day at midnight
* **week**: receives a timestamp attribute
and produces a timestamp for the first day of the week at midnight
* **month**: receives a timestamp attribute
and produces a timestamp for the first day of the month at midnight
* **year**: receives a timestamp attribute
and produces a timestamp for the first day of the year at midnight
* **hX**: receives latitude and longitude coordinates
to produce an h3 hexadecimal identifier of level X.
Available levels span from 3 to 12.
* **hX_begin**: receives latitude and longitude coordinates
to produce an h3 hexadecimal identifier of level X.
Available levels span from 3 to 12.
This identifier is intended to specify the begin of something, e.g. a trip.
* **hX_end**: receives latitude and longitude coordinates
to produce an h3 hexadecimal identifier of level X.
Available levels span from 3 to 12.
This identifier is intended to specify the end of something, e.g. a trip.

[//]: <> (##### Split)
[//]: <> (The `split` field adds a new level of partition to the tables.)
[//]: <> (It allows the creation of tables by `day`, `week`, `month`, or `year`.)

[//]: <> (The resulting table name has the format `{name}_{dateframe}`.)
[//]: <> (For example, the name of the `fms` table for `18/06/2019` is `fms_18_06_2019`.)

[//]: <> (Finally, to ensure that no redundant queries exist the parser)
[//]: <> (will throw an error if any query has the same `time` required columns)
[//]: <> (as the `split` partition.)

### Examples
The simplest example is:

```yaml
version: '0.1'

name: 'devices'
keyspace: 'meight'

columns:
  device_id:
    type: text

query:
  base:
    required:
      id: device_id
```

## Partitions

One of Cassandra's strongest points is how data is arranged and stored making it quickly accessible. 
This mechanism revolves around partitions.
**primeight** recognises 3 major types of partition: time, space and ids.
This makes the structuring of the tables consistent and fast to access.

Each table has its partitions defined in the query `required` attribute.