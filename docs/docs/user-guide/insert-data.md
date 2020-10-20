# Insert data

Inserting data to Cassandra can seem tricky some times.
**primeight** tries to abstract it the most and make it the easiest experience possible.

Because each row is identified by their Primary Keys,
specified in the table configuration file by the required and optional columns of the `base` query,
those keys are required when making inserts.

## Generated Columns

When inserting rows into Cassandra using **primeight**,
it automatically generates the `#!python generated_columns` for you,
so that you only need introduce the columns you desire.

For instance, using the `devices.yaml` configuration file.

```yaml
version: '1.0'

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
      id: asc
      day: desc
```

When inserting rows using **primeight**, the final row will look like this:

```python
...

table.insert({'device_id': '659b6222-19fb-416e-9aa5-9df8cf679247', 'ts': 1592575324304}).execute()

# Actual insert: {'device_id': '659b6222-19fb-416e-9aa5-9df8cf679247', 'ts': 1592575324304, 'day': 1592524800000}
```

## Updating rows

One important note is that, in contrast to SQL,
Cassandra does not check if a row exists prior to the insert,
and as such it updates it if it already exists.

It is, however, possible to avoid this behaviour by setting the `#!python insert`
method parameter `#!python if_not_exists` to `#!python True`.

```python
table.insert({'device_id': '659b6222-19fb-416e-9aa5-9df8cf679247', 'ts': 1592575324304}, if_not_exists=True)
```

Keep in mind though that this incurs in a performance cost on insert.

### Deleting column value

Cassandra has no concept for `#!python None` values, 
meaning `#!python None` values (or `#!cql NULL` values in Cassandra) are considered as "empty" values.
As such, whenever updating a row with `#!python None` values, will cause those values to be deleted.

This can be really usefull, but has also some dangers attached, 
so you must be very cautious when inserting data, to make sure you are not accidentally deleting any columns.
