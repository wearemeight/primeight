# CassandraManager

The `#!python CassandraManager` manages all the interactions with Cassandra.


## Import

```python
from primeight import CassandraManager
```

## Constructor

- _contact_points_ `#!python List[str]` __[Required]__: List of Cassandra contact points.
- _connect_timeout_ `#!python float` __(Default:__ `#!python 5.0`__)__: Timeout, in seconds,
  for creating new connections.
- _control_connection_timeout_ `#!python float` __(Default:__ `#!python 2.0`__)__: A timeout,
  in seconds, for queries made by the control connection,
  such as querying the current schema and information about nodes in the cluster.
  If set to `#!python None`, there will be no timeout for these queries.
- profiles `#!python Dict[str, cassandra.cluster.ExecutionProfile]` __(Default:__ `#!python None`__)__:
  Dictionary of Cassandra [Execution Profiles](https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/cluster/#cassandra.cluster.ExecutionProfile), with the execution profile name as key.
  These profiles are  available when executing queries.
  If set to `#!python None`, defaults to a execution profile with:
    - `#!python cassandra.policies.RoundRobinPolicy` load balancing policy
    - `#!python cassandra.policies.RetryPolicy` retry policy
    - `#!python LOCAL_ONE` consistency level
    - `#!python cassandra.query.dict_factory` row factory
    - `#!python 10.0` second `#!python request_timeout`

## Attributes

### contact_points
__Type__: `#!python List[str]`

List of Cassandra contact points, i.e. the addresses used to connect.

### cluster
__Type__: `#!python cassandra.cluster.Cluster`

Cassandra [Cluster](https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/cluster/#module-cassandra.cluster) object.

### session
__Type__: `#!python cassandra.cluster.Session`

Cassandra [Session](https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/cluster/#cassandra.cluster.Session) object.

### execution_profiles
__Type:__ `#!python Dict[str, cassandra.cluster.ExecutionProfile]`
Dictionary of Cassandra [ExecutionProfiles](https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/cluster/#cassandra.cluster.ExecutionProfile),
with the execution profile name as key.
These profiles are  available when executing queries.

### address_translator
__Type:__ `#!python cassandra.policies.AddressTranslator`
Cassandra [AddressTranslator](https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/policies/#cassandra.policies.AddressTranslator) in use (if any).

## Methods

### create_execution_profile

==Static Method==

Create a Cassandra [execution profile](https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/cluster/#cassandra.cluster.ExecutionProfile).

__Parameters:__

- _load_balancing_policy_ `#!python cassandra.policies.LoadBalancingPolicy` __[Required]__
- _retry_policy_ `#!python cassandra.policies.RetryPolicy` __[Required]__
- _consistency_level_ `#!python cassandra.policies.RetryPolicy` __(Default:__ `#!python cassandra.ConsistencyLevel.LOCAL_ONE`__)__
- _row_factory_ `#!python Callable` __(Default:__ `#!python cassandra.query.dict_factory`__)__
- _request_timeout_ `#!python float` __(Default:__ `#!python 10.0`__)__

__Return:__ `#!python cassandra.cluster.ExecutionProfile`

### connect

Connect to Cassandra, and creates a [session](https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/cluster/#cassandra.cluster.Session).

__Parameters:__

- _keyspace_ `#!python str` __(Default:__ `#!python None`__)__: Default keyspace for operations.

__Return:__ `#!python self`

### execute

Execute statement(s) sequentially.
The return type depends on the `#!python row_factory` defined in the execution profile.

__Parameters:__

- _statements_ `#!python List[str]` __(Default:__ `#!python None`__)__: List of statements
- _execution_profile_ `#!python str or cassandra.cluster.ExecutionProfile` __(Default:__ `#!python None`__)__: 
    Execution profile name or ExecutionProfile object

__Return:__ `#!python List[tuple] or List[dict]`

### execute_concurrent

Execute statement(s) concurrently.
The return type depends on the `#!python row_factory` defined in the execution profile.

__Parameters:__

- _statements_ `#!python List[str]` __(Default:__ `#!python None`__)__: List of statements
- _raise_on_first_error_ `#!python bool` __(Default:__ `#!python False`__)__:
  Whether to stop after the first failed statement

__Return:__ `#!python List[tuple] or List[dict]`
