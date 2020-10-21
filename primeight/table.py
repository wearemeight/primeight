import json
import logging
from datetime import datetime, timedelta
from typing import Any, List, Dict, Optional

import pytz
from cassandra.encoder import cql_quote
from pydantic import create_model
import h3.api.basic_str as h3
from geojson import Polygon

from primeight.base import CassandraBase
from primeight.manager import CassandraManager
from primeight.keyspace import CassandraKeyspace
from primeight.column import CassandraColumn
from primeight.generators import Generators
from primeight.utils import UUIDEncoder
from primeight.exceptions import \
    DateNotDefinedError, QueryNotFound, \
    MissingColumnError, NotARequiredColumnError


class CassandraTable(CassandraBase):

    TABLE_FORMAT = {
        'day': '%d_%m_%Y',
        'week': '%d_%m_%Y',
        'month': '%m_%Y',
        'year': '%Y'
    }

    @property
    def name(self):
        """Returns the table name."""
        return self.config['name']

    @property
    def keyspace(self) -> CassandraKeyspace:
        """Returns the table keyspace object."""
        if self._keyspace is None:
            raise ValueError("Cassandra keyspace not specified.")

        return self._keyspace

    @property
    def keyspace_name(self) -> str:
        """Returns the table keyspace name."""
        try:
            _name = self.keyspace.name
        except ValueError:
            _name = self.config['keyspace']

        return _name

    @property
    def columns(self) -> List[CassandraColumn]:
        """Returns the table column list."""
        return self._columns

    @property
    def col(self) -> Dict[str, CassandraColumn]:
        return {col.name.lower(): col for col in self.columns}

    @property
    def model(self):
        """Returns a Pydantic model of the table arguments."""
        name = self.config['name'].title().replace('_', '')
        fields = {}
        for f in self.columns:
            fields[f.name] = (f.pydantic_type(), None)

        return create_model(name, **fields)

    @property
    def statements(self) -> list:
        """Returns current statements."""
        if self._current_statements is None:
            return []

        _tags = ['where', 'and', 'limit']
        _statements = []
        for statement in self._current_statements:
            if '{columns}' in statement:
                statement = self._replace(statement, 'columns', '*')

            for tag in _tags:
                _tag = '{' + tag + '}'
                if _tag in statement:
                    statement = self._replace(statement, tag, '')

            _statements.append(statement)

        return _statements

    def get_columns(
        self, names: List[str] = None, alias: List[str] = None
    ) -> List[CassandraColumn]:
        """Returns list of columns."""
        column_list = []
        for col in self.columns:
            if names is not None and col.name in names:
                column_list.append(col)

            elif alias is not None and col.alias in alias:
                column_list.append(col)

        return column_list

    def get(self, name: str) -> Optional[CassandraColumn]:
        """Returns column, returns None if column does not exist in table."""
        return self.col.get(name.lower())

    def __init__(
            self,
            config: dict,
            keyspace: CassandraKeyspace = None,
            cassandra_manager: CassandraManager = None
    ):
        """Cassandra table constructor.

        :param config: table template configuration
        :param keyspace: table keyspace
        """
        super().__init__(config, cassandra_manager)

        self._config = config
        self._keyspace = keyspace

        columns = []
        for name, content in self._config['columns'].items():
            col_min = content.get('min', None)
            col_max = content.get('max', None)
            col_alias = content.get('alias', None)
            col_description = content.get('description', None)

            col = CassandraColumn(
                name, content['type'],
                min_value=col_min, max_value=col_max,
                alias=col_alias, description=col_description
            )
            columns.append(col)

        if 'generated_columns' in self._config:
            for name, content in self._config['generated_columns'].items():
                if name in ['day', 'week', 'month', 'year']:
                    col = CassandraColumn(name, 'timestamp')
                else:
                    col = CassandraColumn(name, 'h3hex')

                columns.append(col)

        self._columns = columns

        self._current_operation = None
        self._current_query = None
        self._current_statements = None

        self._manager = cassandra_manager

    def has_split(self) -> bool:
        """Returns True if table has split, False otherwise."""
        return 'split' in self.config

    def _is_query_valid(self) -> bool:
        """Raises an Exception if statement is not ready to be executed."""

        for statement in self._current_statements:
            if '{date}' in statement:
                raise DateNotDefinedError(
                    "When splitting table by date, "
                    "you are required to specify a time frame."
                )

        return True

    @staticmethod
    def _calculate_table_partitions(partition: str, start: datetime, end: datetime) -> List[datetime]:
        """Create list of datetime from start to end date, based on partition.
        Partition may be day, month, or year.
        All datetime are return representing the initial moment of the start
        of that partition.

        :param partition: partition
        :param start: start date
        :param end: end date
        :return: list of datetime
        """

        if partition == 'day':
            dd = (end - start).days + 1

            days = []
            for i in range(dd):
                d = start + timedelta(days=i)
                days.append(d.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC))

            return days
        elif partition == 'week':
            weeks = set()

            dd = (end - start).days + 1
            for i in range(dd):
                d = start + timedelta(days=i)
                last_monday = d - timedelta(days=d.weekday())
                weeks.add(last_monday)

            return list(weeks)
        elif partition == 'month':

            def total_months(dt):
                return dt.month + 12 * dt.year

            months = []
            for tot_m in range(total_months(start) - 1, total_months(end)):
                y, m = divmod(tot_m, 12)
                months.append(datetime(y, m + 1, 1, 0, 0, 0, 0, tzinfo=pytz.UTC))

            return months
        elif partition == 'year':
            dy = (end.year - start.year) + 1

            years = []
            for i in range(dy):
                y = start.year + i
                years.append(start.replace(year=y, month=1, day=1,
                                           hour=0, minute=0, second=0,
                                           microsecond=0, tzinfo=pytz.UTC))

            return years

    @staticmethod
    def _replace(statement, column, value):
        """Format statement without throwing missing key exception.
        This is required because we want to sequentially define our statement.

        :param statement: statement
        :type statement: str
        :param column: column to replace
        :type column: str
        :param value: value to replace
        :type value: str
        :return: statement with the column replaced
        """

        class PartialFormatDict(dict):

            def __missing__(self, key):
                return '{' + key + '}'

        mapping = {column: value}
        return statement.format_map(PartialFormatDict(mapping))

    def create(
        self,
        keyspace: str or List[str] = None,
        gc_grace_seconds: int = 86400,
        if_not_exists: bool = False,
        create_materialized_views: bool = True
    ):
        """Create table statement.

        :param keyspace: keyspace name.
            This may be an str or List[str]  (default: None)
        :param gc_grace_seconds: garbage collect grace seconds.
            Defines the time in seconds that a tombstone is kept before being
            discarded.
        :param if_not_exists: if True, creates table if it does not exist.
            When set to False and the table exists, an exception is thrown.
            (default: False)
        :param create_materialized_views: if True, creates materialized views
            (default: True)
        :return: self
        """
        self._current_operation = 'create'

        keyspaces = keyspace
        if keyspaces is None:
            keyspaces = [self.keyspace_name]
        elif isinstance(keyspace, str):
            keyspaces = [keyspace]

        self._current_statements = []
        for keyspace_name in keyspaces:
            base = self._config['query']['base']

            statement = f"CREATE TABLE "

            if if_not_exists:
                statement += "IF NOT EXISTS "

            statement += f"{keyspace_name}.{self.name}"
            if self.has_split():
                statement += "_{date}"

            columns = \
                ', '.join([f'{a.name} {a.cassandra_type()}' for a in self.columns])
            primary_keys = ', '.join([pk for _, pk in base['required'].items()])
            statement += f" ( {columns}, PRIMARY KEY ( ({primary_keys})"

            if 'optional' in base:
                clustering_keys = ', '.join([ck for ck in base['optional']])
                statement += f", {clustering_keys} ) ) "
            else:
                statement += " ) ) "

            statement += "WITH"

            if 'order' in base:
                statement += f" CLUSTERING ORDER BY ( "
                for index, (column, order) in enumerate(base['order'].items()):
                    if index == 0:
                        statement += f"{column} {order.upper()}"
                    else:
                        statement += f", {column} {order.upper()}"

                statement += " ) AND"

            statement += f" gc_grace_seconds={gc_grace_seconds};"
            self._current_statements.append(statement)

        if create_materialized_views and len(self.config['query']) > 1:
            for name, query_config in self.config['query'].items():
                if name == 'base':
                    continue

                self._current_statements += \
                    CassandraMaterializedView(self.config, name, keyspace=keyspace) \
                    .create(
                        keyspace=keyspace,
                        gc_grace_seconds=gc_grace_seconds,
                        if_not_exists=if_not_exists
                    ).statements

        return self

    def drop(
        self,
        keyspace: str or List[str] = None,
        if_exists: bool = False,
        drop_materialized_views: bool = True
    ):
        """Drop table statement.

        :param keyspace: keyspace name.
            This may be an str or List[str]  (default: None)
        :param if_exists: if True, drops table if it exist.
            When set to False and the table does not exist,
            an exception is thrown (default: False)
        :param drop_materialized_views: if True, creates materialized views
            (default: True)
        :return: self
        """
        self._current_operation = 'drop'

        keyspaces = keyspace
        if keyspaces is None:
            keyspaces = [self.keyspace_name]
        elif isinstance(keyspace, str):
            keyspaces = [keyspace]

        self._current_statements = []
        if drop_materialized_views and len(self.config['query']) > 1:
            for name, query_config in self.config['query'].items():
                if name == 'base':
                    continue

                self._current_statements += \
                    CassandraMaterializedView(self.config, name, self.keyspace) \
                    .drop(keyspace=keyspace, if_exists=if_exists) \
                    .statements

        for keyspace_name in keyspaces:
            statement = f"DROP TABLE "
            if if_exists:
                statement += "IF EXISTS "

            statement += f"{keyspace_name}.{self.name}"
            if self.has_split():
                statement += "_{date}"
            statement += ";"
            self._current_statements.append(statement)

        return self

    def insert(
            self,
            row: Dict[str, Any],
            keyspace: str or List[str] = None,
            ttl: int = None
    ):
        """Insert row into Cassandra.
        This method creates an insert statement that can be chained with
        other insert statements.

        When Cassandra does an insert and the row already exists,
        it overwrites the current column values.

        Also, for Cassandra, `None` row values are considered as empty,
        meaning that if you overwrite a column with a `None` value
        it will delete that row column.
        This means that you need to be careful whenever making inserts,
        so that you do not delete any undesired columns.

        :param row: values to insert in row
        :param keyspace: keyspace name.
            This may be an str or List[str]  (default: None)
        :param ttl: time to live
        :return: self
        """
        keyspaces = keyspace
        if keyspaces is None:
            keyspaces = [self.keyspace_name]
        elif isinstance(keyspace, str):
            keyspaces = [keyspace]

        # Create generated columns using the predefined generators.
        generated_columns = {}
        if 'generated_columns' in self.config:
            for name, column in self.config['generated_columns'].items():
                generator = getattr(Generators, name)

                if ',' in column:
                    columns = "".join(column.split()).split(',')
                    values = [row[a] for a in columns]
                    generated_columns[name] = generator(*values)
                else:
                    generated_columns[name] = generator(row[column])

        json_values = json.dumps({**row, **generated_columns}, cls=UUIDEncoder)

        statements = []
        for keyspace_name in keyspaces:
            statement = f"INSERT INTO {keyspace_name}.{self.name}"
            if self.has_split():
                split = self.config['split']
                split_col = self.config['generated_columns'][split]

                date = \
                    datetime.fromtimestamp(row[split_col] / 1000, tz=pytz.UTC)
                split_date = self._calculate_table_partitions(split, date, date)[0]
                date_str = split_date.strftime(self.TABLE_FORMAT[split])

                statement += f"_{date_str}"
            statement += f" JSON \'{json_values}\' "

            if ttl is not None:
                statement += f"USING TTL {ttl}"
            statement += ";"

            statements.append(statement)

        if self._current_operation == 'insert':
            self._current_statements += statements
        else:
            self._current_operation = 'insert'
            self._current_statements = statements

        return self

    def query(self, name: str = 'base', keyspace: str or List[str] = None):
        """Query statement.
        Query must be declared in the Yaml templates file.

        If left clients is not defined, it will look in the yaml
        for the keyspace field.

        This method must be chained with the methods required to define
        the primary and clustering keys, and finalizing with
        :func:`~table.CassandraTable.execute`

        :param name: query name (default: base)
        :param keyspace: keyspace name.
            This may be an str or List[str]  (default: None)
        :return: self
        """
        if name not in self.config['query']:
            raise QueryNotFound(name)

        self._current_operation = 'query'
        self._current_query = name

        keyspaces = keyspace
        if keyspaces is None:
            keyspaces = [self.keyspace_name]
        elif isinstance(keyspace, str):
            keyspaces = [keyspace]

        self._current_statements = []
        for keyspace_name in keyspaces:
            if name == 'base':
                statement = (
                    "SELECT {columns} FROM "
                    f"{keyspace_name}.{self.name}"
                )
                if self.has_split():
                    statement += "_{date}"
                statement += " {where} {limit} ;"
                self._current_statements.append(statement)
            else:
                self._current_statements += \
                    CassandraMaterializedView(self.config, name, self.keyspace) \
                    .query(keyspace=keyspace) \
                    .statements

        return self

    def select(self, columns: List[str]):
        """Select which columns to query.
        If not called it will load all columns.

        :param columns: list of column names to retrieve
        :return: self
        """
        column_names = [a.name for a in self.columns]
        for column in columns:
            if column not in column_names:
                raise MissingColumnError(f"{column} not in table columns")

        cols_str = ", ".join(columns)
        self._current_statements = [self._replace(s, 'columns', cols_str)
                                    for s in self._current_statements]

        return self

    def _prepare_column_clause_single(self, statement: str, column_name: str):
        """Prepare statement to specify required columns.
        This method is required because we allow the query of the whole table.

        :param statement: cassandra statement
        :param column_name: column name to prepare for
        :return: list of prepared statements
        """
        if '{where}' in statement:
            prefix = "WHERE "
        else:
            prefix = "AND "
        key = '{' + column_name + '}'
        sufix = " {and} "

        return self._replace(
            statement,
            prefix.lower().replace(' ', ''),
            f"{prefix}{key}{sufix}"
        )

    def _prepare_column_clause(self, column_name: str):
        """Prepare statements to specify required columns.
        This method is required because we allow the query of the whole table.

        :param column_name: column name to prepare for
        :return: list of prepared statements
        """
        statements = []
        for statement in self._current_statements:
            if '{where}' in statement:
                prefix = "WHERE "
            else:
                prefix = "AND "
            key = '{' + column_name + '}'
            sufix = " {and}"

            statements.append(self._replace(
                statement,
                prefix.lower().replace(' ', ''),
                f"{prefix}{key}{sufix}"
            ))

        return statements

    def time(
        self,
        start: datetime, end: datetime,
        prepare: bool = False, split_only: bool = False
    ):
        """Select query time frame.

        Time must be one of the required columns in the table yaml.

        :param start: start date in UTC
        :param end: end date in UTC
        :param prepare: if True, will only define table name
            and (if applicable) will leave the required key as
            a named parameter (default: False)
        :param split_only: if True, will only replace the table split
            (default: False)
        :return: self
        """

        if self.has_split():
            split = self.config['split']
            split_date_list = self._calculate_table_partitions(split, start, end)

            statements = []
            for statement in self._current_statements:
                if '{date}' in statement:
                    for date in split_date_list:
                        date_str = date.strftime(self.TABLE_FORMAT[split])
                        statements.append(
                            self._replace(statement, 'date', date_str))
                else:
                    statements.append(statement)

            self._current_statements = statements

        if not split_only and self._current_operation == 'query':
            query = self.config['query'][self._current_query]

            if 'time' not in query['required'] and not self.has_split():
                raise NotARequiredColumnError('time', self._current_query)
            elif 'time' in query['required']:
                partition = query['required']['time']

                if self.has_split() and self.config['split'] == partition:
                    return self

                self._current_statements = \
                    self._prepare_column_clause(partition)

                date_list = self._calculate_table_partitions(partition, start, end)

                statements = []
                for statement in self._current_statements:
                    if prepare:
                        replacement = f"{partition}=?"
                        s = self._replace(statement, partition, replacement)
                        statements.append(s)
                    else:
                        for date in date_list:
                            if self.has_split():
                                split = self.config['split']
                                date_str = \
                                    date.strftime(self.TABLE_FORMAT[split])
                                if f'{self.name}_{date_str}' in statement:
                                    ts = int(date.timestamp() * 1000)
                                    replacement = f"{partition}={ts}"
                                    s = self._replace(
                                        statement, partition, replacement
                                    )

                                    statements.append(s)
                            else:
                                ts = int(date.timestamp() * 1000)
                                replacement = f"{partition}={ts}"
                                s = self._replace(
                                    statement, partition, replacement
                                )

                                statements.append(s)
                self._current_statements = statements

        return self

    def space(self, identifier: str or List[str] = None):
        """Select query spacial region.

        Space must be one of the required columns in the table yaml.

        If `identifier` parameter is missing or set to `None`,
        it will be set as a named parameter.
        This method accepts either a list of identifiers or a single value.
        If it receives a list it will
        behave like :func:`~table.CassandraTable.among`.
        If it receives a value it will
        behave like :func:`~table.CassandraTable.equals`.

        :param identifier: H3 identifier or list of H3 identifiers
        :return: self
        """

        def _log_geojson(_identifiers):
            features = []
            for _tmp in _identifiers:
                hex_json = {
                    'type': 'Feature',
                    'geometry':
                        Polygon([h3.h3_to_geo_boundary(_tmp, geo_json=True)]),
                    'properties': {}
                }
                features.append(hex_json)

            geojson_dumps = json.dumps({'type': 'FeatureCollection',
                                        'features': features})
            logging.debug(geojson_dumps)

        query = self.config['query'][self._current_query]
        if 'space' not in query['required']:
            raise NotARequiredColumnError('space', self._current_query)

        level = query['required']['space']
        if identifier is None:
            replacement = f"{level}='?'"
        elif isinstance(identifier, list):
            ids_str = ', '.join([f"'{i}'" for i in identifier])
            replacement = f"{level} IN ({ids_str})"
        else:
            replacement = f"{level}='{identifier}'"

        statements = self._prepare_column_clause(level)
        self._current_statements = \
            [self._replace(s, level, replacement) for s in statements]

        logger = logging.getLogger()
        if logger.level == logging.DEBUG:
            # Since creating a geojson takes a lot of time,
            # we only log the geojson when in logging level DEBUG.
            _log_geojson([_h3 for _h3 in identifier if h3.h3_is_valid(_h3)])

        return self

    def id(self, identifier: Any or List[Any] = None):
        """Select list of ids to query.

        Id must be one of the required columns in the table yaml.

        If `identifier` parameter is missing or set to `None`,
        it will be set as a named parameter.
        This method accepts either a list of identifiers or a single value.
        If it receives a list it will
        behave like :func:`~table.CassandraTable.among`.
        If it receives a value it will
        behave like :func:`~table.CassandraTable.equals`.

        :param identifier: identifier or list of identifiers (e.g. device_id)
        :return: self
        """
        query = self.config['query'][self._current_query]

        if 'id' not in query['required']:
            raise NotARequiredColumnError('id', self._current_query)

        name = query['required']['id']
        if identifier is None:
            replacement = f"{name}=?"
        elif isinstance(identifier, list):
            ids_str = ', '.join([cql_quote(i) for i in identifier])
            replacement = f"{name} IN ({ids_str})"
        else:
            replacement = f"{name}={cql_quote(identifier)}"

        statements = self._prepare_column_clause(name)
        self._current_statements = \
            [self._replace(s, name, replacement) for s in statements]

        return self

    def equals(self, column: str, value: Any = None):
        """Select a value to query on a specified column.

        This column must be an optional column in the table yaml.

        If `value` parameter is missing or set to `None`,
        it will be set as a named parameter with the column name.

        :param column: column name
        :param value: value
        :return: self
        """
        if value is None:
            replacement = f"{column}=?"
        else:
            replacement = f"{column}={cql_quote(value)}"

        statements = self._prepare_column_clause(column)
        self._current_statements = \
            [self._replace(s, column, replacement) for s in statements]

        return self

    def among(self, column, values: List[Any] = None):
        """Select list of values to query on a specified column.

        This column must be an optional column in the table yaml.

        If `values` parameter is missing or set to `None`,
        it will be set as a named parameter with the column name.

        :param column: column name
        :param values: list of values to filter
        :return: self
        """
        if values is None:
            replacement = f"{column} IN (?)"
        else:
            values_str = ', '.join([cql_quote(v) for v in values])
            replacement = f"{column} IN ({values_str})"

        statements = self._prepare_column_clause(column)
        self._current_statements = \
            [self._replace(s, column, replacement) for s in statements]

        return self

    def between(
        self, column: str,
        lower: int or float = None, higher: int or float = None
    ):
        """Select range of values to query on a specified column.

        This column must be an optional column in the table yaml.

        If `lower` or `higher` parameter(s) are missing or set to `None`,
        they will be set as a named parameter with the column name -
        lt_<column-name> and ht_<column-name>.

        :param column: column name
        :param lower: lower boundary
        :param higher: higher boundary
        :return: self
        """
        if lower is None:
            replacement_lower = f"{column} > ?"
        else:
            replacement_lower = f"{column} > {lower}"

        if higher is None:
            replacement_higher = f"{column} < ?"
        else:
            replacement_higher = f"{column} < {higher}"

        if self.has_split() and column in ['day', 'month', 'year']:
            split = self.config.get('split')

            lower_datetime = datetime.fromtimestamp(lower / 1000, tz=pytz.UTC)
            higher_datetime = datetime.fromtimestamp(higher / 1000, tz=pytz.UTC)

            date_list = self._calculate_table_partitions(
                self.config['split'], lower_datetime, higher_datetime
            )

            statements = []
            for statement in self._current_statements:
                lower_split = \
                    lower_datetime.strftime(self.TABLE_FORMAT[split])
                higher_split = \
                    higher_datetime.strftime(self.TABLE_FORMAT[split])

                for date in date_list:
                    date_str = date.strftime(self.TABLE_FORMAT[split])
                    if f'_{date_str}' in statement:
                        if lower_split == date_str:
                            s = self._prepare_column_clause_single(
                                statement, column
                            )
                            statement = \
                                self._replace(s, column, replacement_lower)

                        if higher_split == date_str:
                            s = self._prepare_column_clause_single(
                                statement, column
                            )
                            statement = \
                                self._replace(s, column, replacement_higher)

                statements.append(statement)

            self._current_statements = statements
        else:
            replacement = f"{replacement_lower} AND {replacement_higher}"
            statements = self._prepare_column_clause(column)
            self._current_statements = \
                [self._replace(s, column, replacement) for s in statements]

        return self

    def between_including(
        self, column: str,
        lower: int or float = None, higher: int or float = None
    ):
        """Select range of values to query on a specified column
        including the boundary.

        This column must be an optional column in the table yaml.

        If `lower` or `higher` parameter(s) are missing or set to `None`,
        they will be set as a named parameter with the column name -
        le_<column-name> and he_<column-name>.

        :param column: column name
        :param lower: lower boundary
        :param higher: higher boundary
        :return: self
        """
        if lower is None:
            replacement_lower = f"{column} >= ?"
        else:
            replacement_lower = f"{column} >= {lower}"

        if higher is None:
            replacement_higher = f"{column} <= ?"
        else:
            replacement_higher = f"{column} <= {higher}"

        if self.has_split() and column in ['day', 'month', 'year']:
            split = self.config.get('split')

            lower_datetime = datetime.fromtimestamp(lower / 1000, tz=pytz.UTC)
            higher_datetime = datetime.fromtimestamp(higher / 1000, tz=pytz.UTC)

            date_list = self._calculate_table_partitions(
                self.config['split'], lower_datetime, higher_datetime
            )

            statements = []
            for statement in self._current_statements:
                lower_split = \
                    lower_datetime.strftime(self.TABLE_FORMAT[split])
                higher_split = \
                    higher_datetime.strftime(self.TABLE_FORMAT[split])

                for date in date_list:
                    date_str = date.strftime(self.TABLE_FORMAT[split])
                    if f'_{date_str}' in statement:
                        if lower_split == date_str:
                            s = self._prepare_column_clause_single(
                                statement, column
                            )
                            statement = \
                                self._replace(s, column, replacement_lower)

                        if higher_split == date_str:
                            s = self._prepare_column_clause_single(
                                statement, column
                            )
                            statement = \
                                self._replace(s, column, replacement_higher)

                statements.append(statement)

            self._current_statements = statements
        else:
            replacement = f"{replacement_lower} AND {replacement_higher}"
            statements = self._prepare_column_clause(column)
            self._current_statements = \
                [self._replace(s, column, replacement) for s in statements]

        return self

    def lower_than(self, column: str, boundary: int or float = None):
        """Select a minimum value to query on a specified column.

        This column must be an optional column in the table yaml.

        If `boundary` parameter is missing or set to `None`,
        it will be set as a named parameter with the column name.

        :param column: column name
        :param boundary: boundary
        :return: self
        """
        if boundary is None:
            replacement = f"{column} < ?"
        else:
            replacement = f"{column} < {boundary}"

        statements = self._prepare_column_clause(column)
        self._current_statements = \
            [self._replace(s, column, replacement) for s in statements]

        return self

    def lower_or_equal_than(self, column: str, boundary: int or float = None):
        """Select a minimum value to query on a specified column.

        This column must be an optional column in the table yaml.

        If `boundary` parameter is missing or set to `None`,
        it will be set as a named parameter with the column name.

        :param column: column name
        :param boundary: boundary
        :return: self
        """
        if boundary is None:
            replacement = f"{column} <= ?"
        else:
            replacement = f"{column} <= {boundary}"

        statements = self._prepare_column_clause(column)
        self._current_statements = \
            [self._replace(s, column, replacement) for s in statements]

        return self

    def higher_than(self, column: str, boundary: int or float = None):
        """Select a maximum value to query on a specified column.

        This column must be an optional column in the table yaml.

        If `boundary` parameter is missing or set to `None`,
        it will be set as a named parameter with the column name.

        :param column: column name
        :param boundary: boundary
        :return: self
        """
        if boundary is None:
            replacement = f"{column} > ?"
        else:
            replacement = f"{column} > {boundary}"

        statements = self._prepare_column_clause(column)
        self._current_statements = \
            [self._replace(s, column, replacement) for s in statements]

        return self

    def higher_or_equal_than(self, column: str, boundary: int or float = None):
        """Select a maximum values to query on a specified column.

        This column must be an optional column in the table yaml.

        If `boundary` parameter is missing or set to `None`,
        it will be set as a named parameter with the column name.

        :param column: column name
        :param boundary: boundary
        :return: self
        """
        if boundary is None:
            replacement = f"{column} >= ?"
        else:
            replacement = f"{column} >= {boundary}"

        statements = self._prepare_column_clause(column)
        self._current_statements = \
            [self._replace(s, column, replacement) for s in statements]

        return self

    def limit(self, value: int):
        """Limit number of results returned by the query.

        :param value: number of results
        :return: self
        """
        replacement = f"LIMIT {value}"
        self._current_statements = [self._replace(s, 'limit', replacement)
                                    for s in self._current_statements]

        return self


class CassandraMaterializedView(CassandraTable):

    @property
    def query_name(self):
        """Returns the table name."""
        return self._query_name

    def __init__(
        self,
        config: dict,
        query_name: str,
        keyspace: CassandraKeyspace = None,
        cassandra_manager: CassandraManager = None
    ):
        """Cassandra materialized view constructor.

        :param config: table template configuration
        :param query_name: materialized view name
        :param keyspace: materialized view keyspace
        :param cassandra_manager: Cassandra manager (default: None)
        """
        if query_name not in config['query']:
            raise QueryNotFound(query_name)

        super().__init__(config, keyspace, cassandra_manager)

        self._query_name = query_name

    def create(
        self,
        keyspace: str or List[str] = None,
        gc_grace_seconds: int = 86400,
        if_not_exists=False,
        **kwargs
    ):
        """Create materialized view statement.

        :param keyspace: keyspace name.
            This may be an str or List[str]  (default: None)
        :param gc_grace_seconds: garbage collect grace seconds.
            Defines the time in seconds that a tombstone is kept before being
            discarded.
        :param if_not_exists: if True, creates table if it does not exist.
            When set to False and the table exists, an exception is thrown.
        :return: self
        """
        self._current_operation = 'create'
        query = self._config['query'][self.query_name]

        keyspaces = keyspace
        if keyspaces is None:
            keyspaces = [self.keyspace_name]
        elif isinstance(keyspace, str):
            keyspaces = [keyspace]

        self._current_statements = []
        for keyspace_name in keyspaces:
            statement = f"CREATE MATERIALIZED VIEW "

            if if_not_exists:
                statement += "IF NOT EXISTS "

            statement += f"{keyspace_name}.{self.name}"
            if self.has_split():
                statement += "_{date}"
            statement += \
                f"_{self.query_name} AS SELECT * FROM {keyspace_name}.{self.name}"

            if self.has_split():
                statement += "_{date}"

            required_keys = list(query['required'].values())
            if 'optional' in query:
                required_keys += query['optional']

            statement += " WHERE "
            for index, key in enumerate(required_keys):
                if index == 0:
                    statement += f"{key} IS NOT NULL "
                else:
                    statement += f"AND {key} IS NOT NULL "

            primary_keys = ', '.join([pk for _, pk in query['required'].items()])
            statement += f"PRIMARY KEY ( ({primary_keys})"

            if 'optional' in query:
                clustering_keys = ', '.join([ck for ck in query['optional']])
                statement += f", {clustering_keys} ) "
            else:
                statement += " ) "

            statement += "WITH "

            if 'order' in query:
                statement += f"CLUSTERING ORDER BY ( "
                for index, (column, order) in enumerate(query['order'].items()):
                    if index == 0:
                        statement += f"{column} {order.upper()}"
                    else:
                        statement += f", {column} {order.upper()}"

                statement += " ) AND "

            statement += f"gc_grace_seconds={gc_grace_seconds};"

            self._current_statements.append(statement)
        return self

    def drop(
        self,
        keyspace: str or List[str] = None,
        if_exists: bool = False,
        **kwargs
    ):
        """Drop materialized view statement.

        :param keyspace: keyspace name.
            This may be an str or List[str]  (default: None)
        :param if_exists: if True, drops table if it exist.
            When set to False and the table does not exist,
            an exception is thrown (default: False)
        :return: self
        """
        self._current_operation = 'drop'

        keyspaces = keyspace
        if keyspaces is None:
            keyspaces = [self.keyspace_name]
        elif isinstance(keyspace, str):
            keyspaces = [keyspace]

        self._current_statements = []
        for keyspace_name in keyspaces:
            statement = f"DROP MATERIALIZED VIEW "
            if if_exists:
                statement += "IF EXISTS "

            statement += f"{keyspace_name}.{self.name}"
            if self.has_split():
                statement += "_{date}"
            statement += f"_{self.query_name};"
            self._current_statements.append(statement)

        return self

    def insert(self, **kwargs):
        """Raises exception. Materialized views do not support inserts."""
        raise Exception("Materialized views do not support inserts.")

    def query(self, keyspace: str or List[str] = None, **kwargs):
        """Query statement.

        This method must be chained with the methods required to define
        the primary and clustering keys, and finalizing with
        :func:`~table.CassandraTable.execute`

        :param keyspace: keyspace name.
            This may be an str or List[str]  (default: None)
        :return: self
        """
        self._current_operation = 'query'
        self._current_query = self.query_name

        keyspaces = keyspace
        if keyspaces is None:
            keyspaces = [self.keyspace_name]
        elif isinstance(keyspace, str):
            keyspaces = [keyspace]

        self._current_statements = []
        for keyspace_name in keyspaces:
            statement = (
                "SELECT {columns} FROM "
                f"{keyspace_name}.{self.name}"
            )
            if self.has_split():
                statement += "_{date}"
            statement += (
                f"_{self.query_name}"
                " {where} {limit} ;"
            )
            self._current_statements.append(statement)

        return self
