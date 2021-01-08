import re
import warnings
import logging
from pathlib import Path

from primeight.column import CassandraColumn
from primeight.generators import Generators


logger = logging.getLogger(__name__)


class Parser:

    _required_fields = ['version', 'keyspace', 'name', 'columns', 'query']
    _optional_fields = ['generated_columns', 'split', 'lifetime', 'backup']

    _recognized_splits = ['day', 'week', 'month', 'year']

    _required_query_fields = ['required']
    _optional_query_fields = ['optional', 'order', 'description']
    _recognized_query_required = ['time', 'space', 'id']

    _recognized_orders = ['asc', 'desc']

    _recognized_types = [k.lower() for k in CassandraColumn.HANDLE_TO_CASSANDRA_TYPE.keys()]

    @staticmethod
    def parse(path: str or Path) -> dict:
        pass

    @staticmethod
    def is_valid_name(name: str) -> bool:
        """Validate if a name is valid."""
        valid_name_re = r'^[a-z]+([a-z]*[0-9]*_?)*$'
        cassandra_reserved_keywords = [
            'ADD', 'AGGREGATE', 'ALL', 'ALLOW', 'ALTER', 'AND', 'ANY', 'APPLY',
            'AS', 'ASC', 'ASCII', 'AUTHORIZE', 'BATCH', 'BEGIN', 'BIGINT',
            'BLOB', 'BOOLEAN', 'BY', 'CLUSTERING', 'COLUMNFAMILY', 'COMPACT',
            'CONSISTENCY', 'COUNT', 'COUNTER', 'CREATE', 'CUSTOM', 'DECIMAL',
            'DELETE', 'DESC', 'DISTINCT', 'DOUBLE', 'DROP', 'EACH_QUORUM',
            'ENTRIES', 'EXISTS', 'FILTERING', 'FLOAT', 'FROM', 'FROZEN', 'FULL',
            'GRANT', 'IF', 'IN', 'INDEX', 'INET', 'INFINITY', 'INSERT', 'INT',
            'INTO', 'KEY', 'KEYSPACE', 'KEYSPACES', 'LEVEL', 'LIMIT', 'LIST',
            'LOCAL_ONE', 'LOCAL_QUORUM', 'MAP', 'MATERIALIZED', 'MODIFY', 'NAN',
            'NORECURSIVE', 'NOSUPERUSER', 'NOT', 'OF', 'ON', 'ONE', 'ORDER',
            'PARTITION', 'PASSWORD', 'PER', 'PERMISSION', 'PERMISSIONS',
            'PRIMARY', 'QUORUM', 'RENAME', 'REVOKE', 'SCHEMA', 'SELECT', 'SET',
            'STATIC', 'STORAGE', 'SUPERUSER', 'TABLE', 'TEXT', 'TIME',
            'TIMESTAMP', 'TIMEUUID', 'THREE', 'TO', 'TOKEN', 'TRUNCATE', 'TTL',
            'TUPLE', 'TWO', 'TYPE', 'UNLOGGED', 'UPDATE', 'USE', 'USER',
            'USERS', 'USING', 'UUID', 'VALUES', 'VARCHAR', 'VARINT', 'VIEW',
            'WHERE', 'WITH', 'WRITETIME'
        ]

        if re.match(valid_name_re, name) is None:
            return False

        if name.upper() in cassandra_reserved_keywords:
            return False

        return True

    @staticmethod
    def is_valid_version(version: str) -> bool:
        """Validate if a version is valid."""
        valid_version_re = r'[0-9](.[0-9]+){0,2}'

        if re.match(valid_version_re, version) is None:
            return False

        return True

    @staticmethod
    def is_valid_config(content: dict) -> bool:
        """Validates Yaml has the correct format.
        Throws SyntaxError if Yaml has a wrong format.

        We are very strict with the Yaml format.
        So, wrong format may be caused by:
        * missing required field
        * unrecognized field
        * column name with invalid characters
        * unrecognized column type
        * unrecognized generator
        * generated columns dependent on undeclared columns
        * unrecognized backup strategy
        * unrecognized split strategy
        * non integer lifetime
        * negative lifetime
        * query name with invalid characters
        * query unrecognized field
        * query required field keys not valid
        * undeclared columns as query required or optional columns

        #TODO: improve docstring
        """
        # Validate that all required fields are present.
        missing_fields = [f for f in Parser._required_fields if f not in content]
        if len(missing_fields) > 0:
            missing_fields_str = ', '.join(missing_fields)
            raise SyntaxError(f"Missing required fields: {missing_fields_str}")

        # Validate that the table has a valid name.
        if not Parser.is_valid_name(content['name']):
            raise SyntaxError(
                f"Table name '{content['name']}' is invalid")

        # Validate that, if present, keyspace has a valid name.
        if 'keyspace' in content and not Parser.is_valid_name(content['keyspace']):
            raise SyntaxError(
                f"Keyspace '{content['keyspace']}' is invalid")

        # Validate version.
        if not Parser.is_valid_version(content['version']):
            raise SyntaxError(
                f"Table version '{content['version']}' is invalid")

        all_fields = Parser._required_fields + Parser._optional_fields

        # Validate that no unrecognized field exist.
        unrecognized_fields = [f for f in content if f not in all_fields]
        if len(unrecognized_fields) > 0:
            unrecognized_fields_str = ', '.join(unrecognized_fields)
            raise SyntaxError(f"Unrecognized fields: {unrecognized_fields_str}")

        # Validate that all columns have recognized types.
        if len('columns') == 0:
            raise SyntaxError("columns list is empty")

        # Validate keyspace partition.
        # If no keyspace is declared, then a client_id column is required,
        # and vice-versa.
        if ('keyspace' not in content
                and 'client_id' not in content['columns']):
            raise SyntaxError("column 'client_id' is required in "
                              "when keyspace is not defined.")

        # Validate that client_id column has a type.
        elif ('client_id' in content['columns']
                and 'type' not in content['columns']['client_id']):
            raise SyntaxError(f"Missing type for column client_id")

        # Validate that the client_id column has type text.
        elif ('client_id' in content['columns']
                and content['columns']['client_id']['type'] != 'text'):
            raise SyntaxError(
                f"column client_id has wrong type, must be text")

        for name, col_content in content['columns'].items():

            # Validate that the columns has type.
            if 'type' not in col_content:
                raise SyntaxError(f"Missing type for column {name}")
            if col_content['type'] is None:
                raise SyntaxError(f"column {name} type is empty")

            t = col_content['type'] \
                .lower() \
                .replace(' ', '') \
                .replace('list<', '') \
                .replace('tuple<', '') \
                .replace('set<', '') \
                .replace('map<', '') \
                .strip('>')
            # Validate that the column type is recognized.
            if ',' in t:
                t_parts = t.split(',')
                for tp in t_parts:
                    if tp not in Parser._recognized_types:
                        raise SyntaxError(f"Unrecognized type '{tp}' for {name}")
            elif t not in Parser._recognized_types:
                raise SyntaxError(f"Unrecognized type '{t}' for {name}")

            # Validate that the column as a valid name.
            if not Parser.is_valid_name(name):
                raise SyntaxError(f"column '{name}' is invalid")
            # If column has a max value, validate that it is an int or float.
            if 'max' in col_content:
                try:
                    float(col_content['max'])
                except ValueError:
                    raise SyntaxError(f"{name} max value is not a number")
            # If column has a min value, validate that it is an int or float.
            if 'min' in col_content:
                try:
                    float(col_content['min'])
                except ValueError:
                    raise SyntaxError(f"{name} min value is not a number")

        generated_columns = []
        if 'generated_columns' in content:
            for generator, column in content['generated_columns'].items():
                # Validate that the generator is recognized.
                if generator not in Generators().names:
                    raise SyntaxError(f"Unrecognized generator '{generator}'")

                # Argument columns may be separated by a comma
                # to indicate a multivariate generator.
                if ',' in column:
                    columns = "".join(column.split()).split(',')
                    # Validate that all argument columns were declared.
                    for a in columns:
                        if a not in content['columns'].keys():
                            raise SyntaxError(f"Undeclared column '{a}'")
                else:
                    # Validate that the argument column was declared.
                    if column not in content['columns'].keys():
                        raise SyntaxError(f"Undeclared column '{column}'")

            generated_columns = list(content['generated_columns'])

            # Throw warning when h3 is defined but not h9.
            if ('h3' in generated_columns
                    and 'h9' not in generated_columns):
                warnings.warn("Without defining a h9 identifier, "
                              "no high accuracy queries can be made.",
                              RuntimeWarning, stacklevel=3)

        all_columns = (list(content['columns'].keys()) + generated_columns)

        # Validate if there are duplicate columns.
        for column in all_columns:
            if all_columns.count(column) > 1:
                raise SyntaxError(f"Query column {column} is duplicated")

        if 'backup' in content:
            # Validate that backup is not empty.
            if content['backup'] is None:
                raise SyntaxError("Backup not defined.")
            # Validate that backup is recognized.
            if content['backup'] not in Parser._recognized_splits:
                raise SyntaxError(f"Unrecognized backup '{content['backup']}'")

        if 'split' in content:
            # Validate that split is not empty.
            if content['split'] is None:
                raise SyntaxError("Split not defined.")
            # Validate that split is recognized.
            if content['split'] not in Parser._recognized_splits:
                raise SyntaxError(f"Unrecognized split '{content['split']}'")
            # Validate that split is a generated column.
            # This enforces consistency in the table names.
            if ('generated_columns' not in content
                    or content['split'] not in content['generated_columns']):
                raise SyntaxError(f"Split needs to be a generated column")

        if 'lifetime' in content:
            # Validate that lifetime is not empty.
            if content['lifetime'] is None:
                raise SyntaxError("Lifetime not defined.")
            # Validate that lifetime is an integer.
            try:
                v = int(content['lifetime'])
            except ValueError:
                raise SyntaxError(
                    f"Lifetime '{content['lifetime']}' is not an integer")
            # Validate that lifetime is positive.
            if v < 0:
                raise SyntaxError(f"Lifetime '{content['lifetime']}' is not positive")

        # Validate queries is not empty.
        if content['query'] is None:
            raise SyntaxError("Query not defined.")
        # Validate that base query is declared.
        if 'base' not in content['query']:
            raise SyntaxError(f"Query 'base' is required")
        # Validate that base query is not empty.
        if content['query']['base'] is None:
            raise SyntaxError("Base query not defined.")

        base = content['query']['base']
        # Validate that base query as all necessary fields.
        missing_fields = [f for f in Parser._required_query_fields if f not in base]
        if len(missing_fields) > 0:
            missing_fields_str = ', '.join(missing_fields)
            raise SyntaxError(
                f"Base query missing required fields: {missing_fields_str}")
        # Validate that base query required field is not empty.
        if base['required'] is None:
            raise SyntaxError("Base query required columns not defined.")

        base_keys = list(base['required'].values())
        if 'optional' in base:
            base_keys += base['optional']

        for name, query in content['query'].items():
            # Validate that query has valid name.
            if not Parser.is_valid_name(name):
                raise SyntaxError(f"Query name '{name}' is invalid")

            # Validate that query has all required fields.
            missing_fields = [f for f in Parser._required_query_fields if f not in query]
            if len(missing_fields) > 0:
                missing_fields_str = ', '.join(missing_fields)
                raise SyntaxError(
                    f"{name} query missing required fields: "
                    f"{missing_fields_str}")
            # Validate that query required field is not empty.
            if query['required'] is None:
                raise SyntaxError(f"{name} query required columns "
                                  f"not defined.")
            # Validate that, if present, the optional field is not empty.
            if 'optional' in query and query['optional'] is None:
                raise \
                    SyntaxError(f"{name} query optional columns not defined")

            all_fields = Parser._required_query_fields + Parser._optional_query_fields

            # Validate that there are no unrecognized fields in query.
            unrecognized_fields = [f for f in query if f not in all_fields]
            if len(unrecognized_fields) > 0:
                unrecognized_fields_str = ', '.join(unrecognized_fields)
                raise SyntaxError(f"{name} query unrecognized fields: "
                                  f"{unrecognized_fields_str}")

            partition_columns = []
            for field, column in query['required'].items():
                # Validate that query required does not match split partition.
                # If it matched then the required field would be redundant.
                if 'split' in content and column == content['split']:
                    raise SyntaxError(
                        f"Query required field can not match split")
                # Validate that query required columns are declared as
                # the base query required or optional columns.
                # This is required by Cassandra.
                if column not in base_keys:
                    raise SyntaxError(f"{name} query required column "
                                      f"'{column}' must be a "
                                      "'base' query key, required or optional")
                # Validate that query required type is recognized.
                if field not in Parser._recognized_query_required:
                    raise SyntaxError(
                        f"{name} query unrecognized required field '{field}'")
                # Validate that query required column is declared.
                if column not in all_columns:
                    raise SyntaxError(
                        f"{name} query unrecognized required column "
                        f"'{column}'")

                partition_columns.append(column)

            if 'optional' in query:
                # Validate that query optional field is not empty.
                if query['optional'] is None:
                    raise SyntaxError(f"{name} query optional "
                                      f"columns not defined")

                for column in query['optional']:
                    # Validate that query optional column is declared.
                    if column not in all_columns:
                        raise SyntaxError(
                            f"{name} query undeclared required column "
                            f"'{column}'"
                        )

                    partition_columns.append(column)

            if 'order' in query:
                # Validate that order is not empty.
                if query['order'] is None:
                    raise SyntaxError(f"{name} query columns order "
                                      "not defined")

                for column, order in query['order'].items():
                    # Validate that order is a clustering key in query.
                    if column not in partition_columns:
                        raise SyntaxError(f"Order column {column} "
                                          "must be a clustering key")
                    # Validate that order has a recognized value.
                    if order not in Parser._recognized_orders:
                        raise SyntaxError(f"Order value {order} is not valid, "
                                          f"use either 'asc' or 'desc'")
