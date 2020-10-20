import uuid
import logging
from typing import List, Tuple, Set


logger = logging.getLogger(__name__)


class CassandraColumn:

    NATIVE_TYPES = [
        'ascii', 'bigint', 'blob', 'boolean', 'counter', 'decimal', 'double',
        'float', 'inet', 'int', 'smallint', 'tinyint', 'text', 'date', 'time',
        'timestamp', 'timeuuid', 'duration', 'uuid', 'varchar', 'varint', 'h3hex'
    ]
    COLLECTIONS = ['list', 'set', 'map']
    OTHER_TYPES = ['tuple']

    HANDLE_TO_TYPE = {
        'ascii': str,
        'bigint': int,
        'blob': bytes,
        'boolean': bool,
        'counter': int,
        'decimal': float,
        'double': float,
        'float': float,
        'inet': str,
        'int': int,
        'smallint': int,
        'tinyint': int,
        'text': str,
        'date': str,
        'time': int,
        'timestamp': int,
        'timeuuid': uuid.UUID,
        'duration': str,
        'uuid': uuid.UUID,
        'varchar': str,
        'varint': int,
        'h3hex': str
    }

    HANDLE_TO_CASSANDRA_TYPE = {
        'ascii': 'ASCII',
        'bigint': 'BIGINT',
        'blob': 'BLOB',
        'boolean': 'BOOLEAN',
        'counter': 'COUNTER',
        'decimal': 'DECIMAL',
        'double': 'DOUBLE',
        'float': 'FLOAT',
        'inet': 'INET',
        'int': 'INT',
        'smallint': 'SMALLINT',
        'tinyint': 'TINYINT',
        'text': 'TEXT',
        'date': 'DATE',
        'time': 'TIME',
        'timestamp': 'BIGINT',
        'timeuuid': 'TIMEUUID',
        'duration': 'DURATION',
        'uuid': 'UUID',
        'varchar': 'VARCHAR',
        'varint': 'VARINT',
        'h3hex': 'TEXT'
    }

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type_handle

    @property
    def min_value(self):
        if self._min is None:
            logger.debug("Column %s has no min value defined.", self.name)

        return self._min

    @property
    def max_value(self):
        if self._max is None:
            logger.debug("Column %s has no max value defined.", self.name)

        return self._max

    @property
    def alias(self):
        if self._alias is None:
            logger.debug("Column %s has no alias defined.", self.name)

        return self._alias

    @property
    def description(self):
        if self._description is None:
            logger.debug("Column %s has no alias defined.", self.name)

        return self._description

    def __init__(
        self,
        name: str,
        type_handle: str,
        alias: str = None,
        description: str = None,
        min_value: int or float = None,
        max_value: int or float = None
    ):
        """Cassandra column constructor.

        :param name: column name
        :param type_handle: column type handle
        :param alias: column alias to the name (default: None)
        :param description: column description (default: None)
        :param min_value: column minimum value.
            This is used to check for invalid data (default: None)
        :param max_value: column maximum value.
            This is used to check for invalid data (default: None)
        """
        self._name = name.lower()
        self._type_handle = type_handle.lower()

        self._min = min_value
        self._max = max_value
        self._alias = alias
        self._description = description

    def pydantic_type(self, handle=None):
        if handle is None:
            handle = self._type_handle

        if handle.startswith('list'):
            h = handle \
                .replace('list<', '', 1) \
                .replace('>', '')
            content = self.pydantic_type(handle=h)

            return List[content]
        elif handle.startswith('tuple'):
            handle = handle \
                .replace('tuple<', '', 1) \
                .replace('>', '') \
                .replace(' ', '')
            content = tuple(self.HANDLE_TO_TYPE[h] for h in handle.split(','))

            return Tuple[content]
        elif handle.startswith('set'):
            h = handle \
                .replace('set<', '', 1) \
                .replace('>', '')
            content = self.pydantic_type(handle=h)

            return Set[content]
        else:
            return self.HANDLE_TO_TYPE[handle]

    @staticmethod
    def _handle_cassandra_frozen(handle):
        return f"FROZEN<{handle}>"

    def _handle_cassandra_list_type(self, handle, frozen=True):
        handle = handle \
            .lower() \
            .replace(' ', '') \
            .replace('list<', '', 1) \
            .strip('>')

        contents = self.cassandra_type(handle, frozen=frozen)
        type_handle = f'LIST<{contents}>'
        if frozen:
            type_handle = CassandraColumn._handle_cassandra_frozen(type_handle)

        return type_handle

    def _handle_cassandra_set_type(self, handle, frozen=True):
        handle = handle \
            .lower() \
            .replace(' ', '') \
            .replace('set<', '', 1) \
            .strip('>')

        type_handle = f'SET<{self.cassandra_type(handle)}>'
        if frozen:
            type_handle = self._handle_cassandra_frozen(type_handle)

        return type_handle

    def _handle_cassandra_tuple_type(self, handle, frozen=True):
        handle = handle \
            .lower() \
            .replace(' ', '') \
            .replace('tuple<', '', 1) \
            .strip('>')

        content = ','.join([self.cassandra_type(t) for t in handle.split(',')])
        type_handle = f'TUPLE<{content}>'
        if frozen:
            type_handle = self._handle_cassandra_frozen(type_handle)

        return type_handle

    def _handle_cassandra_map_type(self, handle, frozen=True):
        handle = handle \
            .lower() \
            .replace(' ', '') \
            .replace('map<', '', 1) \
            .strip('>')

        content = ','.join([self.cassandra_type(t) for t in handle.split(',')])
        type_handle = f'MAP<{content}>'
        if frozen:
            type_handle = self._handle_cassandra_frozen(type_handle)

        return type_handle

    def cassandra_type(self, handle: str = None, frozen: bool = True) -> str:
        type_handle = handle or self._type_handle

        if type_handle.startswith('list'):
            return self._handle_cassandra_list_type(type_handle, frozen=frozen)
        elif type_handle.startswith('set'):
            return self._handle_cassandra_set_type(type_handle, frozen=frozen)
        elif type_handle.startswith('map'):
            return self._handle_cassandra_map_type(type_handle, frozen=frozen)
        elif type_handle.startswith('tuple'):
            return self._handle_cassandra_tuple_type(type_handle, frozen=frozen)
        else:
            return self.HANDLE_TO_CASSANDRA_TYPE[type_handle]

    def is_valid(self, value):
        """Returns True if value is valid, and False otherwise."""

        if self.max_value is not None and value > self.max_value:
            logger.debug("value '%s' is above '%s'", value, self.max_value)
            return False

        if self.min_value is not None and value < self.min_value:
            logger.debug("value '%s' is below '%s'", value, self.min_value)
            return False

        return True
