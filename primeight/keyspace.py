from typing import List, Dict

from cassandra.query import Statement, SimpleStatement

from primeight.base import CassandraBase
from primeight.manager import CassandraManager


class CassandraKeyspace(CassandraBase):
    """Cassandra keyspace class.
    This class incorporates all the information and operations
    allowed over keyspaces.

    """

    @property
    def config(self) -> dict:
        """Returns template configuration."""
        return self._config

    @property
    def statements(self) -> List[str] or List[Statement]:
        """Returns Cassandra statements."""
        if self._current_statements is None:
            return []

        return self._current_statements

    @property
    def name(self) -> str:
        """Returns keyspace name."""
        if 'keyspace' not in self.config:
            raise ValueError("Missing keyspace name.")
        return self.config['keyspace']

    def __init__(self, config: dict, cassandra_manager: CassandraManager = None):
        """Cassandra keyspace constructor.

        :param config: table template configuration
        :param cassandra_manager: cassandra manager
        """
        super().__init__(config, cassandra_manager)

        self._config = config
        self._current_statements = []

    def create(
        self,
        name: str or List[str] = None,
        replication_strategy='SimpleStrategy',
        replication_factor: int or Dict[str, int] = 3,
        if_not_exists=False
    ):
        """Create new keyspace statement.

        When using the `SimpleStrategy` replication strategy,
        `replication_factor` should be an integer.
        When using the `NetworkTopologyStrategy` replication strategy,
        `replication_factor` should be a dictionary mapping data center names
        to an integer value representing the replication factor in that
        data center.

        :param name: keyspace name.
            This may be an str or List[str]  (default: None)
        :param replication_strategy: keyspace replication strategy
            (default: SimpleStrategy)
        :param replication_factor: keyspace replication factor (default: 3)
        :param if_not_exists: if True, creates keyspace if it does not exist.
            When set to False and the keyspace exists, an exception is thrown.
            (default: False)
        :return: self
        """
        keyspaces = name
        if keyspaces is None:
            keyspaces = [self.name]
        elif isinstance(name, str):
            keyspaces = [name]

        self._current_statements = []
        for keyspace_name in keyspaces:
            statement = "CREATE KEYSPACE "
            if if_not_exists:
                statement += "IF NOT EXISTS "

            statement += (
                f"{keyspace_name} "
                "WITH replication={ "
                f"'class' : '{replication_strategy}' "
            )

            if isinstance(replication_factor, int):
                statement += f", 'replication_factor': {replication_factor} "
            elif isinstance(replication_factor, dict):
                for dc_name, repl_factor in replication_factor.items():
                    statement += f", '{dc_name}': {repl_factor} "
            else:
                raise Exception("replication_factor has wrong type")

            statement += "};"

            self._current_statements.append(SimpleStatement(statement))

        return self

    def drop(self, name: str or List[str] = None, if_exists: bool = False):
        """Drop keyspace statement.

        :param name: keyspace name.
            This may be an str or List[str]  (default: None)
        :param if_exists: if True, drops keyspace if it exists.
            When set to False and the keyspace does not exist,
            an exception is thrown. (default: False)
        :return: self
        """
        keyspaces = name
        if keyspaces is None:
            keyspaces = [self.name]
        elif isinstance(name, str):
            keyspaces = [name]

        self._current_statements = []
        for keyspace_name in keyspaces:
            statement = f"DROP KEYSPACE "
            if if_exists:
                statement += "IF EXISTS "
            statement += f"{keyspace_name} ;"

            self._current_statements.append(SimpleStatement(statement))

        return self

    def alter(
        self,
        name: str or List[str] = None,
        replication_strategy='SimpleStrategy',
        replication_factor: int or Dict[str, int] = 3
    ):
        """Alter keyspace statement.

        When using the `SimpleStrategy` replication strategy,
        `replication_factor` should be an integer.
        When using the `NetworkTopologyStrategy` replication strategy,
        `replication_factor` should be a dictionary mapping data center names
        to an integer value representing the replication factor in that
        data center.

        :param name: keyspace name.
            This may be an str or List[str]  (default: None)
        :param replication_strategy: keyspace replication strategy
            (default: SimpleStrategy)
        :param replication_factor: keyspace replication factor (default: 3)
        :return: self
        """
        keyspaces = name
        if keyspaces is None:
            keyspaces = [self.name]
        elif isinstance(name, str):
            keyspaces = [name]

        self._current_statements = []
        for keyspace_name in keyspaces:
            statement = (
                f"ALTER KEYSPACE {keyspace_name} "
                "WITH replication={ "
                f"'class' : '{replication_strategy}' "
            )

            if isinstance(replication_factor, int):
                statement += f", 'replication_factor': {replication_factor} "
            elif isinstance(replication_factor, dict):
                for dc_name, repl_factor in replication_factor.items():
                    statement += f", '{dc_name}': {repl_factor} "
            else:
                raise Exception("replication_factor has wrong type")

            statement += "};"

            self._current_statements.append(SimpleStatement(statement))

        return self
