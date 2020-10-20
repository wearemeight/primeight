from typing import List

from cassandra.cluster import ExecutionProfile

from primeight import CassandraManager


class CassandraBase:

    @property
    def cassandra_manager(self) -> CassandraManager:
        """Returns the Cassandra manager."""
        return self._cassandra_manager

    @property
    def config(self) -> dict:
        """Returns the table config."""
        return self._config

    def __init__(
        self, config: dict, cassandra_manager: CassandraManager = None
    ):
        """Cassandra base constructor.

        :param config: table template configuration
        :param cassandra_manager: cassandra manager
        """
        self._config = config
        self._cassandra_manager = cassandra_manager

        self._current_statements = None

    def execute(
            self, execution_profile: str or ExecutionProfile = None
    ) -> List[tuple] or List[dict]:
        """Execute list of query statements sequentially.

        :param execution_profile: execution profile (default: None)
            This parameter can be both the name of a configured profile,
            or the execution profile itself.
        :return: list of rows as formatted by the rows_factory
            in the execution profile
        """
        result = self.cassandra_manager.execute(self.statements, execution_profile)

        return result

    def execute_concurrent(
        self,
        raise_on_first_error: bool = False
    ) -> List[tuple] or List[dict]:
        """Execute list of query statements concurrently.

        This method does not allow to specify the execution profile to use,
        it is thus encouraged to set the desired execution profile as default
        when creating the CassandraManager object. You can do this with the
        :func:`~manager.CassandraManager.create_default_execution_profile`
        method.

        :param raise_on_first_error: raise exception on first error
            or continue and log possible errors (default: True)
        :return: list of rows as formatted by the rows_factory
            in the execution profile
        """
        result = self.cassandra_manager.execute_concurrent(
            self.statements, raise_on_first_error
        )

        return result
