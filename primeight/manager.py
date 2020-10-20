import logging
from typing import List, Dict, Callable

from cassandra import concurrent, ConsistencyLevel
from cassandra.cluster import \
    Cluster, Session, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.query import dict_factory
from cassandra.auth import AuthProvider
from cassandra.policies import \
    LoadBalancingPolicy, RetryPolicy, RoundRobinPolicy, \
    AddressTranslator


class CassandraManager:
    """Cassandra Manager class.
    This class is responsible for creating and managing the cluster connection.

    """

    @property
    def contact_points(self) -> List[str]:
        return self._contact_points

    @property
    def cluster(self) -> Cluster:
        return self._cluster

    @property
    def session(self) -> Session:
        return self._session

    @property
    def execution_profiles(self) -> Dict[str, ExecutionProfile]:
        return self._execution_profiles

    @property
    def address_translator(self) -> AddressTranslator:
        return self._address_translator

    @staticmethod
    def create_execution_profile(
        load_balancing_policy: LoadBalancingPolicy,
        retry_policy: RetryPolicy,
        consistency_level: int = ConsistencyLevel.LOCAL_ONE,
        row_factory: Callable = dict_factory,
        request_timeout: float = 10.0
    ) -> ExecutionProfile:
        """Creates a Cassandra ExecutionProfile.

        :param load_balancing_policy: load balancing policy
        :param retry_policy: retry policy used when not set on execute
        :param consistency_level: consistency level used when not set on execute
        :param row_factory: row factory (default: dict_factory)
        :param request_timeout: request timeout (default: 10.0)
        :return: Cassandra execution profile
        """

        return ExecutionProfile(
            load_balancing_policy=load_balancing_policy,
            retry_policy=retry_policy,
            consistency_level=consistency_level,
            row_factory=row_factory,
            request_timeout=request_timeout
        )

    def __init__(
        self,
        contact_points: List[str],
        connect_timeout: float = 5.0,
        control_connection_timeout: float = 2.0,
        profiles: Dict[str, ExecutionProfile] = None,
        address_translator: AddressTranslator = None,
        auth_provider: AuthProvider = None
    ):
        """Cassandra Manager constructor.

        If profiles is not defined defaults to ExecutionProfile with:
          - cassandra.policies.RoundRobinPolicy load balancing policy
          - cassandra.policies.RetryPolicy retry policy
          - LOCAL_ONE consistency level
          - dict_factory row factory
          - 10.0 seconds request_timeout

        :param contact_points: list of Cassandra node ip addresses
        :type contact_points: list[str]
        :param control_connection_timeout: connect timeout (default: 5.0)
        :type control_connection_timeout: int
        :param profiles: execution profiles (defaults:
        :param address_translator: translator to be used in translating
            server node addresses to driver connection addresses (default: None)
        :param auth_provider: authentication provider (default: None)
        :type profiles: dict
        """
        self._contact_points = contact_points
        self._address_translator = address_translator
        self._session = None

        if profiles is None:
            self._execution_profiles = {
                EXEC_PROFILE_DEFAULT: self.create_execution_profile(
                    load_balancing_policy=RoundRobinPolicy(),
                    retry_policy=RetryPolicy()
                )
            }
        else:
            self._execution_profiles = profiles

        self._cluster = Cluster(
            contact_points=self.contact_points,
            connect_timeout=connect_timeout,
            control_connection_timeout=control_connection_timeout,
            execution_profiles=self.execution_profiles,
            address_translator=self.address_translator,
            auth_provider=auth_provider
        )

    def connect(self, keyspace: str = None):
        """Connects to the Cassandra cluster, creating a session.

        :param keyspace: keyspace name
        :return: self
        """
        self._session = self.cluster.connect(keyspace=keyspace)

        return self

    def execute(
        self,
        statements: List[str],
        execution_profile: str or ExecutionProfile = None
    ) -> List[tuple] or List[dict]:
        """Execute list of query statements sequentially.

        :param statements: list of query statements
        :param execution_profile: execution profile (default: None)
            This parameter can be both the name of a configured profile,
            or the execution profile itself.
        :return: list of rows as formatted by the rows_factory
            in the execution profile
        """
        result_list = []
        for statement in statements:
            if execution_profile is not None:
                result = self.session.execute(statement, execution_profile=execution_profile)
            else:
                result = self.session.execute(statement)

            result_list += [row for row in result]

        return result_list

    def execute_concurrent(
        self,
        statements: List[str],
        raise_on_first_error: bool = False
    ) -> List[tuple] or List[dict]:
        """Execute list of query statements concurrently.

        This method does not allow to specify the execution profile to use,
        it is thus encouraged to set the desired execution profile as default
        when creating the CassandraManager object. You can do this with the
        :func:`~manager.CassandraManager.create_default_execution_profile`
        method.

        :param statements: list of query statements
        :param raise_on_first_error: raise exception on first error
            or continue and log possible errors (default: True)
        :return: list of rows as formatted by the rows_factory
            in the execution profile
        """
        result_list = []

        statements_and_params = [(s, ()) for s in statements]
        query_results = concurrent.execute_concurrent(
            self.session,
            statements_and_params,
            raise_on_first_error=raise_on_first_error
        )

        for (success, result) in query_results:
            if not success:
                logging.error(f"A query failed with error: {result}")
            else:
                result_list += [row for row in result]

        return result_list

    def close(self) -> None:
        """Close Cassandra cluster connection."""
        self.cluster.shutdown()
