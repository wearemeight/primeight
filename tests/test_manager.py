import unittest
from unittest.mock import patch, call, MagicMock

from primeight.manager import \
    CassandraManager, \
    Cluster, ExecutionProfile, AddressTranslator, AuthProvider, \
    EXEC_PROFILE_DEFAULT, LoadBalancingPolicy, RetryPolicy, dict_factory, \
    concurrent


class CassandraManagerTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.contact_points = ['127.0.0.1']

    def test_create_execution_profile(self) -> None:
        mock_load_balancing_policy = MagicMock(spec=LoadBalancingPolicy)
        mock_retry_policy = MagicMock(spec=RetryPolicy)
        profile = CassandraManager.create_execution_profile(
            load_balancing_policy=mock_load_balancing_policy,
            retry_policy=mock_retry_policy,
            row_factory=dict_factory,
            consistency_level=1,
            request_timeout=5.0
        )

        self.assertEqual(
            mock_load_balancing_policy,
            profile.load_balancing_policy
        )
        self.assertEqual(mock_retry_policy, profile.retry_policy)
        self.assertEqual(1, profile.consistency_level)
        self.assertEqual(dict_factory, profile.row_factory)
        self.assertAlmostEqual(5.0, profile.request_timeout)
        mock_execution_profile = MagicMock(spec=ExecutionProfile)
        mock_address_translator = MagicMock(spec=AddressTranslator)
        mock_auth_provider = MagicMock(spec=AuthProvider)

        with patch.object(
                CassandraManager, 'create_execution_profile',
                return_value=mock_execution_profile
        ) as mock_create_execution_profile:
            cm = CassandraManager(
                self.contact_points,
                connect_timeout=10.0,
                control_connection_timeout=5.0,
                address_translator=mock_address_translator,
                auth_provider=mock_auth_provider
            )

            mock_create_execution_profile.assert_called_once()

        self.assertEqual(self.contact_points, cm.contact_points)
        self.assertIsInstance(cm.cluster, Cluster)
        self.assertEqual(10.0, cm.cluster.connect_timeout)
        self.assertEqual(5.0, cm.cluster.control_connection_timeout)
        self.assertIsNone(cm.session)
        self.assertEqual(mock_address_translator, cm.address_translator)
        self.assertEqual(
            {EXEC_PROFILE_DEFAULT: mock_execution_profile},
            cm.execution_profiles
        )

    def test_constructor_with_profiles(self) -> None:
        mock_execution_profiles = {'mock': MagicMock(spec=ExecutionProfile)}

        cm = CassandraManager(
            self.contact_points,
            profiles=mock_execution_profiles
        )

        self.assertEqual(self.contact_points, cm.contact_points)
        self.assertEqual(mock_execution_profiles, cm.execution_profiles)

    def test_connect(self) -> None:
        with patch.object(CassandraManager, 'create_execution_profile'):
            cassandra_manager = CassandraManager(self.contact_points)

        mock_session = MagicMock()
        with patch.object(Cluster, 'connect',
                          return_value=mock_session) as mock_connect:
            cassandra_manager.connect()

            mock_connect.assert_called_once_with(keyspace=None)
        self.assertEqual(mock_session, cassandra_manager.session)

    def test_connect_with_keyspace(self) -> None:
        with patch.object(CassandraManager, 'create_execution_profile'):
            cassandra_manager = CassandraManager(self.contact_points)

        mock_session = MagicMock()
        with patch.object(Cluster, 'connect',
                          return_value=mock_session) as mock_connect:
            cassandra_manager.connect(keyspace='mock_keyspace')

            mock_connect.assert_called_once_with(keyspace='mock_keyspace')
        self.assertEqual(mock_session, cassandra_manager.session)

    def test_execute(self) -> None:
        with patch.object(CassandraManager, 'create_execution_profile'):
            cassandra_manager = CassandraManager(self.contact_points)

        mock_statements = [f'mock_statement_{i}' for i in range(10)]
        mock_result = [{'mock_col': 'mock_val'}]

        mock_session = MagicMock()
        mock_session.execute = MagicMock(return_value=mock_result)
        with patch.object(Cluster, 'connect', return_value=mock_session):
            cassandra_manager.connect()

        result = cassandra_manager \
            .execute(mock_statements, execution_profile='mock_profile')

        calls = \
            [call(s, execution_profile='mock_profile') for s in mock_statements]
        mock_session.execute.assert_has_calls(calls)

        self.assertEqual(mock_result * 10, result)

    def test_execute_concurrent(self) -> None:
        with patch.object(CassandraManager, 'create_execution_profile'):
            cassandra_manager = CassandraManager(self.contact_points)

        mock_statements = [f'mock_statement_{i}' for i in range(10)]
        mock_result = \
            [(True, [{'mock_col': 'mock_val'}]), (False, "mock_error")]

        mock_session = MagicMock()
        with patch.object(Cluster, 'connect', return_value=mock_session):
            cassandra_manager.connect()

        mock_statements_and_params = [(s, ()) for s in mock_statements]
        with patch.object(concurrent, 'execute_concurrent',
                          return_value=mock_result) as mock_execute_concurrent:
            result = cassandra_manager.execute_concurrent(mock_statements)

            mock_execute_concurrent.assert_called_once_with(
                mock_session,
                mock_statements_and_params,
                raise_on_first_error=False
            )

        self.assertEqual([{'mock_col': 'mock_val'}], result)

    def test_close(self) -> None:
        with patch.object(CassandraManager, 'create_execution_profile'):
            cassandra_manager = CassandraManager(self.contact_points)

        with patch.object(Cluster, 'shutdown') as mock_shutdown:
            cassandra_manager.close()

            mock_shutdown.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
