import unittest

from cassandra.query import SimpleStatement

from primeight.keyspace import CassandraKeyspace


class CassandraKeyspaceTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.mock_config = {
            'name': 'mock_table',
            'columns': {
                'col1': {'type': 'text'},
                'col2': {'type': 'timestamp'},
                'col3': {'type': 'float'},
                'col4': {'type': 'float'},
                'col5': {'type': 'smallint'}
            },
            'generated_columns': {
                'day': 'col2',
                'h3': 'col3,col4'
            },
            'query': {
                'base': {
                    'required': {'id': 'col1'},
                    'optional': ['col2']
                }
            }
        }

    def test_name_with_keyspace(self) -> None:
        self.mock_config['keyspace'] = 'mock_keyspace'
        keyspace = CassandraKeyspace(self.mock_config)

        self.assertEqual('mock_keyspace', keyspace.name)

    def test_name_raises_value_error(self) -> None:
        keyspace = CassandraKeyspace(self.mock_config)
        with self.assertRaises(ValueError):
            _ = keyspace.name

    def test_create(self) -> None:
        keyspace = \
            CassandraKeyspace(self.mock_config) \
            .create('mock_keyspace')

        self.assertEqual(1, len(keyspace.statements))
        self.assertIsInstance(keyspace.statements[0], SimpleStatement)
        self.assertEqual(
            "CREATE KEYSPACE mock_keyspace "
            "WITH replication={ "
            "'class' : 'SimpleStrategy' , "
            "'replication_factor': 3 "
            "};",
            keyspace.statements[0].query_string
        )

        keyspace = \
            CassandraKeyspace(self.mock_config) \
            .create(
                'mock_keyspace',
                replication_strategy='NetworkTopologyStrategy',
                replication_factor={'eu-central': 3, 'us-east': 2}
            )

        self.assertEqual(1, len(keyspace.statements))
        self.assertIsInstance(keyspace.statements[0], SimpleStatement)
        self.assertEqual(
            "CREATE KEYSPACE mock_keyspace "
            "WITH replication={ "
            "'class' : 'NetworkTopologyStrategy' , "
            "'eu-central': 3 , 'us-east': 2 "
            "};",
            keyspace.statements[0].query_string
        )

    def test_create_if_not_exists(self) -> None:
        keyspace = \
            CassandraKeyspace(self.mock_config) \
            .create('mock_keyspace', if_not_exists=True)

        self.assertEqual(1, len(keyspace.statements))
        self.assertIsInstance(keyspace.statements[0], SimpleStatement)
        self.assertEqual(
            "CREATE KEYSPACE IF NOT EXISTS mock_keyspace "
            "WITH replication={ "
            "'class' : 'SimpleStrategy' , "
            "'replication_factor': 3 "
            "};",
            keyspace.statements[0].query_string
        )

        keyspace = \
            CassandraKeyspace(self.mock_config) \
            .create(
                'mock_keyspace',
                replication_strategy='NetworkTopologyStrategy',
                replication_factor={'eu-central': 3, 'us-east': 2},
                if_not_exists=True
            )

        self.assertEqual(1, len(keyspace.statements))
        self.assertIsInstance(keyspace.statements[0], SimpleStatement)
        self.assertEqual(
            "CREATE KEYSPACE IF NOT EXISTS mock_keyspace "
            "WITH replication={ "
            "'class' : 'NetworkTopologyStrategy' , "
            "'eu-central': 3 , 'us-east': 2 "
            "};",
            keyspace.statements[0].query_string
        )

    def test_drop(self) -> None:
        keyspace = \
            CassandraKeyspace(self.mock_config) \
            .drop('mock_keyspace')

        self.assertEqual(1, len(keyspace.statements))
        self.assertIsInstance(keyspace.statements[0], SimpleStatement)
        self.assertEqual(
            "DROP KEYSPACE mock_keyspace ;",
            keyspace.statements[0].query_string
        )

    def test_drop_if_exists(self) -> None:
        keyspace = \
            CassandraKeyspace(self.mock_config) \
            .drop('mock_keyspace', if_exists=True)

        self.assertEqual(1, len(keyspace.statements))
        self.assertIsInstance(keyspace.statements[0], SimpleStatement)
        self.assertEqual(
            "DROP KEYSPACE IF EXISTS mock_keyspace ;",
            keyspace.statements[0].query_string
        )

    def test_alter(self) -> None:
        keyspace = \
            CassandraKeyspace(self.mock_config) \
            .alter('mock_keyspace')

        self.assertEqual(1, len(keyspace.statements))
        self.assertIsInstance(keyspace.statements[0], SimpleStatement)
        self.assertEqual(
            "ALTER KEYSPACE mock_keyspace "
            "WITH replication={ "
            "'class' : 'SimpleStrategy' , "
            "'replication_factor': 3 "
            "};",
            keyspace.statements[0].query_string
        )

        keyspace = \
            CassandraKeyspace(self.mock_config) \
            .alter(
                'mock_keyspace',
                replication_strategy='NetworkTopologyStrategy',
                replication_factor={'eu-central': 3, 'us-east': 2}
            )

        self.assertEqual(1, len(keyspace.statements))
        self.assertIsInstance(keyspace.statements[0], SimpleStatement)
        self.assertEqual(
            "ALTER KEYSPACE mock_keyspace "
            "WITH replication={ "
            "'class' : 'NetworkTopologyStrategy' , "
            "'eu-central': 3 , 'us-east': 2 "
            "};",
            keyspace.statements[0].query_string
        )


if __name__ == '__main__':
    unittest.main()
