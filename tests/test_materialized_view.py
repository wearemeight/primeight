import unittest
from unittest.mock import patch
from datetime import datetime

from primeight import CassandraKeyspace, CassandraMaterializedView
from primeight.table import QueryNotFound


class CassandraMaterializedViewCase(unittest.TestCase):

    def setUp(self) -> None:
        self.mock_config = {
            'name': 'mock_table',
            'columns': {
                'col1': {'type': 'text', 'alias': 'ca'},
                'col2': {'type': 'timestamp', 'alias': 'cb'},
                'col3': {'type': 'float', 'alias': 'a4'},
                'col4': {'type': 'float', 'alias': 'ac'},
                'col5': {'type': 'smallint', 'alias': 'af'}
            },
            'generated_columns': {
                'day': 'col2',
                'h3': 'col3,col4'
            },
            'query': {
                'base': {
                    'required': {'id': 'col1'},
                    'optional': ['col2'],
                    'order': {'id': 'desc'}
                },
                'second': {
                    'required': {'time': 'col2'},
                    'optional': ['col1'],
                    'order': {'id': 'desc'}
                }
            }
        }

        self.name = 'second'
        self.keyspace = CassandraKeyspace(self.mock_config)

    def test_create(self) -> None:
        materialized_view = CassandraMaterializedView(
                self.mock_config, self.name, self.keyspace
            ) \
            .create(
                keyspace='mock_keyspace',
                gc_grace_seconds=1000,
                if_not_exists=False,
                create_materialized_views=False
            )
        self.assertEqual(1, len(materialized_view.statements))
        self.assertEqual(
            "CREATE MATERIALIZED VIEW mock_keyspace.mock_table_second "
            "AS SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 IS NOT NULL AND col1 IS NOT NULL "
            "PRIMARY KEY ( (col2), col1 ) "
            "WITH CLUSTERING ORDER BY ( id DESC ) "
            "AND gc_grace_seconds=1000;",
            materialized_view.statements[0]
        )

    def test_create_with_split(self) -> None:
        self.mock_config['split'] = 'year'
        self.mock_config['generated_columns'] = {
            'day': 'col2',
            'year': 'col2',
            'h3': 'col3,col4'
        }
        materialized_view = CassandraMaterializedView(
                self.mock_config, self.name, self.keyspace
            ) \
            .create(
                keyspace='mock_keyspace',
                gc_grace_seconds=1000,
                if_not_exists=False,
                create_materialized_views=False
            ) \
            .time(datetime(2019, 1, 1), datetime(2020, 12, 31))
        self.assertEqual(2, len(materialized_view.statements))
        self.assertEqual(
            "CREATE MATERIALIZED VIEW mock_keyspace.mock_table_2019_second "
            "AS SELECT * FROM mock_keyspace.mock_table_2019 "
            "WHERE col2 IS NOT NULL AND col1 IS NOT NULL "
            "PRIMARY KEY ( (col2), col1 ) "
            "WITH CLUSTERING ORDER BY ( id DESC ) "
            "AND gc_grace_seconds=1000;",
            materialized_view.statements[0]
        )
        self.assertEqual(
            "CREATE MATERIALIZED VIEW mock_keyspace.mock_table_2020_second "
            "AS SELECT * FROM mock_keyspace.mock_table_2020 "
            "WHERE col2 IS NOT NULL AND col1 IS NOT NULL "
            "PRIMARY KEY ( (col2), col1 ) "
            "WITH CLUSTERING ORDER BY ( id DESC ) "
            "AND gc_grace_seconds=1000;",
            materialized_view.statements[1]
        )

    def test_create_if_not_exists(self) -> None:
        materialized_view = CassandraMaterializedView(
                self.mock_config, self.name, self.keyspace
            ) \
            .create(
                keyspace='mock_keyspace',
                gc_grace_seconds=1000,
                if_not_exists=True,
                create_materialized_views=False
            )
        self.assertEqual(1, len(materialized_view.statements))
        self.assertEqual(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS "
            "mock_keyspace.mock_table_second "
            "AS SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 IS NOT NULL AND col1 IS NOT NULL "
            "PRIMARY KEY ( (col2), col1 ) "
            "WITH CLUSTERING ORDER BY ( id DESC ) "
            "AND gc_grace_seconds=1000;",
            materialized_view.statements[0]
        )

    def test_drop(self) -> None:
        materialized_view = CassandraMaterializedView(
                self.mock_config, self.name, self.keyspace
            ) \
            .drop(
                keyspace='mock_keyspace',
                if_exists=False,
                drop_materialized_views=False
            )
        self.assertEqual(1, len(materialized_view.statements))
        self.assertEqual(
            "DROP MATERIALIZED VIEW mock_keyspace.mock_table_second;",
            materialized_view.statements[0]
        )

    def test_drop_if_exists(self) -> None:
        materialized_views = CassandraMaterializedView(
                self.mock_config, self.name, self.keyspace
            ) \
            .drop(
                keyspace='mock_keyspace',
                if_exists=True,
                drop_materialized_views=False
            )
        self.assertEqual(1, len(materialized_views.statements))
        self.assertEqual(
            "DROP MATERIALIZED VIEW IF EXISTS mock_keyspace.mock_table_second;",
            materialized_views.statements[0]
        )

    def test_drop_with_split(self) -> None:
        self.mock_config['split'] = 'year'
        self.mock_config['generated_columns'] = {
            'day': 'col2',
            'year': 'col2',
            'h3': 'col3,col4'
        }
        materialized_view = CassandraMaterializedView(
                self.mock_config, self.name, self.keyspace
            ) \
            .drop(
                keyspace='mock_keyspace',
                if_exists=False,
                drop_materialized_views=False
            ) \
            .time(datetime(2019, 1, 1), datetime(2020, 12, 31))
        self.assertEqual(2, len(materialized_view.statements))
        self.assertEqual(
            "DROP MATERIALIZED VIEW mock_keyspace.mock_table_2019_second;",
            materialized_view.statements[0]
        )
        self.assertEqual(
            "DROP MATERIALIZED VIEW mock_keyspace.mock_table_2020_second;",
            materialized_view.statements[1]
        )

    def test_insert_raises_exception(self) -> None:
        materialized_view = CassandraMaterializedView(
            self.mock_config, self.name, self.keyspace
        )

        with self.assertRaises(Exception):
            materialized_view.insert(
                row={'col1': 'mock_id', 'col2': 1546304400000},
                keyspace='mock_keyspace'
            )

    def test_query(self) -> None:
        materialized_view = CassandraMaterializedView(
                self.mock_config, self.name, self.keyspace
            ) \
            .query(keyspace='mock_keyspace')

        self.assertEqual(1, len(materialized_view.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table_second   ;",
            materialized_view.statements[0]
        )

    def test_query_with_split(self) -> None:
        self.mock_config['split'] = 'day'
        materialized_view = CassandraMaterializedView(
                self.mock_config, self.name, self.keyspace
            ) \
            .query(keyspace='mock_keyspace')

        self.assertEqual(1, len(materialized_view.statements))
        self.assertIn('_{date}', materialized_view.statements[0])
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table_{date}_second   ;",
            materialized_view.statements[0]
        )

    def test_raises_query_not_found(self) -> None:
        with self.assertRaises(QueryNotFound):
            CassandraMaterializedView(self.mock_config, 'third', self.keyspace)


if __name__ == '__main__':
    unittest.main()
