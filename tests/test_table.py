import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

from pydantic import BaseModel

from primeight.keyspace import CassandraKeyspace
from primeight.table import \
    CassandraTable, \
    DateNotDefinedError, QueryNotFound, MissingColumnError


class CassandraTableCase(unittest.TestCase):

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
                }
            }
        }

        self.keyspace = CassandraKeyspace(self.mock_config)

    def test_model(self) -> None:
        class MockTable(BaseModel):
            col1: str = None
            col2: int = None
            col3: float = None
            col4: float = None
            col5: int = None
            day: int = None
            h3: str = None

        mock_keyspace = MagicMock(spec=CassandraKeyspace)
        model = CassandraTable(self.mock_config, keyspace=mock_keyspace).model
        self.assertEqual(MockTable.schema(), model.schema())

    def test_has_split(self) -> None:
        table = CassandraTable(self.mock_config, self.keyspace)
        self.assertFalse(table.has_split())

        self.mock_config['split'] = 'day'
        table = CassandraTable(self.mock_config, self.keyspace)
        self.assertTrue(table.has_split())

    def test_get_columns(self) -> None:
        table = CassandraTable(self.mock_config, self.keyspace)

        cols = table.get_columns(names=['col1', 'col2', 'day'])
        self.assertEqual(3, len(cols))

        cols = table.get_columns(names=['col4'])
        self.assertEqual('col4', cols[0].name)

        cols = table.get_columns(alias=['ca', 'cb'])
        self.assertEqual(2, len(cols))

        cols = table.get_columns(alias=['ac'])
        self.assertEqual('col4', cols[0].name)

    def test_get(self) -> None:
        table = CassandraTable(self.mock_config, self.keyspace)

        col = table.get('col4')
        self.assertEqual('col4', col.name)

        col = table.get('day')
        self.assertEqual('day', col.name)

        col = table.get('month')
        self.assertIsNone(col)

    def test_is_query_valid(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query(keyspace='mock_keyspace')
        is_valid = table._is_query_valid()
        self.assertTrue(is_valid)

    def test_is_query_valid_raises_date_not_defined_error(self) -> None:
        self.mock_config['split'] = 'day'
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .id('mock_col1')

        with self.assertRaises(DateNotDefinedError):
            table._is_query_valid()

    def test_replace(self) -> None:
        statement = "SELECT * FROM {keyspace_name}.{table_name} {where};"
        keyspace_statement = \
            CassandraTable._replace(statement, 'keyspace_name', 'mock_keyspace')

        self.assertEqual(
            keyspace_statement,
            "SELECT * FROM mock_keyspace.{table_name} {where};"
        )

        table_statement = CassandraTable \
            ._replace(keyspace_statement, 'table_name', 'mock_table')
        self.assertEqual(
            table_statement,
            "SELECT * FROM mock_keyspace.mock_table {where};"
        )

        final_statement = \
            CassandraTable._replace(table_statement, 'where', '')
        self.assertEqual(
            final_statement,
            "SELECT * FROM mock_keyspace.mock_table ;"
        )

    def test_create(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .create(
                keyspace='mock_keyspace',
                gc_grace_seconds=1000,
                if_not_exists=False,
                create_materialized_views=False
            )
        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "CREATE TABLE mock_keyspace.mock_table ( "
            "col1 TEXT, col2 BIGINT, col3 FLOAT, col4 FLOAT, col5 SMALLINT, "
            "day BIGINT, h3 TEXT, "
            "PRIMARY KEY ( (col1), col2 ) "
            ") WITH CLUSTERING ORDER BY ( id DESC ) AND gc_grace_seconds=1000;",
            table.statements[0]
        )

    def test_create_with_split(self) -> None:
        self.mock_config['split'] = 'year'
        self.mock_config['generated_columns'] = {
            'day': 'col2',
            'year': 'col2',
            'h3': 'col3,col4'
        }
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .create(
                keyspace='mock_keyspace',
                gc_grace_seconds=1000,
                if_not_exists=False,
                create_materialized_views=False
            ) \
            .time(datetime(2019, 1, 1), datetime(2020, 12, 31))
        self.assertEqual(2, len(table.statements))
        self.assertEqual(
            "CREATE TABLE mock_keyspace.mock_table_2019 ( "
            "col1 TEXT, col2 BIGINT, col3 FLOAT, col4 FLOAT, col5 SMALLINT, "
            "day BIGINT, year BIGINT, h3 TEXT, "
            "PRIMARY KEY ( (col1), col2 ) "
            ") WITH CLUSTERING ORDER BY ( id DESC ) AND gc_grace_seconds=1000;",
            table.statements[0]
        )
        self.assertEqual(
            "CREATE TABLE mock_keyspace.mock_table_2020 ( "
            "col1 TEXT, col2 BIGINT, col3 FLOAT, col4 FLOAT, col5 SMALLINT, "
            "day BIGINT, year BIGINT, h3 TEXT, "
            "PRIMARY KEY ( (col1), col2 ) "
            ") WITH CLUSTERING ORDER BY ( id DESC ) AND gc_grace_seconds=1000;",
            table.statements[1]
        )

    def test_create_if_not_exists(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .create(
                keyspace='mock_keyspace',
                gc_grace_seconds=1000,
                if_not_exists=True,
                create_materialized_views=False
            )
        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "CREATE TABLE IF NOT EXISTS mock_keyspace.mock_table ( "
            "col1 TEXT, col2 BIGINT, col3 FLOAT, col4 FLOAT, col5 SMALLINT, "
            "day BIGINT, h3 TEXT, "
            "PRIMARY KEY ( (col1), col2 ) "
            ") WITH CLUSTERING ORDER BY ( id DESC ) AND gc_grace_seconds=1000;",
            table.statements[0]
        )

    def test_create_with_materialized_views(self) -> None:
        self.mock_config['query'] = {
            'base': {
                'required': {'id': 'col1'},
                'optional': ['day'],
                'order': {'id': 'desc'}
            },
            'second': {
                'required': {'time': 'day'},
                'optional': ['col1'],
                'order': {'day': 'desc'}
            }
        }
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .create(
                keyspace='mock_keyspace',
                gc_grace_seconds=1000,
                if_not_exists=False,
                create_materialized_views=True
            )
        self.assertEqual(2, len(table.statements))
        self.assertEqual(
            "CREATE TABLE mock_keyspace.mock_table ( "
            "col1 TEXT, col2 BIGINT, col3 FLOAT, col4 FLOAT, col5 SMALLINT, "
            "day BIGINT, h3 TEXT, "
            "PRIMARY KEY ( (col1), day ) "
            ") WITH CLUSTERING ORDER BY ( id DESC ) AND gc_grace_seconds=1000;",
            table.statements[0]
        )
        self.assertEqual(
            "CREATE MATERIALIZED VIEW mock_keyspace.mock_table_second "
            "AS SELECT * FROM mock_keyspace.mock_table "
            "WHERE day IS NOT NULL AND col1 IS NOT NULL "
            "PRIMARY KEY ( (day), col1 ) "
            "WITH CLUSTERING ORDER BY ( day DESC ) "
            "AND gc_grace_seconds=1000;",
            table.statements[1]
        )

    def test_drop(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .drop(
                keyspace='mock_keyspace',
                if_exists=False,
                drop_materialized_views=False
            )
        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "DROP TABLE mock_keyspace.mock_table;",
            table.statements[0]
        )

    def test_drop_if_exists(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .drop(
                keyspace='mock_keyspace',
                if_exists=True,
                drop_materialized_views=False
            )
        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "DROP TABLE IF EXISTS mock_keyspace.mock_table;",
            table.statements[0]
        )

    def test_drop_with_materialized_views(self) -> None:
        self.mock_config['query'] = {
            'base': {'required': {'id': 'col1'}, 'optional': ['day']},
            'second': {'required': {'time': 'day'}, 'optional': ['col1']}
        }
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .drop(
                keyspace='mock_keyspace',
                if_exists=False,
                drop_materialized_views=True
            )
        self.assertEqual(2, len(table.statements))
        self.assertEqual(
            "DROP MATERIALIZED VIEW mock_keyspace.mock_table_second;",
            table.statements[0]
        )
        self.assertEqual(
            "DROP TABLE mock_keyspace.mock_table;",
            table.statements[1]
        )

    def test_drop_with_split(self) -> None:
        self.mock_config['split'] = 'year'
        self.mock_config['generated_columns'] = {
            'day': 'col2',
            'year': 'col2',
            'h3': 'col3,col4'
        }
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .drop(
                keyspace='mock_keyspace',
                if_exists=False,
                drop_materialized_views=False
            ) \
            .time(datetime(2019, 1, 1), datetime(2020, 12, 31))
        self.assertEqual(2, len(table.statements))
        self.assertEqual(
            "DROP TABLE mock_keyspace.mock_table_2019;",
            table.statements[0]
        )
        self.assertEqual(
            "DROP TABLE mock_keyspace.mock_table_2020;",
            table.statements[1]
        )

    def test_insert(self) -> None:
        table = CassandraTable(self.mock_config, self.keyspace)
        table.insert({
            'col1': 'mock_id',
            'col2': 1546304400000,
            'col3': 26.919388,
            'col4': -8.932613
        }, keyspace='mock_keyspace')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "INSERT INTO mock_keyspace.mock_table "
            "JSON '{\"col1\": \"mock_id\", \"col2\": 1546304400000, "
            "\"col3\": 26.919388, \"col4\": -8.932613, \"day\": 1546300800000, "
            "\"h3\": \"835525fffffffff\"}' ;",
            table.statements[0]
        )

        for _ in range(4):
            table = table.insert({
                'col1': 'mock_id',
                'col2': 1546304400000,
                'col3': 26.919388,
                'col4': -8.932613
            }, keyspace='mock_keyspace')
        self.assertEqual(5, len(table.statements))
        for i in range(5):
            self.assertEqual(
                "INSERT INTO mock_keyspace.mock_table "
                "JSON '{\"col1\": \"mock_id\", \"col2\": 1546304400000, "
                "\"col3\": 26.919388, \"col4\": -8.932613, "
                "\"day\": 1546300800000, \"h3\": \"835525fffffffff\"}' ;",
                table.statements[i]
            )

    def test_insert_with_split(self) -> None:
        self.mock_config['split'] = 'day'
        table = CassandraTable(self.mock_config, self.keyspace)
        table.insert({
            'col1': 'mock_id',
            'col2': 1546304400000,
            'col3': 26.919388,
            'col4': -8.932613
        }, keyspace='mock_keyspace')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "INSERT INTO mock_keyspace.mock_table_01_01_2019 "
            "JSON '{\"col1\": \"mock_id\", \"col2\": 1546304400000, "
            "\"col3\": 26.919388, \"col4\": -8.932613, "
            "\"day\": 1546300800000, \"h3\": \"835525fffffffff\"}' ;",
            table.statements[0]
        )

    def test_insert_with_ttl(self) -> None:
        table = CassandraTable(self.mock_config, self.keyspace)
        table.insert({
            'col1': 'mock_id',
            'col2': 1546304400000,
            'col3': 26.919388,
            'col4': -8.932613
        }, keyspace='mock_keyspace', ttl=86400)

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "INSERT INTO mock_keyspace.mock_table "
            "JSON '{\"col1\": \"mock_id\", \"col2\": 1546304400000, "
            "\"col3\": 26.919388, \"col4\": -8.932613, \"day\": 1546300800000, "
            "\"h3\": \"835525fffffffff\"}' "
            "USING TTL 86400;",
            table.statements[0]
        )

        for _ in range(4):
            table = table.insert({
                'col1': 'mock_id',
                'col2': 1546304400000,
                'col3': 26.919388,
                'col4': -8.932613
            }, keyspace='mock_keyspace', ttl=86400)
        self.assertEqual(5, len(table.statements))
        for i in range(5):
            self.assertEqual(
                "INSERT INTO mock_keyspace.mock_table "
                "JSON '{\"col1\": \"mock_id\", \"col2\": 1546304400000, "
                "\"col3\": 26.919388, \"col4\": -8.932613, "
                "\"day\": 1546300800000, \"h3\": \"835525fffffffff\"}' "
                "USING TTL 86400;",
                table.statements[i]
            )

    def test_query(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table   ;",
            table.statements[0]
        )

    def test_query_materialized_view(self) -> None:
        self.mock_config['query'] = {
            'base': {'required': {'id': 'col1'}, 'optional': ['day']},
            'second': {'required': {'time': 'day'}, 'optional': ['col1']}
        }
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('second', keyspace='mock_keyspace')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table_second   ;",
            table.statements[0]
        )

    def test_query_with_split(self) -> None:
        self.mock_config['split'] = 'day'
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace')

        self.assertEqual(1, len(table.statements))
        self.assertIn('_{date}', table.statements[0])
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table_{date}   ;",
            table.statements[0]
        )

    def test_query_raises_query_not_found(self) -> None:
        table = CassandraTable(self.mock_config, self.keyspace)
        with self.assertRaises(QueryNotFound):
            table.query('second')

    def test_select(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .select(['col1', 'col2', 'col3'])

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT col1, col2, col3 "
            "FROM mock_keyspace.mock_table   ;",
            table.statements[0]
        )

    def test_select_raises_missing_column_error(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace')
        with self.assertRaises(MissingColumnError):
            table.select(['col1', 'col2', 'col6'])

    def test_time(self) -> None:
        self.mock_config['query'] = {
            'base': {'required': {'time': 'day'}, 'optional': ['col1']},
        }
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .time(datetime(2019, 1, 1), datetime(2019, 1, 1))

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE day=1546300800000   ;",
            table.statements[0]
        )

    def test_time_with_required_and_split(self) -> None:
        self.mock_config['query'] = {
            'base': {'required': {'time': 'day'}, 'optional': ['col1']},
        }
        self.mock_config['split'] = 'year'
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .time(datetime(2019, 1, 1), datetime(2019, 1, 1))

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table_2019 "
            "WHERE day=1546300800000   ;",
            table.statements[0]
        )

    def test_time_with_required_and_split_same(self) -> None:
        self.mock_config['query'] = {
            'base': {'required': {'time': 'day'}, 'optional': ['col1']},
        }
        self.mock_config['split'] = 'day'
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .time(datetime(2019, 1, 1), datetime(2019, 1, 2))

        self.assertEqual(2, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table_01_01_2019   ;",
            table.statements[0]
        )
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table_02_01_2019   ;",
            table.statements[1]
        )

    def test_time_with_split(self) -> None:
        self.mock_config['split'] = 'day'
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .time(datetime(2019, 1, 1), datetime(2019, 1, 2))

        self.assertEqual(2, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table_01_01_2019   ;",
            table.statements[0]
        )
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table_02_01_2019   ;",
            table.statements[1]
        )

    def test_time_prepare(self) -> None:
        self.mock_config['query'] = {
            'base': {'required': {'time': 'day'}, 'optional': ['col1']},
        }
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .time(datetime(2019, 1, 1), datetime(2019, 1, 1), prepare=True)

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE day=?   ;",
            table.statements[0]
        )

    def test_space(self) -> None:
        self.mock_config['query'] = {
            'base': {'required': {'space': 'h3'}, 'optional': ['col1']},
        }
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .space('835525fffffffff')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE h3='835525fffffffff'   ;",
            table.statements[0]
        )

        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .space(['835525fffffffff'])

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE h3 IN ('835525fffffffff')   ;",
            table.statements[0]
        )

    def test_space_prepare(self) -> None:
        self.mock_config['query'] = {
            'base': {'required': {'space': 'h3'}, 'optional': ['col1']},
        }
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .space()

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE h3='?'   ;",
            table.statements[0]
        )

    def test_id(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .id('mock_id')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col1='mock_id'   ;",
            table.statements[0]
        )

        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .id(['mock_id'])

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col1 IN ('mock_id')   ;",
            table.statements[0]
        )

    def test_id_prepare(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .id()

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col1=?   ;",
            table.statements[0]
        )

    def test_equals(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .equals('col1', 'mock_id')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col1='mock_id'   ;",
            table.statements[0]
        )

    def test_equals_prepare(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .equals('col1')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col1=?   ;",
            table.statements[0]
        )

    def test_among(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .among('col1', ['mock_id'])

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col1 IN ('mock_id')   ;",
            table.statements[0]
        )

    def test_among_prepare(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .among('col1')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col1 IN (?)   ;",
            table.statements[0]
        )

    def test_between(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .between('col2', 1546304400000, 1546304400010)

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 > 1546304400000 "
            "AND col2 < 1546304400010   ;",
            table.statements[0]
        )

    def test_between_prepare(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .between('col2')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 > ? AND col2 < ?   ;",
            table.statements[0]
        )

    def test_between_including(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .between_including('col2', 1546304400000, 1546304400010)

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 >= 1546304400000 "
            "AND col2 <= 1546304400010   ;",
            table.statements[0]
        )

    def test_between_including_prepare(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .between_including('col2')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 >= ? AND col2 <= ?   ;",
            table.statements[0]
        )

    def test_lower_than(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .lower_than('col2', 1546304400000)

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 < 1546304400000   ;",
            table.statements[0]
        )

    def test_lower_than_prepare(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .lower_than('col2')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 < ?   ;",
            table.statements[0]
        )

    def test_lower_or_equal_than(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .lower_or_equal_than('col2', 1546304400000)

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 <= 1546304400000   ;",
            table.statements[0]
        )

    def test_lower_or_equal_than_prepare(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .lower_or_equal_than('col2')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 <= ?   ;",
            table.statements[0]
        )

    def test_higher_than(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .higher_than('col2', 1546304400000)

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 > 1546304400000   ;",
            table.statements[0]
        )

    def test_higher_than_prepare(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .higher_than('col2')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 > ?   ;",
            table.statements[0]
        )

    def test_higher_or_equal_than(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .higher_or_equal_than('col2', 1546304400000)

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 >= 1546304400000   ;",
            table.statements[0]
        )

    def test_higher_or_equal_than_prepare(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .higher_or_equal_than('col2')

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table "
            "WHERE col2 >= ?   ;",
            table.statements[0]
        )

    def test_limit(self) -> None:
        table = \
            CassandraTable(self.mock_config, self.keyspace) \
            .query('base', keyspace='mock_keyspace') \
            .limit(10)

        self.assertEqual(1, len(table.statements))
        self.assertEqual(
            "SELECT * FROM mock_keyspace.mock_table  LIMIT 10 ;",
            table.statements[0]
        )


if __name__ == '__main__':
    unittest.main()
