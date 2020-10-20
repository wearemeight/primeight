import unittest
from datetime import datetime

from primeight.keyspace import CassandraKeyspace
from primeight.table import CassandraTable
from primeight.exceptions import \
    MissingColumnError, NotARequiredColumnError


class ExceptionsTestCase(unittest.TestCase):

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

    def test_missing_column(self):
        mock_keyspace = CassandraKeyspace(self.mock_config)
        table = CassandraTable(self.mock_config, mock_keyspace)

        with self.assertRaises(MissingColumnError):
            table \
                .query('base', keyspace='mock_keyspace') \
                .select('missing_column')

    def test_not_a_required_column(self):
        mock_keyspace = CassandraKeyspace(self.mock_config)
        table = CassandraTable(self.mock_config, mock_keyspace)
        mock_date = datetime.now()

        with self.assertRaises(NotARequiredColumnError):
            table \
                .query('base', keyspace='mock_keyspace') \
                .time(mock_date, mock_date)

        with self.assertRaises(NotARequiredColumnError):
            table \
                .query('base', keyspace='mock_keyspace') \
                .space('mock_hex')

        self.mock_config['query'] = {
            'base': {
                'required': {'time': 'col2'}
            }
        }
        mock_keyspace = CassandraKeyspace(self.mock_config)
        table = CassandraTable(self.mock_config, mock_keyspace)
        with self.assertRaises(NotARequiredColumnError):
            table \
                .query('base', keyspace='mock_keyspace') \
                .id('mock_id')


if __name__ == '__main__':
    unittest.main()
