import unittest

from primeight.parser.parser import Parser


class ParserTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self._config = {
            'version': '1.0.0',
            'name': 'test_table',
            'keyspace': 'test_keyspace',
            'columns': {
                'user_id': {'type': 'uuid', 'description': "User unique identifier"},
                'arrival_time': {'type': 'timestamp'},
                'priority': {'type': 'int', 'min': 0, 'max': 5}
            },
            'generated_columns': {
                'day': 'arrival_time'
            },
            'query': {
                'base': {
                    'required': {'id': 'user_id'},
                    'optional': ['day']
                },
                'arrival_day': {
                    'required': {'time': 'day'},
                    'optional': ['user_id']
                }
            },
            'lifetime': 10,
            'backup': 'day'
        }

    def test_is_valid_name(self):
        is_valid = Parser.is_valid_name("speed")
        self.assertTrue(is_valid)

        is_valid = Parser.is_valid_name("2fast")
        self.assertFalse(is_valid)

        is_valid = Parser.is_valid_name("speed-track")
        self.assertFalse(is_valid)

        is_valid = Parser.is_valid_name("boolean")
        self.assertFalse(is_valid)

    def test_is_valid_version(self):
        is_valid = Parser.is_valid_version("1")
        self.assertTrue(is_valid)

        is_valid = Parser.is_valid_version("1.0")
        self.assertTrue(is_valid)

        is_valid = Parser.is_valid_version("1.1.0")
        self.assertTrue(is_valid)

        is_valid = Parser.is_valid_version("v1.0.0")
        self.assertFalse(is_valid)

        is_valid = Parser.is_valid_version("one")
        self.assertFalse(is_valid)

    def test_is_valid_config_empty(self):
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config({})

    def test_is_valid_config_invalid_version(self):
        self._config['version'] = 'abc'
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_invalid_name(self):
        self._config['name'] = 'invalid-name'
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_missing_columns(self):
        del self._config['columns']
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_missing_query(self):
        del self._config['query']
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_missing_base_query(self):
        del self._config['query']['base']
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_unrecognized_field(self):
        self._config['random'] = 'base'
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_unrecognized_backup(self):
        self._config['backup'] = 'time to time'
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_unreconized_lifetime(self):
        self._config['lifetime'] = 'now'
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_negative_lifetime(self):
        self._config['lifetime'] = -5
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_unrecognized_type(self):
        self._config['columns']['user_id']['type'] = 'random'
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_unrecognized_generator(self):
        self._config['generated_columns']['random'] = 'arrival_time'
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_undeclared_column_used_in_generator(self):
        self._config['generated_columns']['day'] = 'shipping_time'
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_undeclared_query_column(self):
        self._config['query']['arrival_day']['required']['time'] = 'shipping_time'
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_max_not_a_number(self):
        self._config['columns']['priority']['max'] = 'random'
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)

    def test_is_valid_config_min_not_a_number(self):
        self._config['columns']['priority']['min'] = 'random'
        with self.assertRaises(SyntaxError):
            Parser.is_valid_config(self._config)


if __name__ == '__main__':
    unittest.main()
