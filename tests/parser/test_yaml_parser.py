import unittest
from unittest.mock import patch

from primeight.parser.yaml_parser import YamlParser, yaml, Path


class YamlParserTestCase(unittest.TestCase):

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

    def test_parse(self):
        with patch.object(Path, 'is_dir', return_value=False), \
                patch.object(Path, 'is_file', return_value=True), \
                patch.object(Path, 'read_text', return_value="mock_text"), \
                patch.object(yaml, 'safe_load', return_value=self._config):
            config = YamlParser.parse('test.yaml')
        self.assertEqual(self._config, config)

    def test_parse_raise_marked_yaml_error(self):
        with patch.object(Path, 'is_dir', return_value=False), \
                patch.object(Path, 'is_file', return_value=True), \
                patch.object(Path, 'read_text', return_value="mock_text"), \
                patch.object(yaml, 'safe_load') as mock_safe_load, \
                self.assertRaises(SyntaxError):
            mock_safe_load.side_effect = yaml.MarkedYAMLError(problem_mark={'line': 10, 'column': 5})

            _ = YamlParser.parse('test.yaml')

    def test_parse_raise_yaml_error(self):
        with patch.object(Path, 'is_dir', return_value=False), \
                patch.object(Path, 'is_file', return_value=True), \
                patch.object(Path, 'read_text', return_value="mock_text"), \
                patch.object(yaml, 'safe_load') as mock_safe_load, \
                self.assertRaises(SyntaxError):
            mock_safe_load.side_effect = yaml.YAMLError

            _ = YamlParser.parse('test.yaml')

    def test_parse_raises_eof_error(self):
        with patch.object(Path, 'is_dir', return_value=False), \
             patch.object(Path, 'is_file', return_value=True), \
             patch.object(Path, 'read_text', return_value=""), \
             self.assertRaises(EOFError):
            YamlParser.parse('test.yaml')

    def test_parse_raises_is_a_directory_error(self):
        with patch.object(Path, 'is_dir', return_value=True), \
                self.assertRaises(IsADirectoryError):
            YamlParser.parse('test.yaml')

    def test_parse_raises_file_not_found_error(self):
        with patch.object(Path, 'is_file', return_value=False), \
             self.assertRaises(FileNotFoundError):
            YamlParser.parse('test.yaml')


if __name__ == '__main__':
    unittest.main()
