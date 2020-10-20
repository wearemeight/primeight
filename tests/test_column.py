import unittest
from typing import List, Tuple, Set

from primeight.column import CassandraColumn


class ColumnTestCase(unittest.TestCase):

    def test_constructor(self) -> None:
        col = CassandraColumn('col1', 'int')
        self.assertEqual('col1', col.name)
        self.assertEqual('int', col.type)

        col = CassandraColumn('col1', 'int', alias='ac')
        self.assertEqual('ac', col.alias)

        col = CassandraColumn('col1', 'int', description="Column 1")
        self.assertEqual("Column 1", col.description)

        col = CassandraColumn('col1', 'int', min_value=0, max_value=1000)
        self.assertEqual(0, col.min_value)
        self.assertEqual(1000, col.max_value)

    def test_cassandra_type(self) -> None:
        col = CassandraColumn('col1', 'list<int>')
        self.assertEqual('FROZEN<LIST<INT>>', col.cassandra_type())

        col = CassandraColumn('col1', 'set<int>')
        self.assertEqual('FROZEN<SET<INT>>', col.cassandra_type())

        col = CassandraColumn('col1', 'tuple<int, text>')
        self.assertEqual('FROZEN<TUPLE<INT,TEXT>>', col.cassandra_type())

        col = CassandraColumn('col1', 'list<tuple<text, int>>')
        self.assertEqual(
            'FROZEN<LIST<FROZEN<TUPLE<TEXT,INT>>>>', col.cassandra_type()
        )

    def test_pydantic_type(self) -> None:
        col = CassandraColumn('col1', 'list<tuple<text, int>>')
        self.assertEqual(List[Tuple[str, int]], col.pydantic_type())

        col = CassandraColumn('col1', 'set<int>')
        self.assertEqual(Set[int], col.pydantic_type())

        col = CassandraColumn('col1', 'tuple<int, text>')
        self.assertEqual(Tuple[int, str], col.pydantic_type())

        col = CassandraColumn('col1', 'list<tuple<text, int>>')
        self.assertEqual(List[Tuple[str, int]], col.pydantic_type())

    def test_has_types(self) -> None:
        self.assertEqual(
            CassandraColumn.NATIVE_TYPES,
            list(CassandraColumn.HANDLE_TO_CASSANDRA_TYPE.keys())
        )
        self.assertEqual(
            list(CassandraColumn.HANDLE_TO_TYPE.keys()),
            list(CassandraColumn.HANDLE_TO_CASSANDRA_TYPE.keys())
        )

    def test_is_valid(self) -> None:
        col = CassandraColumn('col1', 'int', min_value=0, max_value=1000)
        self.assertTrue(col.is_valid(50))
        self.assertFalse(col.is_valid(1050))


if __name__ == '__main__':
    unittest.main()
