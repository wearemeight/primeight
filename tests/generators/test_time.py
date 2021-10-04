import unittest
from datetime import datetime
from uuid import uuid1

from primeight.generators import Generators


class TimeGeneratorsTestCase(unittest.TestCase):

    def setUp(self):
        self.timestamp = 1567173886896
        self.uuid = uuid1()
        self.date = datetime.fromtimestamp(self.timestamp / 1000)

    def test_day_generator(self):
        ts = Generators.day(self.timestamp)
        self.assertEqual(1567123200000, ts)

    def test_month_generator(self):
        ts = Generators.month(self.timestamp)
        self.assertEqual(1564617600000, ts)

    def test_month_generator_from_uuid(self):
        ts = Generators.month(self.uuid)
        self.assertEqual(1633046400000, ts)

    def test_year_generator(self):
        ts = Generators.year(self.timestamp)
        self.assertEqual(1546300800000, ts)


if __name__ == '__main__':
    unittest.main()
