import unittest

from primeight.generators import Generators


class SpaceGeneratorsTestCase(unittest.TestCase):

    def setUp(self):
        self.lat = 38.702767
        self.lon = -9.168398

    def test_h3_generator(self):
        identifier = Generators.h3(self.lat, self.lon)
        self.assertEqual('833933fffffffff', identifier)

    def test_h4_generator(self):
        identifier = Generators.h4(self.lat, self.lon)
        self.assertEqual('8439337ffffffff', identifier)

    def test_h5_generator(self):
        identifier = Generators.h5(self.lat, self.lon)
        self.assertEqual('85393363fffffff', identifier)

    def test_h6_generator(self):
        identifier = Generators.h6(self.lat, self.lon)
        self.assertEqual('86393362fffffff', identifier)

    def test_h7_generator(self):
        identifier = Generators.h7(self.lat, self.lon)
        self.assertEqual('87393360cffffff', identifier)

    def test_h8_generator(self):
        identifier = Generators.h8(self.lat, self.lon)
        self.assertEqual('88393360c9fffff', identifier)

    def test_h9_generator(self):
        identifier = Generators.h9(self.lat, self.lon)
        self.assertEqual('89393360c97ffff', identifier)

    def test_h10_generator(self):
        identifier = Generators.h10(self.lat, self.lon)
        self.assertEqual('8a393360c977fff', identifier)

    def test_h11_generator(self):
        identifier = Generators.h11(self.lat, self.lon)
        self.assertEqual('8b393360c974fff', identifier)

    def test_h12_generator(self):
        identifier = Generators.h12(self.lat, self.lon)
        self.assertEqual('8c393360c9741ff', identifier)


if __name__ == '__main__':
    unittest.main()
