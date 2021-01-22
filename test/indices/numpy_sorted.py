import tempfile
import unittest
import numpy as np
from ir_datasets.indices import NumpySortedIndex


class TestNumpySortedIndex(unittest.TestCase):
    def test_numpy_sorted_index(self):
        with tempfile.NamedTemporaryFile() as f:
            idx = NumpySortedIndex(f.name)
            values = idx['key', 'key1231', 'key', 'missing']
            self.assertEqual(len(idx), 0)
            self.assertEqual(tuple(iter(idx)), tuple())
            values = idx['key', 'key1231', 'key', 'missing']
            self.assertEqual(values[0], -1)
            self.assertEqual(values[1], -1)
            self.assertEqual(values[2], -1)
            self.assertEqual(values[3], -1)

            idx.add('k', 1)
            idx.add('key', 3)
            idx.add('key4', 2)
            idx.add('key1231', 4)
            idx.add('k', 3)
            idx.commit()
            self.assertEqual(len(idx), 4)
            values = idx['key', 'key1231', 'key', 'missing']
            self.assertEqual(values[0], 3)
            self.assertEqual(values[1], 4)
            self.assertEqual(values[2], 3)
            self.assertEqual(values[3], -1)
            idx.add('key', 5)
            idx.add('key4', 1)
            idx.add('key5', 8)
            idx.commit()
            self.assertEqual(len(idx), 5)
            values = idx['key', 'key1231', 'key']
            self.assertEqual(values[0], 5)
            self.assertEqual(values[1], 4)
            self.assertEqual(values[2], 5)
            values = idx['key4']
            self.assertEqual(values[0], 1)
            idx.close()
            self.assertEqual(len(idx), 5)
            values = idx['key4']
            self.assertEqual(values[0], 1)
            self.assertEqual(tuple(iter(idx)), ('k', 'key', 'key1231', 'key4', 'key5'))

            idx.close()


if __name__ == '__main__':
    unittest.main()
