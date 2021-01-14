import unittest
import ir_datasets


class TestUtil(unittest.TestCase):
    def test_apply_sub_slice(self):
    	ass = ir_datasets.util.apply_sub_slice
    	self.assertEqual(ass(slice(0, 100), slice(0, 10)), slice(0, 10))
    	self.assertEqual(ass(slice(0, 10), slice(0, 100)), slice(0, 10))
    	self.assertEqual(ass(slice(10, 100), slice(0, 10)), slice(10, 20))
    	self.assertEqual(ass(slice(0, 10), slice(10, 100)), slice(10, 10))
    	self.assertEqual(ass(slice(0, 100), slice(0, None)), slice(0, 100))
    	self.assertEqual(ass(slice(0, 100), slice(0, -1)), slice(0, 99))
    	self.assertEqual(ass(slice(0, 100), slice(0, -2)), slice(0, 98))
    	self.assertEqual(ass(slice(0, 100), slice(1, -2)), slice(1, 98))
    	self.assertEqual(ass(slice(0, 100), slice(-1, None)), slice(99, 100))
    	self.assertEqual(ass(slice(0, 100), slice(-2, None)), slice(98, 100))
    	self.assertEqual(ass(slice(0, 100), slice(-2, -1)), slice(98, 99))
    	self.assertEqual(ass(slice(0, 100), slice(0/3, 1/3)), slice(0, 33))
    	self.assertEqual(ass(slice(0, 100), slice(1/3, 2/3)), slice(33, 66))
    	self.assertEqual(ass(slice(0, 100), slice(2/3, 3/3)), slice(66, 100))


if __name__ == '__main__':
    unittest.main()
