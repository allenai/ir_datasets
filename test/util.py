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

    def test_corpus_id(self):
        # typical
        self.assertEqual(ir_datasets.corpus_id("msmarco-document/trec-dl-2019/judged"), "msmarco-document")
        # identity
        self.assertEqual(ir_datasets.corpus_id("msmarco-document"), "msmarco-document")
        # wikir doesn't support docs
        self.assertEqual(ir_datasets.corpus_id("wikir/en1k/test"), "wikir/en1k")
        # clueweb09 supports docs, but clueweb09/catb is a different subset
        self.assertEqual(ir_datasets.corpus_id("clueweb09/catb/trec-web-2009"), "clueweb09/catb")
        self.assertEqual(ir_datasets.corpus_id("clueweb09/catb"), "clueweb09/catb")
        self.assertEqual(ir_datasets.corpus_id("clueweb09"), "clueweb09")
        # clirmatrix uses matching patterns
        self.assertEqual(ir_datasets.corpus_id("clirmatrix/en"), "clirmatrix/en")
        self.assertEqual(ir_datasets.corpus_id("clirmatrix/en/bi139-full/de/train"), "clirmatrix/en")


if __name__ == '__main__':
    unittest.main()
