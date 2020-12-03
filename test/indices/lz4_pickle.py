import tempfile
import unittest
import numpy as np
from ir_datasets.indices import Lz4PickleLookup
from ir_datasets.formats import GenericDoc


class TestLz4PickleLookup(unittest.TestCase):
    def test_lz4_pickle_lookup(self):
        with tempfile.TemporaryDirectory() as d:
            idx = Lz4PickleLookup(d, GenericDoc)
            self.assertEqual(tuple(idx['id3', 'id2', 'id1', 'id4']), tuple())
            self.assertEqual(tuple(iter(idx)), tuple())
            with idx.transaction() as trans:
                trans.add(GenericDoc('id1', 'some text'))
                trans.add(GenericDoc('id2', 'some other text'))
                trans.add(GenericDoc('id3', 'short'))
                trans.add(GenericDoc('id2', 'duplicate - should overwrite'))
            self.assertEqual(list(idx['id1'])[0], GenericDoc('id1', 'some text'))
            self.assertEqual(list(idx['id2'])[0], GenericDoc('id2', 'duplicate - should overwrite'))
            self.assertEqual(list(idx['id3'])[0], GenericDoc('id3', 'short'))
            results = tuple(idx['id3', 'id2', 'id1', 'id4'])
            self.assertEqual(results, (GenericDoc('id1', 'some text'), GenericDoc('id3', 'short'), GenericDoc('id2', 'duplicate - should overwrite')))

            with idx.transaction() as trans:
                trans.add(GenericDoc('id1', 'new text'))
                trans.add(GenericDoc('id4', 'new doc'))
            self.assertEqual(list(idx['id1'])[0], GenericDoc('id1', 'new text'))
            self.assertEqual(list(idx['id2'])[0], GenericDoc('id2', 'duplicate - should overwrite'))
            self.assertEqual(list(idx['id3'])[0], GenericDoc('id3', 'short'))
            self.assertEqual(list(idx['id4'])[0], GenericDoc('id4', 'new doc'))
            results = tuple(idx['id3', 'id2', 'id1', 'id4'])
            self.assertEqual(results, (GenericDoc('id3', 'short'), GenericDoc('id2', 'duplicate - should overwrite'), GenericDoc('id1', 'new text'), GenericDoc('id4', 'new doc')))

            with idx.transaction() as trans:
                trans.add(GenericDoc('id1', 'newer text'))
                trans.add(GenericDoc('id4', 'newer doc'))
                trans.rollback()
            self.assertEqual(list(idx['id1'])[0], GenericDoc('id1', 'new text'))
            self.assertEqual(list(idx['id2'])[0], GenericDoc('id2', 'duplicate - should overwrite'))
            self.assertEqual(list(idx['id3'])[0], GenericDoc('id3', 'short'))
            self.assertEqual(list(idx['id4'])[0], GenericDoc('id4', 'new doc'))
            results = tuple(idx['id3', 'id2', 'id1', 'id4'])
            self.assertEqual(results, (GenericDoc('id3', 'short'), GenericDoc('id2', 'duplicate - should overwrite'), GenericDoc('id1', 'new text'), GenericDoc('id4', 'new doc')))

            idx.close()



if __name__ == '__main__':
    unittest.main()
