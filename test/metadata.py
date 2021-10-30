import unittest
import ir_datasets


class TestMetadata(unittest.TestCase):
    def test_all_metadata_available(self):
        for dsid in ir_datasets.registry._registered:
            with self.subTest(dsid):
                dataset = ir_datasets.load(dsid)
                metadata = dataset.metadata()
                for etype in ir_datasets.EntityType:
                    if dataset.has(etype):
                        self.assertTrue(etype.value in metadata, f"{dsid} missing {etype.value} metadata")
                        self.assertTrue('count' in metadata[etype.value], f"{dsid} missing {etype.value} metadata")


if __name__ == '__main__':
    unittest.main()
