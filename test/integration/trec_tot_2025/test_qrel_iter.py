import unittest

def load_dataset(dataset_id):
    import ir_datasets
    return ir_datasets.load(dataset_id)

def load_qrel_number(dataset_id, num):
    index = 0
    for i in load_dataset(dataset_id).qrels_iter():
        if num == index:
            return i
        index += 1

class TestQrelIter(unittest.TestCase):
    def test_train_dataset_can_be_loaded(self):
        actual = load_dataset("trec-tot/2025/train")
        self.assertIsNotNone(actual)

    def test_dev1_dataset_can_be_loaded(self):
        actual = load_dataset("trec-tot/2025/dev1")
        self.assertIsNotNone(actual)

    def test_dev2_dataset_can_be_loaded(self):
        actual = load_dataset("trec-tot/2025/dev2")
        self.assertIsNotNone(actual)

    def test_dev3_dataset_can_be_loaded(self):
        actual = load_dataset("trec-tot/2025/dev3")
        self.assertIsNotNone(actual)

    def test_train_qrel_iter(self):
        actual = load_qrel_number("trec-tot/2025/train", 12)
        self.assertEqual("1014", actual.query_id)
        self.assertEqual("46264411", actual.doc_id)
        self.assertEqual(1, actual.relevance)

    def test_dev1_qrel_iter(self):
        actual = load_qrel_number("trec-tot/2025/dev1", 12)
        self.assertEqual("898", actual.query_id)
        self.assertEqual("3761238", actual.doc_id)
        self.assertEqual(1, actual.relevance)

    def test_dev2_qrel_iter(self):
        actual = load_qrel_number("trec-tot/2025/dev2", 12)
        self.assertEqual("632", actual.query_id)
        self.assertEqual("3261733", actual.doc_id)
        self.assertEqual(1, actual.relevance)

    def test_dev3_qrel_iter(self):
        actual = load_qrel_number("trec-tot/2025/dev3", 12)
        self.assertEqual("2014", actual.query_id)
        self.assertEqual("446518", actual.doc_id)
        self.assertEqual(1, actual.relevance)

