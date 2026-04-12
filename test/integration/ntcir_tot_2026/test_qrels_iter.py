import unittest

def load_dataset(lang, split):
    import ir_datasets
    return ir_datasets.load(f"ntcir-tot/2026/{lang}/{split}")

def load_qrel_number(lang, split, num):
    index = 0
    for i in load_dataset(lang, split).qrels_iter():
        if num == index:
            return i
        index += 1


class TestQueriesIter(unittest.TestCase):
    def test_first_en_train_qrel(self):
        actual = load_qrel_number("en", "train", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("0001", actual.query_id)
        self.assertEqual(1, actual.relevance)
        self.assertEqual("1405697", actual.doc_id)

    def test_first_en_dev_qrel(self):
        actual = load_qrel_number("en", "dev", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("4001", actual.query_id)
        self.assertEqual(1, actual.relevance)
        self.assertEqual("226682", actual.doc_id)

    def test_first_ja_train_qrel(self):
        actual = load_qrel_number("ja", "train", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("0001", actual.query_id)
        self.assertEqual(1, actual.relevance)
        self.assertEqual("2168072", actual.doc_id)

    def test_first_ja_dev_qrel(self):
        actual = load_qrel_number("ja", "dev", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("4001", actual.query_id)
        self.assertEqual(1, actual.relevance)
        self.assertEqual("1265355", actual.doc_id)

    def test_first_ko_train_qrel(self):
        actual = load_qrel_number("ko", "train", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("0001", actual.query_id)
        self.assertEqual(1, actual.relevance)
        self.assertEqual("3053301", actual.doc_id)

    def test_first_ko_dev_qrel(self):
        actual = load_qrel_number("ko", "dev", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("4001", actual.query_id)
        self.assertEqual(1, actual.relevance)
        self.assertEqual("1080933", actual.doc_id)

    def test_first_zh_train_qrel(self):
        actual = load_qrel_number("zh", "train", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("0001", actual.query_id)
        self.assertEqual(1, actual.relevance)
        self.assertEqual("8173481", actual.doc_id)

    def test_first_zh_dev_qrel(self):
        actual = load_qrel_number("zh", "dev", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("4001", actual.query_id)
        self.assertEqual(1, actual.relevance)
        self.assertEqual("1532371", actual.doc_id)

