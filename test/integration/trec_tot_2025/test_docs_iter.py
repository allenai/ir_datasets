import unittest

def load_dataset():
    import ir_datasets
    return ir_datasets.load("trec-tot/2025")

def load_doc_number(num):
    index = 0
    for i in load_dataset().docs_iter():
        if num == index:
            return i
        index += 1

class TestDocsIter(unittest.TestCase):
    def test_dataset_can_be_loaded(self):
        actual = load_dataset()
        self.assertIsNotNone(actual)

    def test_first_doc(self):
        actual = load_doc_number(0)

        self.assertIsNotNone(actual)
        self.assertEqual("12", actual.doc_id)
        self.assertEqual("https://en.wikipedia.org/wiki/Anarchism", actual.url)
        self.assertEqual("Anarchism", actual.title)
        self.assertIn("a political philosophy and movement that is skeptical", actual.text)
        self.assertIn("a political philosophy and movement that is skeptical", actual.default_text())
        self.assertIn("Anarchism Anarchism is a political philosophy and movement that is skeptical", actual.default_text())

    def test_third_doc(self):
        actual = load_doc_number(3)

        self.assertIsNotNone(actual)
        self.assertEqual("303", actual.doc_id)
        self.assertEqual("https://en.wikipedia.org/wiki/Alabama", actual.url)
        self.assertEqual("Alabama", actual.title)
        self.assertIn("Alabama's economy in the 21st century is based on automotive", actual.text)
        self.assertIn("Alabama's economy in the 21st century is based on automotive", actual.default_text())
       
