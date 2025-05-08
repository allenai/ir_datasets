import unittest

def load_docs_store():
    import ir_datasets
    return ir_datasets.load("trec-tot/2025").docs_store()

class TestDocsStore(unittest.TestCase):
    def test_docs_store_can_be_loaded(self):
        actual = load_docs_store()
        self.assertIsNotNone(actual)

    def test_first_doc(self):
        actual = load_docs_store().get("12")

        self.assertIsNotNone(actual)
        self.assertEqual("12", actual.doc_id)
        self.assertEqual("https://en.wikipedia.org/wiki/Anarchism", actual.url)
        self.assertEqual("Anarchism", actual.title)
        self.assertIn("a political philosophy and movement that is skeptical", actual.text)
        self.assertIn("a political philosophy and movement that is skeptical", actual.default_text())
        self.assertIn("Anarchism Anarchism is a political philosophy and movement that is skeptical", actual.default_text())

    def test_third_doc(self):
        actual = load_docs_store().get("303")

        self.assertIsNotNone(actual)
        self.assertEqual("303", actual.doc_id)
        self.assertEqual("https://en.wikipedia.org/wiki/Alabama", actual.url)
        self.assertEqual("Alabama", actual.title)
        self.assertIn("Alabama's economy in the 21st century is based on automotive", actual.text)
        self.assertIn("Alabama's economy in the 21st century is based on automotive", actual.default_text())

    def test_some_random_doc(self):
        actual = load_docs_store().get("6596604")

        self.assertIsNotNone(actual)
        self.assertEqual("6596604", actual.doc_id)
        self.assertEqual("https://en.wikipedia.org/wiki/Radio%20Reloj", actual.url)
        self.assertEqual("Radio Reloj", actual.title)
        self.assertIn("Radio Reloj (Spanish for Radio Clock) is an internationally broadcast Spanish-language radio station", actual.text)
        self.assertIn("Radio Reloj (Spanish for Radio Clock) is an internationally broadcast Spanish-language radio station", actual.default_text())
      
