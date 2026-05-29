import unittest

def load_dataset(lang):
    import ir_datasets
    return ir_datasets.load(f"ntcir-tot/2026/{lang}")

def load_doc_number(lang, num):
    index = 0
    for i in load_dataset(lang).docs_iter():
        if num == index:
            return i
        index += 1

class TestDocsIter(unittest.TestCase):
    def test_en_dataset_can_be_loaded(self):
        actual = load_dataset("en")
        self.assertIsNotNone(actual)

    def test_ja_dataset_can_be_loaded(self):
        actual = load_dataset("ja")
        self.assertIsNotNone(actual)

    def test_ko_dataset_can_be_loaded(self):
        actual = load_dataset("ko")
        self.assertIsNotNone(actual)

    def test_zh_dataset_can_be_loaded(self):
        actual = load_dataset("zh")
        self.assertIsNotNone(actual)

    def test_first_en_doc(self):
        actual = load_doc_number("en", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("12", actual.doc_id)
        self.assertEqual("https://en.wikipedia.org/wiki/Anarchism", actual.url)
        self.assertEqual("Anarchism", actual.title)
        self.assertIn("a political philosophy and movement that is skeptical", actual.text)
        self.assertIn("a political philosophy and movement that is skeptical", actual.default_text())
        self.assertIn("Anarchism Anarchism is a political philosophy and movement that is skeptical", actual.default_text())

    def test_third_en_doc(self):
        actual = load_doc_number("en", 3)

        self.assertIsNotNone(actual)
        self.assertEqual("303", actual.doc_id)
        self.assertEqual("https://en.wikipedia.org/wiki/Alabama", actual.url)
        self.assertEqual("Alabama", actual.title)
        self.assertIn("Alabama's economy in the 21st century is based on automotive", actual.text)
        self.assertIn("Alabama's economy in the 21st century is based on automotive", actual.default_text())

    def test_first_ja_doc(self):
        actual = load_doc_number("ja", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("5", actual.doc_id)
        self.assertEqual("https://ja.wikipedia.org/wiki/%E3%82%A2%E3%83%B3%E3%83%91%E3%82%B5%E3%83%B3%E3%83%89", actual.url)
        self.assertEqual("アンパサンド", actual.title)
        self.assertIn("を意味する記号である", actual.text)
        self.assertIn("アンパサンド アンパサンド", actual.default_text())

    def test_third_ja_doc(self):
        actual = load_doc_number("ja", 3)

        self.assertIsNotNone(actual)
        self.assertEqual("23", actual.doc_id)
        self.assertEqual("https://ja.wikipedia.org/wiki/%E5%9B%BD%E3%81%AE%E4%B8%80%E8%A6%A7", actual.url)
        self.assertEqual("国の一覧", actual.title)
        self.assertIn("世界の独立国の一覧", actual.text)
        self.assertIn("国際法上国家と言え", actual.default_text())

    def test_first_ko_doc(self):
        actual = load_doc_number("ko", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("19", actual.doc_id)
        self.assertEqual("https://ko.wikipedia.org/wiki/%EB%AC%B8%ED%95%99", actual.url)
        self.assertEqual("문학", actual.title)
        self.assertIn("표현의 제재로 삼아", actual.text)
        self.assertIn("언어를 예술적 표현의 제재로 삼아", actual.default_text())

    def test_third_ko_doc(self):
        actual = load_doc_number("ko", 3)

        self.assertIsNotNone(actual)
        self.assertEqual("36", actual.doc_id)
        self.assertEqual("https://ko.wikipedia.org/wiki/%ED%95%A8%EC%84%9D%ED%97%8C", actual.url)
        self.assertEqual("함석헌", actual.title)
        self.assertIn("대한민국의", actual.text)
        self.assertIn("대한민국의 독립운동가", actual.default_text())

    def test_first_zh_doc(self):
        actual = load_doc_number("zh", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("18", actual.doc_id)
        self.assertEqual("https://zh.wikipedia.org/wiki/%E5%93%B2%E5%AD%A6", actual.url)
        self.assertEqual("哲学", actual.title)
        self.assertIn("语言等领域", actual.text)
        self.assertIn("前苏格拉底时期", actual.default_text())

    def test_third_zh_doc(self):
        actual = load_doc_number("zh", 3)

        self.assertIsNotNone(actual)
        self.assertEqual("25", actual.doc_id)
        self.assertEqual("https://zh.wikipedia.org/wiki/%E8%AE%A1%E7%AE%97%E6%9C%BA%E7%A7%91%E5%AD%A6", actual.url)
        self.assertEqual("计算机科学", actual.title)
        self.assertIn("基础以及它们在计", actual.text)
        self.assertIn("但实际上计算机科学", actual.default_text())

