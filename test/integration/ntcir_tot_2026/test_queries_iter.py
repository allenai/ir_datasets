import unittest

def load_dataset(lang, split):
    import ir_datasets
    return ir_datasets.load(f"ntcir-tot/2026/{lang}/{split}")

def load_query_number(lang, split, num):
    index = 0
    for i in load_dataset(lang, split).queries_iter():
        if num == index:
            return i
        index += 1


class TestQueriesIter(unittest.TestCase):
    def test_first_en_train_query(self):
        actual = load_query_number("en", "train", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("0001", actual.query_id)
        self.assertIn("I remember this film I watched ages ago", actual.default_text())

    def test_first_en_dev_query(self):
        actual = load_query_number("en", "dev", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("4001", actual.query_id)
        self.assertIn("I remember catching this intense film years ago, maybe during a late-night", actual.default_text())

    def test_first_en_test_query(self):
        actual = load_query_number("en", "test", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("4501", actual.query_id)
        self.assertIn("he film had this epic feel, with grand sets that made you feel like you were stepping", actual.default_text())

    def test_first_ja_train_query(self):
        actual = load_query_number("ja", "train", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("0001", actual.query_id)
        self.assertIn("名前がどうしても思い出せない", actual.default_text())

    def test_first_ja_dev_query(self):
        actual = load_query_number("ja", "dev", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("4001", actual.query_id)
        self.assertIn("ずっと前に観た日本の映画のことを思い", actual.default_text())

    def test_first_ja_test_query(self):
        actual = load_query_number("ja", "test", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("4501", actual.query_id)
        self.assertIn("あの映画のことを思い出そ", actual.default_text())

    def test_first_ko_train_query(self):
        actual = load_query_number("ko", "train", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("0001", actual.query_id)
        self.assertIn("아마도 오래된 흑백 영화 중 하", actual.default_text())

    def test_first_ko_dev_query(self):
        actual = load_query_number("ko", "dev", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("4001", actual.query_id)
        self.assertIn("오래된 기억이 떠올랐어요", actual.default_text())

    def test_first_ko_test_query(self):
        actual = load_query_number("ko", "test", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("4501", actual.query_id)
        self.assertIn("그날은 유난히 하늘이 붉게", actual.default_text())

    def test_first_zh_train_query(self):
        actual = load_query_number("zh", "train", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("0001", actual.query_id)
        self.assertIn("我在一个朋友家聚会时看到了一部令", actual.default_text())

    def test_first_zh_dev_query(self):
        actual = load_query_number("zh", "dev", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("4001", actual.query_id)
        self.assertIn("在一个视频平台上发现了一部特别", actual.default_text())

    def test_first_zh_test_query(self):
        actual = load_query_number("zh", "test", 0)

        self.assertIsNotNone(actual)
        self.assertEqual("4501", actual.query_id)
        self.assertIn("推荐下看了一部电影", actual.default_text())
