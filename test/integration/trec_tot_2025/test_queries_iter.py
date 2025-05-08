import unittest

def load_dataset(dataset_id):
    import ir_datasets
    return ir_datasets.load(dataset_id)

def load_query_number(dataset_id, num):
    index = 0
    for i in load_dataset(dataset_id).queries_iter():
        if num == index:
            return i
        index += 1

class TestQueriesIter(unittest.TestCase):
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

    def test_query_from_train_dataset_can_be_loaded_01(self):
        actual = load_query_number("trec-tot/2025/train", 2)
        self.assertIsNotNone(actual)
        self.assertEqual("950", actual.query_id)
        self.assertIn("two girls who run away", actual.default_text())

    def test_query_from_train_dataset_can_be_loaded_02(self):
        actual = load_query_number("trec-tot/2025/train", 25)
        self.assertIsNotNone(actual)
        self.assertEqual("484", actual.query_id)
        self.assertIn("Main character is a famous person like a celebrity", actual.default_text())

    def test_query_from_dev1_dataset_can_be_loaded_01(self):
        actual = load_query_number("trec-tot/2025/dev1", 2)
        self.assertIsNotNone(actual)
        self.assertEqual("473", actual.query_id)
        self.assertIn("possibly a ghost killing in an old house", actual.default_text())

    def test_query_from_dev1_dataset_can_be_loaded_02(self):
        actual = load_query_number("trec-tot/2025/dev1", 25)
        self.assertIsNotNone(actual)
        self.assertEqual("153", actual.query_id)
        self.assertIn("Martial arts movie where the human is fighting aliens", actual.default_text())

    def test_query_from_dev2_dataset_can_be_loaded_01(self):
        actual = load_query_number("trec-tot/2025/dev2", 2)
        self.assertIsNotNone(actual)
        self.assertEqual("477", actual.query_id)
        self.assertIn("Pretty sure it was a comedy", actual.default_text())

    def test_query_from_dev2_dataset_can_be_loaded_02(self):
        actual = load_query_number("trec-tot/2025/dev2", 25)
        self.assertIsNotNone(actual)
        self.assertEqual("873", actual.query_id)
        self.assertIn("I remember there were 2 siblings involved in the movie", actual.default_text())

    def test_query_from_dev3_dataset_can_be_loaded_01(self):
        actual = load_query_number("trec-tot/2025/dev3", 2)
        self.assertIsNotNone(actual)
        self.assertEqual("2003", actual.query_id)
        self.assertIn("I remember a scene where the bell tower guy and the soldier had to sneak into this hidden place", actual.default_text())

    def test_query_from_dev3_dataset_can_be_loaded_02(self):
        actual = load_query_number("trec-tot/2025/dev3", 25)
        self.assertIsNotNone(actual)
        self.assertEqual("2028", actual.query_id)
        self.assertIn("The place had this weird energy source", actual.default_text())

