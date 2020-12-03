import re
import unittest
import ir_datasets


_logger = ir_datasets.log.easy()


class DatasetIntegrationTest(unittest.TestCase):
    def _test_docs(self, dataset_name, count=None, items=None):
        with self.subTest('docs', dataset=dataset_name):
            dataset = ir_datasets.load(dataset_name)
            expected_count = count
            items = items or {}
            count = 0
            for i, doc in enumerate(_logger.pbar(dataset.docs_iter(), f'{dataset_name} docs')):
                count += 1
                if i in items:
                    self._assert_namedtuple(doc, items[i])
                    del items[i]
                    if expected_count is None and len(items) == 0:
                        break # no point in going further

            if expected_count is not None:
                self.assertEqual(expected_count, count)

            self.assertEqual(0, len(items))

    def _test_queries(self, dataset_name, count=None, items=None):
        with self.subTest('queries', dataset=dataset_name):
            dataset = ir_datasets.load(dataset_name)
            expected_count = count
            items = items or {}
            count = 0
            for i, query in enumerate(_logger.pbar(dataset.queries_iter(), f'{dataset_name} queries')):
                count += 1
                if i in items:
                    self._assert_namedtuple(query, items[i])
                    del items[i]
                    if expected_count is None and len(items) == 0:
                        break # no point in going further

            if expected_count is not None:
                self.assertEqual(expected_count, count)

            self.assertEqual(0, len(items))

    def _test_qrels(self, dataset_name, count=None, items=None):
        with self.subTest('qrels', dataset=dataset_name):
            dataset = ir_datasets.load(dataset_name)
            expected_count = count
            items = items or {}
            count = 0
            for i, qrel in enumerate(_logger.pbar(dataset.qrels_iter(), f'{dataset_name} qrels')):
                count += 1
                if i in items:
                    self._assert_namedtuple(qrel, items[i])
                    del items[i]
                    if expected_count is None and len(items) == 0:
                        break # no point in going further

            if expected_count is not None:
                self.assertEqual(expected_count, count)

            self.assertEqual(0, len(items))

    def _test_scoreddocs(self, dataset_name, count=None, items=None):
        with self.subTest('scoreddocs', dataset=dataset_name):
            dataset = ir_datasets.load(dataset_name)
            expected_count = count
            items = items or {}
            count = 0
            for i, scoreddoc in enumerate(_logger.pbar(dataset.scoreddocs_iter(), f'{dataset_name} scoreddocs')):
                count += 1
                if i in items:
                    self._assert_namedtuple(scoreddoc, items[i])
                    del items[i]
                    if expected_count is None and len(items) == 0:
                        break # no point in going further

            if expected_count is not None:
                self.assertEqual(expected_count, count)

            self.assertEqual(0, len(items))

    def _test_docpairs(self, dataset_name, count=None, items=None):
        with self.subTest('docpairs', dataset=dataset_name):
            dataset = ir_datasets.load(dataset_name)
            expected_count = count
            items = items or {}
            count = 0
            for i, docpair in enumerate(_logger.pbar(dataset.docpairs_iter(), f'{dataset_name} docpairs')):
                count += 1
                if i in items:
                    self._assert_namedtuple(docpair, items[i])
                    del items[i]
                    if expected_count is None and len(items) == 0:
                        break # no point in going further

            if expected_count is not None:
                self.assertEqual(expected_count, count)

            self.assertEqual(0, len(items))

    def _build_test_docs(self, dataset_name, include_count=True):
        items = {}
        count = 0
        for i, doc in enumerate(_logger.pbar(ir_datasets.load(dataset_name).docs_iter(), f'{dataset_name} docs')):
            count += 1
            if i in (0, 9):
                items[i] = doc
            if not include_count and i == 1000:
                break
        items[count-1] = doc
        items = {k: self._replace_regex_namedtuple(v) for k, v in items.items()}
        count = f', count={count} ' if include_count else ''
        _logger.info(f'''
self._test_docs({repr(dataset_name)}, {count}items={self._repr_namedtuples(items)})
''')

    def _build_test_queries(self, dataset_name):
        items = {}
        count = 0
        for i, query in enumerate(_logger.pbar(ir_datasets.load(dataset_name).queries_iter(), f'{dataset_name} queries')):
            count += 1
            if i in (0, 9):
                items[i] = query
        items[count-1] = query
        _logger.info(f'''
self._test_queries({repr(dataset_name)}, count={count}, items={self._repr_namedtuples(items)})
''')

    def _build_test_qrels(self, dataset_name):
        items = {}
        count = 0
        for i, qrel in enumerate(_logger.pbar(ir_datasets.load(dataset_name).qrels_iter(), f'{dataset_name} qrels')):
            count += 1
            if i in (0, 9):
                items[i] = qrel
        items[count-1] = qrel
        _logger.info(f'''
self._test_qrels({repr(dataset_name)}, count={count}, items={self._repr_namedtuples(items)})
''')

    def _build_test_scoreddocs(self, dataset_name):
        items = {}
        count = 0
        for i, scoreddoc in enumerate(_logger.pbar(ir_datasets.load(dataset_name).scoreddocs_iter(), f'{dataset_name} scoreddocs')):
            count += 1
            if i in (0, 9):
                items[i] = scoreddoc
        items[count-1] = scoreddoc
        _logger.info(f'''
self._test_scoreddocs({repr(dataset_name)}, count={count}, items={self._repr_namedtuples(items)})
''')

    def _build_test_docpairs(self, dataset_name):
        items = {}
        count = 0
        for i, docpair in enumerate(_logger.pbar(ir_datasets.load(dataset_name).docpairs_iter(), f'{dataset_name} docpairs')):
            count += 1
            if i in (0, 9):
                items[i] = docpair
        items[count-1] = docpair
        _logger.info(f'''
self._test_docpairs(i{repr(dataset_name)}, count={count}, items={self._repr_namedtuples(items)})
''')

    def _assert_namedtuple(self, a, b):
        self.assertEqual(type(a).__name__, type(b).__name__)
        self.assertEqual(type(a)._fields, type(b)._fields)
        for v_a, v_b in zip(a, b):
            # support compiled regex for matching (e.g., for long documents)
            if isinstance(v_b, re.Pattern):
                self.assertRegex(v_a, v_b)
            elif isinstance(v_a, re.Pattern):
                self.assertRegex(v_b, v_a)
            else:
                self.assertEqual(v_a, v_b)

    def _replace_regex_namedtuple(self, tup, maxlen=200):
        result = []
        for value in tup:
            if isinstance(value, str) and len(value) > maxlen:
                count = len(value) - maxlen
                pattern = '^' + re.escape(value[:maxlen//2]) + (r'.{%i}' % count) + re.escape(value[-(maxlen//2):]) + '$'
                result.append(re.compile(pattern, re.DOTALL))
            else:
                result.append(value)
        return type(tup)(*result)

    def _repr_namedtuples(self, items):
        result = '{\n'
        for key, value in items.items():
            result += f'    {repr(key)}: {type(value).__name__}('
            for item in value:
                if isinstance(item, re.Pattern):
                    pattern = item.pattern.replace('\\ ', ' ').replace('\\\n', '\n') # don't want these escaped
                    result += f're.compile({repr(pattern)}, flags={item.flags}), '
                else:
                    result += f'{repr(item)}, '
            result = result[:-2] + '),\n'
        result += '}'
        return result
