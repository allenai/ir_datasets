import re
import unittest
import ir_datasets


_logger = ir_datasets.log.easy()


class DatasetIntegrationTest(unittest.TestCase):
    def _test_docs(self, dataset_name, count=None, items=None, test_docstore=True, test_iter_split=True):
        orig_items = dict(items)
        with self.subTest('docs', dataset=dataset_name):
            if isinstance(dataset_name, str):
                dataset = ir_datasets.load(dataset_name)
            else:
                dataset = dataset_name
            expected_count = count
            items = items or {}
            count = 0
            for i, doc in enumerate(_logger.pbar(dataset.docs_iter(), f'{dataset_name} docs', unit='doc')):
                count += 1
                if i in items:
                    self._assert_namedtuple(doc, items[i])
                    del items[i]
                    if expected_count is None and len(items) == 0:
                        break # no point in going further

            if expected_count is not None:
                self.assertEqual(expected_count, count)

            self.assertEqual({}, items)

        if test_iter_split:
            with self.subTest('docs_iter split', dataset=dataset_name):
                it = dataset.docs_iter()
                with _logger.duration('doc lookups by index'):
                    for idx, doc in orig_items.items():
                        self._assert_namedtuple(next(it[idx:idx+1]), doc)
                        self._assert_namedtuple(it[idx], doc)

        if test_docstore:
            with self.subTest('docs_store', dataset=dataset_name):
                doc_store = dataset.docs_store()
                with _logger.duration('doc lookups by doc_id'):
                    for doc in orig_items.values():
                        ret_doc = doc_store.get(doc.doc_id)
                        self._assert_namedtuple(doc, ret_doc)

    def _test_queries(self, dataset_name, count=None, items=None):
        with self.subTest('queries', dataset=dataset_name):
            if isinstance(dataset_name, str):
                dataset = ir_datasets.load(dataset_name)
            else:
                dataset = dataset_name
            expected_count = count
            items = items or {}
            count = 0
            for i, query in enumerate(_logger.pbar(dataset.queries_iter(), f'{dataset_name} queries', unit='query')):
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
            if isinstance(dataset_name, str):
                dataset = ir_datasets.load(dataset_name)
            else:
                dataset = dataset_name
            expected_count = count
            items = items or {}
            count = 0
            for i, qrel in enumerate(_logger.pbar(dataset.qrels_iter(), f'{dataset_name} qrels', unit='qrel')):
                count += 1
                if i in items:
                    self._assert_namedtuple(qrel, items[i])
                    del items[i]
                    if expected_count is None and len(items) == 0:
                        break # no point in going further

            if expected_count is not None:
                self.assertEqual(expected_count, count)

            self.assertEqual(0, len(items))

    def _test_qlogs(self, dataset_name, count=None, items=None):
        with self.subTest('qlogs', dataset=dataset_name):
            if isinstance(dataset_name, str):
                dataset = ir_datasets.load(dataset_name)
            else:
                dataset = dataset_name
            expected_count = count
            items = items or {}
            count = 0
            for i, qlogs in enumerate(_logger.pbar(dataset.qlogs_iter(), f'{dataset_name} qlogs', unit='qlog')):
                count += 1
                if i in items:
                    self._assert_namedtuple(qlogs, items[i])
                    del items[i]
                    if expected_count is None and len(items) == 0:
                        break # no point in going further

            if expected_count is not None:
                self.assertEqual(expected_count, count)

            self.assertEqual(0, len(items))

    def _test_docpairs(self, dataset_name, count=None, items=None):
        with self.subTest('docpairs', dataset=dataset_name):
            if isinstance(dataset_name, str):
                dataset = ir_datasets.load(dataset_name)
            else:
                dataset = dataset_name
            expected_count = count
            items = items or {}
            count = 0
            for i, docpair in enumerate(_logger.pbar(dataset.docpairs_iter(), f'{dataset_name} docpairs', unit='docpair')):
                count += 1
                if i in items:
                    self._assert_namedtuple(docpair, items[i])
                    del items[i]
                    if expected_count is None and len(items) == 0:
                        break # no point in going further

            if expected_count is not None:
                self.assertEqual(expected_count, count)

            self.assertEqual(0, len(items))

    def _build_test_docs(self, dataset_name, include_count=True, include_idxs=(0, 9)):
        items = {}
        count = 0
        if isinstance(dataset_name, str):
            dataset = ir_datasets.load(dataset_name)
        else:
            dataset = dataset_name
        for i, doc in enumerate(_logger.pbar(dataset.docs_iter(), f'{dataset_name} docs', unit='doc')):
            count += 1
            if i in include_idxs:
                items[i] = doc
            if not include_count and ((include_idxs[-1] < 1000 and i == 1000) or (include_idxs[-1] >= 1000 and i == include_idxs[-1])):
                break
        items[count-1] = doc
        items = {k: self._replace_regex_namedtuple(v) for k, v in items.items()}
        count = f', count={count}' if include_count else ''
        _logger.info(f'''
self._test_docs({repr(dataset_name)}{count}, items={self._repr_namedtuples(items)})
''')

    def _build_test_queries(self, dataset_name):
        items = {}
        count = 0
        if isinstance(dataset_name, str):
            dataset = ir_datasets.load(dataset_name)
        else:
            dataset = dataset_name
        for i, query in enumerate(_logger.pbar(dataset.queries_iter(), f'{dataset_name} queries', unit='query')):
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
        if isinstance(dataset_name, str):
            dataset = ir_datasets.load(dataset_name)
        else:
            dataset = dataset_name
        for i, qrel in enumerate(_logger.pbar(dataset.qrels_iter(), f'{dataset_name} qrels', unit='qrel')):
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
        if isinstance(dataset_name, str):
            dataset = ir_datasets.load(dataset_name)
        else:
            dataset = dataset_name
        for i, scoreddoc in enumerate(_logger.pbar(dataset.scoreddocs_iter(), f'{dataset_name} scoreddocs', unit='scoreddoc')):
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
        for i, docpair in enumerate(_logger.pbar(ir_datasets.load(dataset_name).docpairs_iter(), f'{dataset_name} docpairs', unit='docpair')):
            count += 1
            if i in (0, 9):
                items[i] = docpair
        items[count-1] = docpair
        _logger.info(f'''
self._test_docpairs({repr(dataset_name)}, count={count}, items={self._repr_namedtuples(items)})
''')

    def _test_scoreddocs(self, dataset_name, count=None, items=None):
        with self.subTest('scoreddocs', dataset=dataset_name):
            if isinstance(dataset_name, str):
                dataset = ir_datasets.load(dataset_name)
            else:
                dataset = dataset_name
            expected_count = count
            items = items or {}
            count = 0
            for i, scoreddoc in enumerate(_logger.pbar(dataset.scoreddocs_iter(), f'{dataset_name} scoreddocs', unit='scoreddoc')):
                count += 1
                if i in items:
                    self._assert_namedtuple(scoreddoc, items[i])
                    del items[i]
                    if expected_count is None and len(items) == 0:
                        break # no point in going further
            if expected_count is not None:
                self.assertEqual(expected_count, count)
            self.assertEqual(0, len(items))

    def _build_test_qlogs(self, dataset_name):
        items = {}
        count = 0
        for i, qlog in enumerate(_logger.pbar(ir_datasets.load(dataset_name).qlogs_iter(), f'{dataset_name} qlogs', unit='qlogs')):
            count += 1
            if i in (0, 9):
                items[i] = qlog
        items[count-1] = qlog
        _logger.info(f'''
self._test_qlogs({repr(dataset_name)}, count={count}, items={self._repr_namedtuples(items)})
''')

    def _assert_namedtuple(self, a, b):
        # needed because python <= 3.6 doesn't expose re.Pattern class
        Pattern = re.Pattern if hasattr(re, 'Pattern') else type(re.compile(''))
        self.assertEqual(type(a).__name__, type(b).__name__)
        if hasattr(type(a), '_fields') or hasattr(type(b), '_fields'):
            self.assertEqual(type(a)._fields, type(b)._fields)
        for v_a, v_b in zip(a, b):
            # support compiled regex for matching (e.g., for long documents)
            if isinstance(v_b, Pattern):
                self.assertRegex(v_a, v_b)
            elif isinstance(v_a, Pattern):
                self.assertRegex(v_b, v_a)
            elif isinstance(v_a, tuple) and isinstance(v_b, tuple):
                self._assert_namedtuple(v_a, v_b)
            elif isinstance(v_a, list) and isinstance(v_b, list):
                self._assert_namedtuple(v_a, v_b)
            else:
                self.assertEqual(v_a, v_b)

    def _replace_regex_namedtuple(self, tup, maxlen=200):
        result = []
        for value in tup:
            if isinstance(value, str) and len(value) > maxlen:
                count = len(value) - maxlen
                pattern = '^' + re.escape(value[:maxlen//2]) + (r'.{%i}' % count) + re.escape(value[-(maxlen//2):]) + '$'
                result.append(re.compile(pattern, re.DOTALL))
            elif isinstance(value, bytes) and len(value) > maxlen:
                count = len(value) - maxlen
                pattern = b'^' + re.escape(value[:maxlen//2]) + (b'.{%i}' % count) + re.escape(value[-(maxlen//2):]) + b'$'
                result.append(re.compile(pattern, re.DOTALL))
            elif isinstance(value, tuple) and isinstance(value[0], tuple):
                result.append(tuple(self._replace_regex_namedtuple(t) for t in value))
            elif isinstance(value, list) and isinstance(value[0], tuple):
                result.append(list(self._replace_regex_namedtuple(t) for t in value))
            else:
                result.append(value)
        return type(tup)(*result)

    def _repr_namedtuples(self, items):
        result = '{\n'
        for key, value in items.items():
            result += f'    {repr(key)}: {self._repr_namedtuple(value)},\n'
        result += '}'
        return result

    def _repr_namedtuple(self, value):
        result = f'{type(value).__name__}('
        for item in value:
            if isinstance(item, re.Pattern):
                if isinstance(item.pattern, str):
                    pattern = item.pattern.replace('\\ ', ' ').replace('\\\n', '\n') # don't want these escaped
                else:
                    pattern = item.pattern.replace(b'\\ ', b' ').replace(b'\\\n', b'\n') # don't want these escaped
                result += f're.compile({repr(pattern)}, flags={item.flags}), '
            elif isinstance(item, list) and len(item) > 0 and isinstance(item[0], tuple) and hasattr(item[0], '_fields'):
                result += '[' + ', '.join(self._repr_namedtuple(i) for i in item) + '], '
            elif isinstance(item, tuple) and len(item) > 0 and isinstance(item[0], tuple) and hasattr(item[0], '_fields'):
                result += '(' + ', '.join(self._repr_namedtuple(i) for i in item) + ',), '
            else:
                result += f'{repr(item)}, '
        result = result[:-2] + ')'
        return result
