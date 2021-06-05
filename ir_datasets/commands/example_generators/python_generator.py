import ir_datasets
from ir_datasets.commands.example_generators import Example, find_corpus_dataset

class PythonExampleGenerator():
    def __init__(self, dataset_id):
        self.dataset_id = dataset_id
        self.dataset = ir_datasets.load(dataset_id)

    def generate_docs(self):
        if not self.dataset.has_docs():
            return None
        fields = ', '.join(self.dataset.docs_cls()._fields)
        return Example(code=f'''
import ir_datasets
dataset = ir_datasets.load("{self.dataset_id}")
for doc in dataset.docs_iter():
    doc # namedtuple<{fields}>
''', message_html='You can find more details about the Python API <a href="python.html">here</a>.')

    def generate_queries(self):
        if not self.dataset.has_queries():
            return None
        fields = ', '.join(self.dataset.queries_cls()._fields)
        return Example(code=f'''
import ir_datasets
dataset = ir_datasets.load("{self.dataset_id}")
for query in dataset.queries_iter():
    query # namedtuple<{fields}>
''', message_html='You can find more details about the Python API <a href="python.html">here</a>.')

    def generate_qrels(self):
        if not self.dataset.has_qrels():
            return None
        fields = ', '.join(self.dataset.qrels_cls()._fields)
        return Example(code=f'''
import ir_datasets
dataset = ir_datasets.load("{self.dataset_id}")
for qrel in dataset.qrels_iter():
    qrel # namedtuple<{fields}>
''', message_html='You can find more details about the Python API <a href="python.html">here</a>.')

    def generate_scoreddocs(self):
        if not self.dataset.has_scoreddocs():
            return None
        fields = ', '.join(self.dataset.scoreddocs_cls()._fields)
        return Example(code=f'''
import ir_datasets
dataset = ir_datasets.load("{self.dataset_id}")
for scoreddoc in dataset.scoreddocs_iter():
    scoreddoc # namedtuple<{fields}>
''', message_html='You can find more details about the Python API <a href="python.html">here</a>.')

    def generate_docpairs(self):
        if not self.dataset.has_docpairs():
            return None
        fields = ', '.join(self.dataset.docpairs_cls()._fields)
        return Example(code=f'''
import ir_datasets
dataset = ir_datasets.load("{self.dataset_id}")
for docpair in dataset.docpairs_iter():
    docpair # namedtuple<{fields}>
''', message_html='You can find more details about the Python API <a href="python.html">here</a>.')
