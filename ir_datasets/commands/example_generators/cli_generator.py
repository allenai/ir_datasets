import ir_datasets
from ir_datasets.commands.example_generators import Example, find_corpus_dataset

class CliExampleGenerator():
    def __init__(self, dataset_id):
        self.dataset_id = dataset_id
        self.dataset = ir_datasets.load(dataset_id)

    def generate_docs(self):
        if not self.dataset.has_docs():
            return None
        fields = '&nbsp;&nbsp;&nbsp;&nbsp;'.join(f'[{f}]' for f in self.dataset.docs_cls()._fields)
        return Example(code=f'''
ir_datasets export {self.dataset_id} docs
''', output=f'''
<div>{fields}</div>
<div>...</div>
''', code_lang='bash', message_html='You can find more details about the CLI <a href="cli.html">here</a>.')

    def generate_queries(self):
        if not self.dataset.has_queries():
            return None
        fields = '&nbsp;&nbsp;&nbsp;&nbsp;'.join(f'[{f}]' for f in self.dataset.queries_cls()._fields)
        return Example(code=f'''
ir_datasets export {self.dataset_id} queries
''', output=f'''
<div>{fields}</div>
<div>...</div>
''', code_lang='bash', message_html='You can find more details about the CLI <a href="cli.html">here</a>.')

    def generate_qrels(self):
        if not self.dataset.has_qrels():
            return None
        fields = '&nbsp;&nbsp;&nbsp;&nbsp;'.join(f'[{f}]' for f in self.dataset.qrels_cls()._fields)
        return Example(code=f'''
ir_datasets export {self.dataset_id} qrels --format tsv
''', output=f'''
<div>{fields}</div>
<div>...</div>
''', code_lang='bash', message_html='You can find more details about the CLI <a href="cli.html">here</a>.')

    def generate_scoreddocs(self):
        if not self.dataset.has_scoreddocs():
            return None
        fields = '&nbsp;&nbsp;&nbsp;&nbsp;'.join(f'[{f}]' for f in self.dataset.scoreddocs_cls()._fields)
        return Example(code=f'''
ir_datasets export {self.dataset_id} scoreddocs --format tsv
''', output=f'''
<div>{fields}</div>
<div>...</div>
''', code_lang='bash', message_html='You can find more details about the CLI <a href="cli.html">here</a>.')

    def generate_docpairs(self):
        if not self.dataset.has_docpairs():
            return None
        fields = '&nbsp;&nbsp;&nbsp;&nbsp;'.join(f'[{f}]' for f in self.dataset.docpairs_cls()._fields)
        return Example(code=f'''
ir_datasets export {self.dataset_id} docpairs
''', output=f'''
<div>{fields}</div>
<div>...</div>
''', code_lang='bash', message_html='You can find more details about the CLI <a href="cli.html">here</a>.')
