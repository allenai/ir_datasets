import ir_datasets
from ir_datasets.commands.example_generators import Example, find_corpus_dataset

class PyTerrierExampleGenerator():
    # some datasets do not work as one would expect with the default generated code.
    # For now, we'll just indicate them here and disable the examples for them.
    OVERRIDE_PREFIXES = ['clueweb09', 'clueweb12', 'gov', 'gov2']

    def __init__(self, dataset_id):
        self.dataset_id = dataset_id
        self.dataset = ir_datasets.load(dataset_id)
        try:
            self.pt_ds_path = find_corpus_dataset(dataset_id)
        except:
            self.pt_ds_path = None
        self.skip = any(self.dataset_id.startswith(p) for p in self.OVERRIDE_PREFIXES)

    def generate_docs(self):
        return self.generate_indexing()

    def generate_queries(self):
        return self.generate_bm25_pipeline()

    def generate_qrels(self):
        return self.generate_bm25_experiment()

    def generate_scoreddocs(self):
        return None

    def generate_docpairs(self):
        return None

    def generate_indexing(self):
        if not self.dataset.has_docs() or self.pt_ds_path is None or self.skip:
            return None
        if self.dataset.docs_lang() != 'en': # TODO: add support for other languages
            return None
        fields = self.dataset.docs_cls()._fields
        text_content_fields = [f for f in fields if self.dataset.docs_cls().__annotations__[f] is str and f not in ('doc_id', 'marked_up_text', 'source_xml', 'msmarco_document_id')]
        text_content_fields_list = ', '.join([f"'{f}'" for f in text_content_fields])
        return Example(code=f'''
import pyterrier as pt
pt.init()
dataset = pt.get_dataset('irds:{self.dataset_id}')
# Index {self.pt_ds_path}
indexer = pt.IterDictIndexer('./indices/{self.pt_ds_path.replace('/', '_')}')
index_ref = indexer.index(dataset.get_corpus_iter(), fields=[{text_content_fields_list}])
''', message_html='You can find more details about PyTerrier indexing <a href="https://pyterrier.readthedocs.io/en/latest/datasets.html#examples">here</a>.')

    def generate_bm25_pipeline(self):
        if not self.dataset.has_docs() and not self.dataset.has_queries() or self.pt_ds_path is None or self.skip:
            return None
        if self.dataset.docs_lang() != 'en' or self.dataset.queries_lang() != 'en': # TODO: add support for other languages
            return None
        query_field = ''
        if len(self.dataset.queries_cls()._fields) > 2 and self.dataset.queries_cls()._fields[1] != 'query':
            query_field = f"'{self.dataset.queries_cls()._fields[1]}'"
        return Example(code=f'''
import pyterrier as pt
pt.init()
dataset = pt.get_dataset('irds:{self.dataset_id}')
index_ref = pt.IndexRef.of('./indices/{self.pt_ds_path.replace('/', '_')}') # assumes you have already built an index
pipeline = pt.BatchRetrieve(index_ref, wmodel='BM25')
# (optionally other pipeline components)
pipeline(dataset.get_topics({query_field}))
''', message_html='You can find more details about PyTerrier retrieval <a href="https://pyterrier.readthedocs.io/en/latest/terrier-retrieval.html">here</a>.')

    def generate_bm25_experiment(self):
        if not self.dataset.has_docs() and not self.dataset.has_queries() and not self.dataset.has_qrels() or self.pt_ds_path is None or self.skip:
            return None
        if self.dataset.docs_lang() != 'en' or self.dataset.queries_lang() != 'en': # TODO: add support for other languages
            return None
        query_field = ''
        if len(self.dataset.queries_cls()._fields) > 2 and self.dataset.queries_cls()._fields[1] != 'query':
            query_field = f"'{self.dataset.queries_cls()._fields[1]}'"
        measures = 'MAP, nDCG@20'
        if 'official_measures' in self.dataset.documentation():
            measures = ', '.join(self.dataset.documentation()['official_measures'])
        return Example(f'''
import pyterrier as pt
from pyterrier.measures import *
pt.init()
dataset = pt.get_dataset('irds:{self.dataset_id}')
index_ref = pt.IndexRef.of('./indices/{self.pt_ds_path.replace('/', '_')}') # assumes you have already built an index
pipeline = pt.BatchRetrieve(index_ref, wmodel='BM25')
# (optionally other pipeline components)
pt.Experiment(
    [pipeline],
    dataset.get_topics({query_field}),
    dataset.get_qrels(),
    [{measures}]
)
''', message_html='You can find more details about PyTerrier experiments <a href="https://pyterrier.readthedocs.io/en/latest/experiments.html">here</a>.')
