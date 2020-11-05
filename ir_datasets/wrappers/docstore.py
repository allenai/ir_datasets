import ir_datasets
from ir_datasets.indices import IndexedTsvDocstore, ZPickleDocStore


_logger = ir_datasets.log.easy()


class DocstoreWrapper:
    def __init__(self, dataset, docstore_cls=ZPickleDocStore):
        self._dataset = dataset
        self._docstore = None
        self._querystore = None
        self._docstore_cls = docstore_cls

    def __getattr__(self, attr):
        # Only expose functionality that's actually available
        if hasattr(self._dataset, 'docs_handler'):
            if attr == 'docs_store':
                return self._docs_store
            if attr == 'docs_iter':
                return self._docs_iter
        if hasattr(self._dataset, 'queries_handler'):
            if attr == 'queries_store':
                return self._queries_store
            if attr == 'queries_iter':
                return self._queries_iter
        return getattr(self._dataset, attr)

    def _docs_store(self):
        if self._docstore is None:
            ds = self._docstore_cls(f'{self._dataset.docs_path()}.{self._docstore_cls.file_ext}', self._dataset.docs_cls())
            if not ds.built():
                doc_iter = self._dataset.docs_iter()
                doc_iter = _logger.pbar(doc_iter, 'building doc store')
                ds.build(doc_iter)
            self._docstore = ds
        return self._docstore

    def _docs_iter(self):
        return iter(self._docs_store())

    def _queries_store(self):
        if self._querystore is None:
            ds = self._docstore_cls(f'{self._dataset.queries_path()}.{self._docstore_cls.file_ext}', self._dataset.queries_cls(), id_field='query_id')
            if not ds.built():
                queries_iter = self._dataset.queries_iter()
                queries_iter = _logger.pbar(queries_iter, 'building query store')
                ds.build(queries_iter)
            self._querystore = ds
        return self._querystore

    def _queries_iter(self):
        return iter(self._queries_store())
