import ir_datasets

class MetadataProvider:
    def __init__(self, dataset_id):
        self._dataset_id = dataset_id

    def dataset_id(self):
        return self._dataset_id

    def docs_metadata(self):
        return ir_datasets.metadata_cached(self._dataset_id, 'docs')

    def queries_metadata(self):
        return ir_datasets.metadata_cached(self._dataset_id, 'queries')

    def qrels_metadata(self):
        return ir_datasets.metadata_cached(self._dataset_id, 'qrels')

    def scoreddocs_metadata(self):
        return ir_datasets.metadata_cached(self._dataset_id, 'scoreddocs')

    def docpairs_metadata(self):
        return ir_datasets.metadata_cached(self._dataset_id, 'docpairs')

    def qlogs_metadata(self):
        return ir_datasets.metadata_cached(self._dataset_id, 'qlogs')
