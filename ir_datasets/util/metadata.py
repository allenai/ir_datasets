from functools import partial
import ir_datasets

class MetadataProvider:
    def __init__(self, dataset_id, dataset):
        self._dataset_id = dataset_id
        self._dataset = dataset
        for etype in ir_datasets.ENTITY_TYPES:
            if getattr(dataset, f'has_{etype}')():
                setattr(self, f'{etype}_metadata', partial(self._metadata, etype))
                setattr(self, f'{etype}_count', partial(self._count, etype))

    def dataset_id(self):
        return self._dataset_id

    def _metadata(self, etype):
        return ir_datasets.metadata_cached(self._dataset_id, etype)

    def _count(self, etype):
        result = None
        if hasattr(self._dataset, f'{etype}_count'):
            try:
                result = getattr(self._dataset, f'{etype}_count')() # may also return None
            except RuntimeError:
                pass # swallow error and fall back onto metadata
        if result is None:
            metadata = self._metadata(etype)
            if 'count' in metadata:
                result = metadata['count']
        return result

    def metadata(self):
        result = {}
        for etype in ir_datasets.ENTITY_TYPES:
            if getattr(self._dataset, f'has_{etype}')():
                result[etype] = self._metadata(etype)
        return result
