import json
from typing import Callable, Optional, Dict, Any
from functools import partial
import ir_datasets
from .fileio import PackageDataFile


class MetadataComponent:
    def __init__(self, dataset_id, dataset, provider=None):
        self._dataset_id = dataset_id
        self._dataset = dataset
        self._metadata_provider = provider if provider is not None else default_metadata_provider
        for etype in ir_datasets.EntityType:
            if dataset.has(etype):
                setattr(self, f'{etype.value}_metadata', partial(self._metadata, etype))
                setattr(self, f'{etype.value}_count', partial(self._count, etype))

    def dataset_id(self):
        return self._dataset_id

    def metadata(self):
        result = {}
        for etype in ir_datasets.EntityType:
            if self._dataset.has(etype):
                result[etype.value] = self._metadata(etype)
        return result

    def _metadata(self, etype: ir_datasets.EntityType):
        return self._metadata_provider.get_metadata(self._dataset_id, etype)

    def _count(self, etype):
        result = None
        if hasattr(self._dataset, f'{etype.value}_count'):
            try:
                result = getattr(self._dataset, f'{etype.value}_count')() # may also return None
            except RuntimeError:
                pass # swallow error and fall back onto metadata
            except FileNotFoundError:
                pass # swallow error and fall back onto metadata
        if result is None:
            metadata = self._metadata(etype)
            if 'count' in metadata:
                result = metadata['count']
        return result


class MetadataProvider:
    def __init__(self, metadata_loader: Callable[[], Dict[str, Any]]):
        self._metadata = None
        self._metadata_loader = metadata_loader

    def get_metadata(self, dsid: str, entity_type: ir_datasets.EntityType) -> Dict[str, Any]:
        entity_type = ir_datasets.EntityType(entity_type) # validate & allow strings
        if self._metadata is None:
            self._metadata = self._metadata_loader()
        result = self._metadata.get(dsid, {}).get(entity_type.value, {})
        while '_ref' in result:
            result = self._metadata.get(result['_ref'], {}).get(entity_type.value, {})
        return result

    @staticmethod
    def json_loader(dlc):
        def wrapped():
            with dlc.stream() as s:
                return json.load(s)
        return wrapped


default_metadata_provider = MetadataProvider(MetadataProvider.json_loader(PackageDataFile('etc/metadata.json')))


def count_hint(
    dsid: str,
    etype: ir_datasets.EntityType = ir_datasets.EntityType.docs,
    metadata_provier: Optional[MetadataProvider] = None) -> Callable[[], Optional[int]]:
    """
    Returns a lambda expression that provides the count from metadata (if available) for
    the given dataset's etype. This is frequently used to provide a time estimate for
    building docstores. It returns a lambda expression so that the metadata does not
    need to be loaded when the package is imported; only when the value is actually
    requested.
    """
    if metadata_provier is None:
        metadata_provier = default_metadata_provider
    return lambda: metadata_provier.get_metadata(dsid, etype).get('count')
