from itertools import chain
from typing import Dict
import json

import ir_datasets
from ir_datasets import registry
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import BaseDocs, GenericDoc
from ir_datasets.util import DownloadConfig, home_path, Cache, GzipExtract
from ir_datasets.indices import PickleLz4FullStore

NAME = "msmarco-document-anchor-text"

SUBSETS = {
    'v1': (1703834, "en"),
    'v2': (4821244, "en"),
}


class MsMarcoAnchorTextDocs(BaseDocs):
    def __init__(self, dlc, version, lang, count_hint=None):
        super().__init__()
        self._dlc = dlc
        self._version = version
        self._lang = lang
        self._count_hint = count_hint

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                yield GenericDoc(data['id'], data['contents'])

    def docs_cls(self):
        return GenericDoc

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME/self._version}.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=self._count_hint,
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return f'{NAME}/{self._version}'

    def docs_lang(self):
        return self._lang


def _init():
    base_path = home_path() / (NAME)

    documentation = YamlDocumentation(f"docs/{NAME}.yaml")
    download_config = DownloadConfig.context(NAME, base_path)

    base = Dataset(documentation('_'))

    documents = {
        name: MsMarcoAnchorTextDocs(
            Cache(GzipExtract(download_config[name]),
                base_path / f"{name}.json"
            ),
            version=name,
            lang=language,
            count_hint=count_hint
        )
        for name, (count_hint, language)
        in SUBSETS.items()
    }

    # Wrap in datasets with documentation.
    datasets = {
        name: Dataset(documents, documentation(name))
        for name, documents in documents.items()
    }

    # Register datasets.
    registry.register(NAME, base)
    for name, documents in datasets.items():
        registry.register(f'{NAME}/{name}', documents)

    return base, datasets


base, datasets = _init()

