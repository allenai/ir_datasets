import json
import codecs
from typing import NamedTuple, Dict
import ir_datasets
from ir_datasets.util import TarExtractAll, RelativePath, GzipExtract
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries
from ir_datasets.formats import TsvQueries, BaseDocs, TrecQrels, GenericDoc
from ir_datasets.indices import PickleLz4FullStore

_logger = ir_datasets.log.easy()


NAME = 'mr-tydi'

QREL_DEFS = {
    1: "Passage identified within Wikipedia article from top Google search results"
}


class MrTydiDocs(BaseDocs):
    def __init__(self, dlc, lang, count_hint=None):
        super().__init__()
        self._dlc = dlc
        self._count_hint = count_hint
        self._lang = lang

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
            path=f'{ir_datasets.util.home_path()/NAME/self._lang}/collection.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=self._count_hint,
        )

    def docs_count(self):
        return self.docs_store().count()

    def docs_namespace(self):
        return f'{NAME}/{self._lang}'

    def docs_lang(self):
        return self._lang


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    base = Dataset(documentation('_'))

    subsets = {}

    langs = {
        'ar': ('mrtydi-v1.0-arabic', 2106586),
        'bn': ('mrtydi-v1.0-bengali', 304059),
        'en': ('mrtydi-v1.0-english', 32907100),
        'fi': ('mrtydi-v1.0-finnish', 1908757),
        'id': ('mrtydi-v1.0-indonesian', 1469399),
        'ja': ('mrtydi-v1.0-japanese', 7000027),
        'ko': ('mrtydi-v1.0-korean', 1496126),
        'ru': ('mrtydi-v1.0-russian', 9597504),
        'sw': ('mrtydi-v1.0-swahili', 136689),
        'te': ('mrtydi-v1.0-telugu', 548224),
        'th': ('mrtydi-v1.0-thai', 568855),
    }

    for lang, (file_name, count_hint) in langs.items():
        dlc_ds = TarExtractAll(dlc[lang], base_path/lang)
        docs = MrTydiDocs(GzipExtract(RelativePath(dlc_ds, f'{file_name}/collection/docs.jsonl.gz')), lang, count_hint=count_hint)
        subsets[lang] = Dataset(
            docs,
            TsvQueries(RelativePath(dlc_ds, f'{file_name}/topic.tsv'), lang=lang),
            TrecQrels(RelativePath(dlc_ds, f'{file_name}/qrels.txt'), QREL_DEFS),
            documentation(lang)
        )
        subsets[f'{lang}/train'] = Dataset(
            docs,
            TsvQueries(RelativePath(dlc_ds, f'{file_name}/topic.train.tsv'), lang=lang),
            TrecQrels(RelativePath(dlc_ds, f'{file_name}/qrels.train.txt'), QREL_DEFS),
            documentation(f'{lang}/train')
        )
        subsets[f'{lang}/dev'] = Dataset(
            docs,
            TsvQueries(RelativePath(dlc_ds, f'{file_name}/topic.dev.tsv'), lang=lang),
            TrecQrels(RelativePath(dlc_ds, f'{file_name}/qrels.dev.txt'), QREL_DEFS),
            documentation(f'{lang}/dev')
        )
        subsets[f'{lang}/test'] = Dataset(
            docs,
            TsvQueries(RelativePath(dlc_ds, f'{file_name}/topic.test.tsv'), lang=lang),
            TrecQrels(RelativePath(dlc_ds, f'{file_name}/qrels.test.txt'), QREL_DEFS),
            documentation(f'{lang}/test')
        )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
