import json
import codecs
from typing import NamedTuple, Dict
import ir_datasets
from ir_datasets.util import TarExtractAll, RelativePath, GzipExtract, Migrator
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
            path=f'{ir_datasets.util.home_path()/NAME/self._lang}.pklz4',
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
        'ar': 'mrtydi-v1.0-arabic',
        'bn': 'mrtydi-v1.0-bengali',
        'en': 'mrtydi-v1.0-english',
        'fi': 'mrtydi-v1.0-finnish',
        'id': 'mrtydi-v1.0-indonesian',
        'ja': 'mrtydi-v1.0-japanese',
        'ko': 'mrtydi-v1.0-korean',
        'ru': 'mrtydi-v1.0-russian',
        'sw': 'mrtydi-v1.0-swahili',
        'te': 'mrtydi-v1.0-telugu',
        'th': 'mrtydi-v1.0-thai',
    }

    migrator = Migrator(base_path/'irds_version.txt', 'v2',
        affected_files=[base_path/lang for lang in langs],
        message='Migrating mr-tydi (restructuring directory)')

    for lang, file_name in langs.items():
        dlc_ds = TarExtractAll(dlc[lang], f'{base_path/lang}.data')
        docs = MrTydiDocs(GzipExtract(RelativePath(dlc_ds, f'{file_name}/collection/docs.jsonl.gz')), lang, count_hint=ir_datasets.util.count_hint(f'{NAME}/{lang}'))
        docs = migrator(docs)
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
