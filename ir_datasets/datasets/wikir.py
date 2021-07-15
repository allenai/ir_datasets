import contextlib
from pathlib import Path
from typing import NamedTuple
import ir_datasets
from ir_datasets.util import ZipExtractCache, DownloadConfig
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import CsvQueries, CsvDocs, TrecQrels, TrecScoredDocs

NAME = 'wikir'

_logger = ir_datasets.log.easy()

QRELS_DEFS = {
    2: "Query is the article title",
    1: "There is a link to the article with the query as its title in the first sentence",
    0: "Otherwise",
}


class File:
    def __init__(self, dlc, relative_path):
        self.dlc = dlc
        self.relative_path = relative_path

    def path(self):
        return str(next(Path(self.dlc.path()).glob(self.relative_path)))

    @contextlib.contextmanager
    def stream(self):
        with open(self.path(), 'rb') as f:
            yield f


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    subsets = {}

    sources = [
        ('en1k', 369721),
        ('en59k', 2454785),
        ('fr14k', 736616),
        ('es13k', 645901),
        ('it16k', 503012),
    ]

    for source, count_hint in sources:
        source_dlc = ZipExtractCache(dlc[source], base_path/source)
        docs = CsvDocs(File(source_dlc, "*/documents.csv"), namespace=source, lang=source[:2], count_hint=count_hint)
        subsets[source] = Dataset(docs, documentation(source))
        for split in ['training', 'validation', 'test']:
            subsets[f'{source}/{split}'] = Dataset(
                docs,
                CsvQueries(File(source_dlc, f"*/{split}/queries.csv"), lang=source[:2]),
                TrecQrels(File(source_dlc, f"*/{split}/qrels"), qrels_defs=QRELS_DEFS),
                TrecScoredDocs(File(source_dlc, f"*/{split}/BM25.res")),
                documentation(f'{source}/{split}')
            )

    base = Dataset(documentation('_'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


collection, subsets = _init()
