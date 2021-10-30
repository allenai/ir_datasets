import contextlib
from pathlib import Path
from typing import NamedTuple
import ir_datasets
from ir_datasets.util import ZipExtractCache, DownloadConfig, RelativePath
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import CsvQueries, CsvDocs, TrecQrels, TrecScoredDocs

NAME = 'wikir'

_logger = ir_datasets.log.easy()

QRELS_DEFS = {
    2: "Query is the article title",
    1: "There is a link to the article with the query as its title in the first sentence",
    0: "Otherwise",
}


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    subsets = {}

    sources = [
        ('en1k', 'wikIR1k', 369721),
        ('en59k', 'wikIR59k', 2454785),
        ('en78k', 'enwikIR', 2454785),
        ('ens78k', 'enwikIRS', 2454785),
        ('fr14k', 'FRwikIR14k', 736616),
        ('es13k', 'ESwikIR13k', 645901),
        ('it16k', 'ITwikIR16k', 503012),
    ]

    for source, zip_dir_name, count_hint in sources:
        source_dlc = ZipExtractCache(dlc[source], base_path/source)
        docs = CsvDocs(RelativePath(source_dlc, f"{zip_dir_name}/documents.csv"), namespace=source, lang=source[:2], count_hint=count_hint, docstore_path=ir_datasets.util.home_path()/NAME/f'{source}.pklz4')
        subsets[source] = Dataset(docs, documentation(source))
        for split in ['training', 'validation', 'test']:
            subsets[f'{source}/{split}'] = Dataset(
                docs,
                CsvQueries(RelativePath(source_dlc, f"{zip_dir_name}/{split}/queries.csv"), lang=source[:2]),
                TrecQrels(RelativePath(source_dlc, f"{zip_dir_name}/{split}/qrels"), qrels_defs=QRELS_DEFS),
                TrecScoredDocs(RelativePath(source_dlc, f"{zip_dir_name}/{split}/BM25.res")),
                documentation(f'{source}/{split}')
            )

    base = Dataset(documentation('_'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


collection, subsets = _init()
