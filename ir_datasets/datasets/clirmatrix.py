import contextlib
from pathlib import Path
from typing import NamedTuple
import ir_datasets
from ir_datasets.util import GzipExtract, DownloadConfig_CM
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import TsvDocs, CLIRMatrixQueries, CLIRMatrixQrels

NAME = 'clirmatrix'

_logger = ir_datasets.log.easy()

QRELS_DEFS = {
    6: "6",
    5: "5",
    4: "4",
    3: "3",
    2: "2",
    1: "1",
    0: "0",
}

def _init():
    base_path = ir_datasets.util.home_path()/NAME

    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    subsets = {}

    docs = {}
    doc_dlc = DownloadConfig_CM.context("clirmatrix_docs", base_path)
    for k in doc_dlc.contents().keys():
        doc_lcode = k.split("/")[-1]
        doc = TsvDocs(GzipExtract(doc_dlc[k]), namespace=doc_lcode, lang=doc_lcode)
        docs[doc_lcode] = doc

    for dataset in ["clirmatrix_multi8", "clirmatrix_bi139_base", "clirmatrix_bi139_full"]:
        dataset_name = dataset.split("_", 1)[-1]
        dlc = DownloadConfig_CM.context(dataset, base_path)
        for k in dlc.contents().keys():
            _, lcodes, split = k.split("/")
            query_lcode, doc_lcode = lcodes.split("_")
            qrel_dlc = GzipExtract(dlc[k])
            qrels = CLIRMatrixQrels(qrel_dlc, QRELS_DEFS)
            queries = CLIRMatrixQueries(qrel_dlc, query_lcode)
            subsets[f"{dataset_name}/{query_lcode}/{doc_lcode}/{split}"] = Dataset(
                    docs[doc_lcode],
                    qrels,
                    queries)

    base = Dataset(documentation('_'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


collection, subsets = _init()
