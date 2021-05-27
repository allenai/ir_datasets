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
    LANGS = ('af', 'als', 'am', 'an', 'ar', 'arz', 'ast', 'az', 'azb', 'ba', 'bar', 'be', 'bg', 'bn', 'bpy', 'br', 'bs', 'bug', 'ca', 'cdo', 'ce', 'ceb', 'ckb', 'cs', 'cv', 'cy', 'da', 'de', 'diq', 'el', 'eml', 'en', 'eo', 'es', 'et', 'eu', 'fa', 'fi', 'fo', 'fr', 'fy', 'ga', 'gd', 'gl', 'gu', 'he', 'hi', 'hr', 'hsb', 'ht', 'hu', 'hy', 'ia', 'id', 'ilo', 'io', 'is', 'it', 'ja', 'jv', 'ka', 'kk', 'kn', 'ko', 'ku', 'ky', 'la', 'lb', 'li', 'lmo', 'lt', 'lv', 'mai', 'mg', 'mhr', 'min', 'mk', 'ml', 'mn', 'mr', 'mrj', 'ms', 'my', 'mzn', 'nap', 'nds', 'ne', 'new', 'nl', 'nn', 'no', 'oc', 'or', 'os', 'pa', 'pl', 'pms', 'pnb', 'ps', 'pt', 'qu', 'ro', 'ru', 'sa', 'sah', 'scn', 'sco', 'sd', 'sh', 'si', 'simple', 'sk', 'sl', 'sq', 'sr', 'su', 'sv', 'sw', 'szl', 'ta', 'te', 'tg', 'th', 'tl', 'tr', 'tt', 'uk', 'ur', 'uz', 'vec', 'vi', 'vo', 'wa', 'war', 'wuu', 'xmf', 'yi', 'yo', 'zh')
    LANG_REGEX = '(' + '|'.join(LANGS) + ')'

    base_path = ir_datasets.util.home_path()/NAME

    def _dlc_init():
        return DownloadConfig_CM

    _dlc = ir_datasets.util.Lazy(_dlc_init)


    _docs_cache = {}
    def _docs_initializer(lang_code):
        if lang_code not in _docs_cache:
            dlc = _dlc().context("clirmatrix_docs", base_path)
            docs = TsvDocs(GzipExtract(dlc[f'docs/{lang_code}']), namespace=f'{NAME}/{lang_code}', lang=lang_code)
            _docs_cache[lang_code] = docs
        return _docs_cache[lang_code]


    def _multi8_initializer(args):
        dlc = _dlc().context('clirmatrix_multi8', base_path)
        queries_lang, docs_lang, split = args
        docs = _docs_initializer(docs_lang)
        dlc_key = f'queries/{queries_lang}_{docs_lang}/{split}'
        qrel_dlc = GzipExtract(dlc[dlc_key])
        qrels = CLIRMatrixQrels(qrel_dlc, QRELS_DEFS)
        queries = CLIRMatrixQueries(qrel_dlc, queries_lang)
        return Dataset(docs, qrels, queries)

    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    subsets = {}

    # docs = {}
    # doc_dlc = DownloadConfig_CM.context("clirmatrix_docs", base_path)
    # for k in doc_dlc.contents().keys():
    #     doc_lcode = k.split("/")[-1]
    #     doc = TsvDocs(GzipExtract(doc_dlc[k]), namespace=doc_lcode, lang=doc_lcode)
    #     docs[doc_lcode] = doc

    # for dataset in ["clirmatrix_multi8", "clirmatrix_bi139_base", "clirmatrix_bi139_full"]:
    #     dataset_name = dataset.split("_", 1)[-1]
    #     dlc = DownloadConfig_CM.context(dataset, base_path)
    #     for k in dlc.contents().keys():
    #         _, lcodes, split = k.split("/")
    #         query_lcode, doc_lcode = lcodes.split("_")
    #         qrel_dlc = GzipExtract(dlc[k])
    #         qrels = CLIRMatrixQrels(qrel_dlc, QRELS_DEFS)
    #         queries = CLIRMatrixQueries(qrel_dlc, query_lcode)
    #         subsets[f"{dataset_name}/{query_lcode}/{doc_lcode}/{split}"] = Dataset(
    #                 docs[doc_lcode],
    #                 qrels,
    #                 queries)

    base = Dataset(documentation('_'))

    ir_datasets.registry.register(NAME, base)
    ir_datasets.registry.register_pattern(rf'{NAME}/multi8/{LANG_REGEX}/{LANG_REGEX}/(train|dev|test1|test2)', _multi8_initializer)
    # for s in sorted(subsets):
    #     ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


collection, subsets = _init()
