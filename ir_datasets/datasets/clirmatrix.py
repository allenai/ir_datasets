import json
import contextlib
from pathlib import Path
from typing import NamedTuple
import ir_datasets
from ir_datasets.util import GzipExtract, Lz4Extract, DownloadConfig, _DownloadConfig, MetadataProvider, MetadataComponent
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import TsvDocs, CLIRMatrixQueries, CLIRMatrixQrels

NAME = 'clirmatrix'

_logger = ir_datasets.log.easy()

QRELS_DEFS = {
    6: "Most relevant, based on Jenks-optimized BM25 retrieval scores in the source language",
    5: "Jenks-optimized BM25 retrieval scores in the source language",
    4: "Jenks-optimized BM25 retrieval scores in the source language",
    3: "Jenks-optimized BM25 retrieval scores in the source language",
    2: "Jenks-optimized BM25 retrieval scores in the source language",
    1: "Jenks-optimized BM25 retrieval scores in the source language",
    0: "Document not retrieved in the source language",
}

def _init():
    LANGS = ('af', 'als', 'am', 'an', 'ar', 'arz', 'ast', 'az', 'azb', 'ba', 'bar', 'be', 'bg', 'bn', 'bpy', 'br', 'bs', 'bug', 'ca', 'cdo', 'ce', 'ceb', 'ckb', 'cs', 'cv', 'cy', 'da', 'de', 'diq', 'el', 'eml', 'en', 'eo', 'es', 'et', 'eu', 'fa', 'fi', 'fo', 'fr', 'fy', 'ga', 'gd', 'gl', 'gu', 'he', 'hi', 'hr', 'hsb', 'ht', 'hu', 'hy', 'ia', 'id', 'ilo', 'io', 'is', 'it', 'ja', 'jv', 'ka', 'kk', 'kn', 'ko', 'ku', 'ky', 'la', 'lb', 'li', 'lmo', 'lt', 'lv', 'mai', 'mg', 'mhr', 'min', 'mk', 'ml', 'mn', 'mr', 'mrj', 'ms', 'my', 'mzn', 'nap', 'nds', 'ne', 'new', 'nl', 'nn', 'no', 'oc', 'or', 'os', 'pa', 'pl', 'pms', 'pnb', 'ps', 'pt', 'qu', 'ro', 'ru', 'sa', 'sah', 'scn', 'sco', 'sd', 'sh', 'si', 'simple', 'sk', 'sl', 'sq', 'sr', 'su', 'sv', 'sw', 'szl', 'ta', 'te', 'tg', 'th', 'tl', 'tr', 'tt', 'uk', 'ur', 'uz', 'vec', 'vi', 'vo', 'wa', 'war', 'wuu', 'xmf', 'yi', 'yo', 'zh')
    LANG_REGEX = '(' + '|'.join(LANGS) + ')'
    MULTI8_LANGS = ('ar', 'de', 'en', 'es', 'fr', 'ja', 'ru', 'zh')
    MULTI8_LANG_REGEX = '(' + '|'.join(MULTI8_LANGS) + ')'

    base_path = ir_datasets.util.home_path()/NAME

    base_dlc = DownloadConfig.context(NAME, base_path)

    def _dlc_init():
        with GzipExtract(base_dlc['downloads']).stream() as f:
            clirmatrix_dlc = _DownloadConfig(contents=json.load(f))
        return clirmatrix_dlc

    _dlc = ir_datasets.util.Lazy(_dlc_init)
    metadata = MetadataProvider(MetadataProvider.json_loader(Lz4Extract(base_dlc['metadata'])))

    _docs_cache = {}
    def _docs_initializer(lang_code):
        if lang_code not in _docs_cache:
            dlc = _dlc().context("clirmatrix_docs", base_path)
            docs = TsvDocs(GzipExtract(dlc[f'docs/{lang_code}']), namespace=f'{NAME}/{lang_code}', lang=lang_code)
            _docs_cache[lang_code] = docs
        return _docs_cache[lang_code]

    def _initializer(dsid, args, dlc_context=None):
        docs_lang, queries_lang, split = args
        docs = _docs_initializer(docs_lang)
        components = [docs]
        if queries_lang: # queries & split are optional
            dlc = _dlc().context(dlc_context, base_path)
            dlc_key = f'queries/{queries_lang}_{docs_lang}/{split}'
            qrel_dlc = GzipExtract(dlc[dlc_key])
            qrels = CLIRMatrixQrels(qrel_dlc, QRELS_DEFS)
            queries = CLIRMatrixQueries(qrel_dlc, queries_lang)
            components += [queries, qrels]
        result = Dataset(*components)
        result = Dataset(MetadataComponent(dsid, result, metadata), result)
        return result

    def _multi8_initializer(dsid, args):
        return _initializer(dsid, args, 'clirmatrix_multi8')

    def _bi139_base_initializer(dsid, args):
        return _initializer(dsid, args, 'clirmatrix_bi139_base')

    def _bi139_full_initializer(dsid, args):
        return _initializer(dsid, args, 'clirmatrix_bi139_full')

    def _corpus_initializer(dsid, args):
        return _initializer(dsid, (args[0], None, None))

    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    base = Dataset(documentation('_'))

    ir_datasets.registry.register(NAME, base)
    ir_datasets.registry.register_pattern(rf'^{NAME}/{LANG_REGEX}$', _corpus_initializer)
    ir_datasets.registry.register_pattern(rf'^{NAME}/{MULTI8_LANG_REGEX}/multi8/{MULTI8_LANG_REGEX}/(train|dev|test1|test2)$', _multi8_initializer)
    ir_datasets.registry.register_pattern(rf'^{NAME}/{LANG_REGEX}/bi139-base/{LANG_REGEX}/(train|dev|test1|test2)$', _bi139_base_initializer)
    ir_datasets.registry.register_pattern(rf'^{NAME}/{LANG_REGEX}/bi139-full/{LANG_REGEX}/(train|dev|test1|test2)$', _bi139_full_initializer)

    return base


collection = _init()
