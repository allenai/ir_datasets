import codecs
import itertools
from typing import NamedTuple, Tuple
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import ir_datasets
from ir_datasets.util import DownloadConfig, GzipExtract, ZipExtract
from ir_datasets.formats import BaseDocs, BaseQueries, GenericQuery, TrecQrels, TrecXmlQueries
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.indices import PickleLz4FullStore
from .highwire import TrecGenomicsQueries

_logger = ir_datasets.log.easy()

QREL_DEFS = {
    0: 'not relevant',
    1: 'possibly relevant',
    2: 'definitely relevant'
}

TREC04_XML_MAP = {
    'ID': 'query_id',
    'TITLE': 'title',
    'NEED': 'need',
    'CONTEXT': 'context',
}


NAME = 'medline'


class MedlineDoc(NamedTuple):
    doc_id: str
    title: str
    abstract: str


class TrecGenomicsQuery(NamedTuple):
    query_id: str
    title: str
    need: str
    context: str


class MedlineDocs(BaseDocs):
    def __init__(self, dlcs):
        self._dlcs = dlcs

    def docs_iter(self):
        return iter(self.docs_store())

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        for dlc in self._dlcs:
            with dlc.stream() as f:
                for _ in itertools.takewhile(lambda x: not x.startswith(b'<MedlineCitation '), f):
                    pass # skip until we get to the first document
                f = itertools.chain([b'<MedlineCitation '], f)
                while next(f, b'').startswith(b'<MedlineCitation '):
                    xml = b''.join(itertools.takewhile(lambda x: x != b'</MedlineCitation>\n', f))
                    xml = ET.fromstring(b'<OUTER>' + xml + b'</OUTER>')
                    doc_id = xml.find('PMID').text
                    xarticle = xml.find('Article')
                    title = xarticle.find('ArticleTitle').text
                    abstract = xarticle.find('Abstract').find('AbstractText').text if xarticle.find('Abstract') is not None else ''
                    yield MedlineDoc(doc_id, title, abstract)

    def docs_path(self):
        return ir_datasets.util.home_path()/NAME/'corpus'

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self.docs_path()}.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
        )

    def docs_cls(self):
        return MedlineDoc

    def docs_namespace(self):
        return NAME

    def docs_count(self):
        return self.docs_store().count()


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    collection = MedlineDocs([GzipExtract(dlc['docs/a']), GzipExtract(dlc['docs/b']), GzipExtract(dlc['docs/c']), GzipExtract(dlc['docs/d'])])
    base = Dataset(collection, documentation('_'))

    subsets['trec-genomics-2004'] = Dataset(
        collection,
        TrecXmlQueries(ZipExtract(dlc['trec-genomics-2004/queries'], 'Official.xml'), qtype=TrecGenomicsQuery, qtype_map=TREC04_XML_MAP, namespace='trec-genomics'),
        TrecQrels(dlc['trec-genomics-2004/qrels'], QREL_DEFS),
        documentation('trec-genomics-2004'),
    )
    subsets['trec-genomics-2005'] = Dataset(
        collection,
        TrecGenomicsQueries(dlc['trec-genomics-2005/queries']),
        TrecQrels(dlc['trec-genomics-2005/qrels'], QREL_DEFS),
        documentation('trec-genomics-2005'),
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
