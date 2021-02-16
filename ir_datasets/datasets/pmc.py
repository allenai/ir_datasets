import codecs
import tarfile
import itertools
from typing import NamedTuple, Tuple
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import ir_datasets
from ir_datasets.util import DownloadConfig, GzipExtract, ZipExtract
from ir_datasets.formats import BaseDocs, GenericQuery, TrecQrels, TrecXmlQueries
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.indices import PickleLz4FullStore

_logger = ir_datasets.log.easy()

QREL_DEFS = {
    0: 'not relevant',
    1: 'possibly relevant',
    2: 'definitely relevant'
}

QUERY_FILE_MAP = {
    'number': 'query_id',
    'type': 'type',
    'description': 'description',
    'summary': 'summary',
    'note': 'note',
}


NAME = 'pmc'


class PmcDoc(NamedTuple):
    doc_id: str
    journal: str
    title: str
    abstract: str
    body: str


class TrecCdsQuery(NamedTuple):
    query_id: str
    type: str
    description: str
    summary: str


class TrecCds2016Query(NamedTuple):
    query_id: str
    type: str
    note: str
    description: str
    summary: str


class PmcDocs(BaseDocs):
    def __init__(self, dlcs, path, duplicate_dlcs=[]):
        self._dlcs = dlcs
        self._path = path
        self._duplicate_dlcs = duplicate_dlcs

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        # There's a set of known "duplicate" files, which are not considered
        # for scoring. Skip them.
        duplicate_file_names = set()
        for dlc in self._duplicate_dlcs:
            with dlc.stream() as f:
                for line in codecs.getreader('utf8')(f):
                    for fn in line.split():
                        duplicate_file_names.add(fn)
        for dlc in self._dlcs:
            with dlc.stream() as f, tarfile.open(fileobj=f, mode=f'r|gz') as tarf:
                for file in tarf:
                    if not file.isfile() or file.name in duplicate_file_names:
                        continue
                    xml = tarf.extractfile(file).read()
                    # Some files have a problem where spaces are missing between tag and attributes.
                    # Fix those here.
                    xml = xml.replace(b'<xrefref-type=', b'<xref ref-type=')
                    xml = xml.replace(b'<tex-mathid=', b'<tex-math id=')
                    xml = xml.replace(b'<graphicxlink:href=', b'<graphic xlink:href=')
                    xml = xml.replace(b'<ext-linkext-link-type=', b'<ext-link ext-link-type=')
                    xml = xml.replace(b'<pub-idpub-id-type=', b'<pub-id pub-id-type=')
                    # Extract relevant parts from the XML
                    xml = ET.fromstring(xml)
                    doc_id = file.name.split('/')[-1].split('.')[0]
                    journal = xml.find('.//journal-title')
                    journal = '\n'.join(journal.itertext()) if journal is not None else ''
                    title = xml.find('.//article-title')
                    title = '\n'.join(title.itertext()) if title is not None else ''
                    abstract = xml.find('.//abstract')
                    abstract = '\n'.join(abstract.itertext()) if abstract is not None else ''
                    body = xml.find('.//body')
                    body = '\n'.join(body.itertext()) if body is not None else ''
                    yield PmcDoc(doc_id, journal, title, abstract, body)

    def docs_path(self):
        return self._path

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self.docs_path()}.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
        )

    def docs_cls(self):
        return PmcDoc

    def docs_namespace(self):
        return NAME

    def docs_count(self):
        return self.docs_store().count()

    def docs_lang(self):
        return 'en'


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    v1_collection = PmcDocs([dlc['v1/source0'], dlc['v1/source1'], dlc['v1/source2'], dlc['v1/source3']], ir_datasets.util.home_path()/NAME/'v1'/'corpus', duplicate_dlcs=[dlc['v1/dup1'], dlc['v1/dup2']])
    v2_collection = PmcDocs([dlc['v2/source0'], dlc['v2/source1'], dlc['v2/source2'], dlc['v2/source3']], ir_datasets.util.home_path()/NAME/'v2'/'corpus')
    base = Dataset(documentation('_'))

    subsets['v1'] = Dataset(v1_collection, documentation('v1'))
    subsets['v2'] = Dataset(v2_collection, documentation('v2'))

    subsets['v1/trec-cds-2014'] = Dataset(
        v1_collection,
        TrecXmlQueries(dlc['trec-cds-2014/queries'], TrecCdsQuery, QUERY_FILE_MAP, namespace='trec-cds-2014', lang='en'),
        TrecQrels(dlc['trec-cds-2014/qrels'], QREL_DEFS),
        documentation('v1/trec-cds-2014'),
    )

    subsets['v1/trec-cds-2015'] = Dataset(
        v1_collection,
        TrecXmlQueries(dlc['trec-cds-2015/queries'], TrecCdsQuery, QUERY_FILE_MAP, namespace='trec-cds-2015', lang='en'),
        TrecQrels(dlc['trec-cds-2015/qrels'], QREL_DEFS),
        documentation('v1/trec-cds-2015'),
    )

    subsets['v2/trec-cds-2016'] = Dataset(
        v2_collection,
        TrecXmlQueries(dlc['trec-cds-2016/queries'], TrecCds2016Query, QUERY_FILE_MAP, namespace='trec-cds-2016', lang='en'),
        TrecQrels(dlc['trec-cds-2016/qrels'], QREL_DEFS),
        documentation('v2/trec-cds-2016'),
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
