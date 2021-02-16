import codecs
import itertools
import io
import gzip
from contextlib import ExitStack
import itertools
from typing import NamedTuple, Tuple
import tarfile
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


class TrecPm2017Query(NamedTuple):
    query_id: str
    disease: str
    gene: str
    demographic: str
    other: str


class TrecPmQuery(NamedTuple):
    query_id: str
    disease: str
    gene: str
    demographic: str


class ConcatFile:
    """
    Simulates a sequence of file-like objects that are cat'd.
    Only supports read operations.
    """
    def __init__(self, files):
        self.file_iter = files
        self.file = next(self.file_iter)
    def read(self, count=None):
        result = b''
        while not result and self.file is not None:
            result = self.file.read(count)
            if not result:
                self.file = next(self.file_iter, None)
        return result


class MedlineDocs(BaseDocs):
    def __init__(self, name, dlcs):
        self._name = name
        self._dlcs = dlcs

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        with ExitStack() as stack:
            if self._name == '2004':
                # The files for 2004 are a large XML file that's split internally.
                # Simulate one big file for the parser below.
                EOF = io.BytesIO(b'\n</MedlineCitationSet>')
                files = [ConcatFile(itertools.chain(
                    (stack.enter_context(dlc.stream()) for dlc in self._dlcs),
                    (EOF,)
                ))]
            elif self._name == '2017':
                # The files for 2017 are individual files in a big tar file. Generate
                # a file for each.
                def _files():
                    for dlc in self._dlcs:
                        with dlc.stream() as f:
                            tarf = stack.enter_context(tarfile.open(fileobj=f, mode=f'r|gz'))
                            for r in tarf:
                                if r.isfile() and r.name.endswith('.gz'):
                                    yield gzip.GzipFile(fileobj=tarf.extractfile(r), mode='r')
                files = _files()
            else:
                raise ValueError(f'unknown {self._name}')
            for file in files:
                for _, el in ET.iterparse(file, events=['end']):
                    if el.tag == 'MedlineCitation':
                        doc_id = el.find('.//PMID').text
                        title = el.find('.//ArticleTitle')
                        abstract = el.find('.//AbstractText')
                        yield MedlineDoc(doc_id, title.text if title is not None else '', abstract.text if abstract is not None else '')
                    if el.tag in ('PubmedArticle', 'MedlineCitation'):
                        el.clear() # so we don't need to keep it all in memory

    def docs_path(self):
        return ir_datasets.util.home_path()/NAME/self._name/'corpus'

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

    def docs_lang(self):
        return 'en'


class AacrAscoDocs(BaseDocs):
    def __init__(self, dlc):
        self._dlc = dlc

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        with self._dlc.stream() as f, tarfile.open(fileobj=f, mode=f'r|gz') as tarf:
            for file in tarf:
                if not file.isfile():
                    continue
                file_reader = tarf.extractfile(file)
                file_reader = codecs.getreader('utf8')(file_reader)
                doc_id = file.name.split('/')[-1].split('.')[0]
                meeting = next(file_reader)
                title = ''
                for line in file_reader:
                    title += line
                    if title.endswith('\n\n'):
                        break
                assert title.startswith('Title:')
                title = title[len('Title:'):].strip()
                abstract = file_reader.read().strip()
                yield MedlineDoc(doc_id, title, abstract)

    def docs_path(self):
        return ir_datasets.util.home_path()/NAME/'2017'/'corpus'

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

    def docs_lang(self):
        return 'en'


class ConcatDocs(BaseDocs):
    def __init__(self, docs):
        self._docs = docs

    def docs_iter(self):
        return iter(self.docs_store())

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        for docs in self._docs:
            yield from docs.docs_iter()

    def docs_path(self):
        return f'{self._docs[0].docs_path()}.concat'

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self.docs_path()}.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
        )

    def docs_cls(self):
        return self._docs[0].docs_cls()

    def docs_namespace(self):
        return self._docs[0].docs_namespace()

    def docs_lang(self):
        return self._docs[0].docs_lang()

    def docs_count(self):
        return self.docs_store().count()


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    base = Dataset(documentation('_'))

    collection04 = MedlineDocs('2004', [GzipExtract(dlc['2004/a']), GzipExtract(dlc['2004/b']), GzipExtract(dlc['2004/c']), GzipExtract(dlc['2004/d'])])

    subsets['2004'] = Dataset(collection04, documentation('2004'))

    subsets['2004/trec-genomics-2004'] = Dataset(
        collection04,
        TrecXmlQueries(ZipExtract(dlc['trec-genomics-2004/queries'], 'Official.xml'), qtype=TrecGenomicsQuery, qtype_map=TREC04_XML_MAP, namespace='trec-genomics', lang='en'),
        TrecQrels(dlc['trec-genomics-2004/qrels'], QREL_DEFS),
        documentation('trec-genomics-2004'),
    )
    subsets['2004/trec-genomics-2005'] = Dataset(
        collection04,
        TrecGenomicsQueries(dlc['trec-genomics-2005/queries']),
        TrecQrels(dlc['trec-genomics-2005/qrels'], QREL_DEFS),
        documentation('trec-genomics-2005'),
    )

    collection17 = ConcatDocs([
        AacrAscoDocs(dlc['2017/aacr_asco_extra']),
        MedlineDocs('2017', [dlc['2017/part1'], dlc['2017/part2'], dlc['2017/part3'], dlc['2017/part4'], dlc['2017/part5']]),
    ])
    subsets['2017'] = Dataset(collection17, documentation('2017'))

    subsets['2017/trec-pm-2017'] = Dataset(
        collection17,
        TrecXmlQueries(dlc['trec-pm-2017/queries'], qtype=TrecPm2017Query, namespace='trec-pm-2017', lang='en'),
        TrecQrels(dlc['trec-pm-2017/qrels'], QREL_DEFS),
        documentation('trec-pm-2017'),
    )
    subsets['2017/trec-pm-2018'] = Dataset(
        collection17,
        TrecXmlQueries(dlc['trec-pm-2018/queries'], qtype=TrecPmQuery, namespace='trec-pm-2018', lang='en'),
        TrecQrels(dlc['trec-pm-2018/qrels'], QREL_DEFS),
        documentation('trec-pm-2018'),
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
