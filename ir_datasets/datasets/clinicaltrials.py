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
from . import medline

_logger = ir_datasets.log.easy()

QREL_DEFS = {
    0: 'not relevant',
    1: 'possibly relevant',
    2: 'definitely relevant'
}

NAME = 'clinicaltrials'


class ClinicalTrialsDoc(NamedTuple):
    doc_id: str
    title: str
    condition: str
    summary: str
    detailed_description: str
    eligibility: str


class ClinicalTrialsDocs(BaseDocs):
    def __init__(self, name, dlcs):
        self._name = name
        self._dlcs = dlcs

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        for dlc in self._dlcs:
            with dlc.stream() as stream, tarfile.open(fileobj=stream, mode='r|gz') as tarf:
                for record in tarf:
                    if record.path.endswith('.xml'):
                        xml = tarf.extractfile(record).read()
                        yield self._parse_doc(xml)

    def _parse_doc(self, xml):
        xml = ET.fromstring(xml)
        doc_id = ''.join(xml.find('.//nct_id').itertext())
        title = xml.find('.//official_title')
        if not title:
            title = xml.find('.//brief_title')
        title = ''.join(title.itertext())
        condition = xml.find('.//condition')
        condition = ''.join(condition.itertext()) if condition else ''
        summary = xml.find('.//brief_summary')
        summary = ''.join(summary.itertext()) if summary else ''
        detailed_description = xml.find('.//detailed_description')
        detailed_description = ''.join(detailed_description.itertext()) if detailed_description else ''
        eligibility = xml.find('.//eligibility/criteria')
        eligibility = ''.join(eligibility.itertext()) if eligibility else ''
        return ClinicalTrialsDoc(doc_id, title, condition, summary, detailed_description, eligibility)

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
        return ClinicalTrialsDoc

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

    base = Dataset(documentation('_'))

    collection17 = ClinicalTrialsDocs('2017', [dlc['docs/2017']])
    collection19 = ClinicalTrialsDocs('2019', [dlc['docs/2019/0'], dlc['docs/2019/1'], dlc['docs/2019/2'], dlc['docs/2019/3']])

    subsets['2017'] = Dataset(collection17, documentation('2017'))

    subsets['2019'] = Dataset(collection19, documentation('2019'))

    subsets['2017/trec-pm-2017'] = Dataset(
        collection17,
        medline.subsets['2017/trec-pm-2017'].queries_handler(),
        TrecQrels(dlc['trec-pm-2017/qrels'], QREL_DEFS),
        documentation('trec-pm-2017')
    )

    subsets['2017/trec-pm-2018'] = Dataset(
        collection17,
        medline.subsets['2017/trec-pm-2018'].queries_handler(),
        TrecQrels(dlc['trec-pm-2018/qrels'], QREL_DEFS),
        documentation('trec-pm-2018')
    )

    subsets['2019/trec-pm-2019'] = Dataset(
        collection19,
        TrecXmlQueries(dlc['trec-pm-2019/queries'], qtype=medline.TrecPmQuery, namespace='trec-pm-2019', lang='en'),
        TrecQrels(dlc['trec-pm-2019/qrels'], QREL_DEFS),
        documentation('trec-pm-2019')
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
