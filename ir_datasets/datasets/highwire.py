import codecs
from typing import NamedTuple, Tuple
from zipfile import ZipFile
import lxml.html
import ir_datasets
from ir_datasets.util import DownloadConfig
from ir_datasets.formats import BaseDocs, BaseQueries, GenericQuery, BaseQrels
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.indices import PickleLz4FullStore

_logger = ir_datasets.log.easy()

SOURCES = ['ajepidem', 'ajpcell', 'ajpendometa', 'ajpgastro', 'ajpheart', 'ajplung', 'ajprenal', 'alcohol', 'andrology', 'annonc', 'bjanast', 'bjp', 'blood', 'carcinogenesis', 'cercor', 'development', 'diabetes', 'endocrinology', 'euroheartj', 'glycobiology', 'humanrep', 'humolgen', 'ijepidem', 'intimm', 'jantichemo', 'jappliedphysio', 'jbc-1995', 'jbc-1996', 'jbc-1997', 'jbc-1998', 'jbc-1999', 'jbc-2000', 'jbc-2001', 'jbc-2002', 'jbc-2003', 'jbc-2004', 'jbc-2005', 'jcb', 'jclinicalendometa', 'jcs', 'jexpbio', 'jexpmed', 'jgenphysio', 'jgenviro', 'jhistocyto', 'jnci', 'jneuro', 'mcp', 'microbio', 'molbiolevol', 'molendo', 'molhumanrep', 'nar', 'nephrodiatransp', 'peds', 'physiogenomics', 'rheumatolgy', 'rna', 'toxsci']

QREL_DEFS_06 = {
    0: 'NOT',
    1: 'POSSIBLY',
    2: 'DEFINITELY'
}
QREL_DEFS_07 = {
    0: 'NOT_RELEVANT',
    1: 'RELEVANT',
}


NAME = 'highwire'


class HighwireSpan(NamedTuple):
    start: int
    length: int
    text: str


class HighwireDoc(NamedTuple):
    doc_id: str
    journal: str
    title: str
    spans: Tuple[HighwireSpan, ...]


class TrecGenomicsQrel(NamedTuple):
    query_id: str
    doc_id: str
    span_start: int
    span_len: int
    relevance: int


class HighwireQrel(NamedTuple):
    query_id: str
    doc_id: str
    start: int
    length: int
    relevance: int


class HighwireDocs(BaseDocs):
    def __init__(self, dlcs, legalspans_dlc):
        self._dlcs = dlcs
        self._legalspans_dlc = legalspans_dlc

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        def _legalspans_iter():
            with self._legalspans_dlc.stream() as f:
                prev_did, spans = None, None
                for line in codecs.getreader('utf8')(f):
                    doc_id, start_idx, length = line.split()
                    if prev_did != doc_id:
                        if prev_did is not None:
                            yield prev_did, spans
                        prev_did, spans = doc_id, []
                    spans.append((int(start_idx), int(length)))
                yield prev_did, spans
        legalspans_iter = _legalspans_iter()

        for source in SOURCES:
            with ZipFile(self._dlcs[source].path(), 'r') as zipf:
                for record in zipf.filelist:
                    doc_id = record.filename.split('/')[-1].split('.')[0]
                    doc_raw = zipf.open(record, 'r').read()
                    legalspans_did, legalspans = next(legalspans_iter, None)
                    assert legalspans_did == doc_id
                    spans = tuple(HighwireSpan(s, l, doc_raw[s:s+l]) for s, l in legalspans)
                    # the title should be in the first span inside a <h2> element
                    title = lxml.html.document_fromstring(b'<OUTER>' + spans[0].text + b'</OUTER>')
                    title = title.xpath("//h2")
                    title = title[0].text_content() if title else ''
                    # keep just the text content within each spans
                    spans = tuple(HighwireSpan(s, l, lxml.html.document_fromstring(b'<OUTER>' + t + b'</OUTER>').text_content()) for s, l, t in spans)
                    yield HighwireDoc(doc_id, source, title, spans)

    def docs_path(self):
        return ir_datasets.util.home_path()/NAME/'corpus'

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self.docs_path()}.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
        )

    def docs_cls(self):
        return HighwireDoc

    def docs_namespace(self):
        return NAME

    def docs_count(self):
        return self.docs_store().count()

    def docs_lang(self):
        return 'en'


class TrecGenomicsQueries(BaseQueries):
    def __init__(self, queries_dlc):
        self._queries_dlc = queries_dlc

    def queries_iter(self):
        with self._queries_dlc.stream() as f:
            for line in codecs.getreader('cp1252')(f):
                if line.strip() == '':
                    continue
                doc_id, text = line[1:4], line[5:].rstrip()
                text = text.replace('[ANTIBODIES]', 'antibodies').replace('[BIOLOGICAL SUBSTANCES]', 'biological substances').replace('[CELL OR TISSUE TYPES]', 'cell or tissue types').replace('[DISEASES]', 'diseases').replace('[DRUGS]', 'drugs').replace('[GENES]', 'genes').replace('[MOLECULAR FUNCTIONS]', 'molecular functions').replace('[MUTATIONS]', 'mutations').replace('[PATHWAYS]', 'pathways').replace('[PROTEINS]', 'proteins').replace('[SIGNS OR SYMPTOMS]', 'signs or symptoms').replace('[STRAINS]', 'strains').replace('[TOXICITIES]', 'toxicities').replace('[TUMOR TYPES]', 'tumor types')
                yield GenericQuery(doc_id, text)

    def queries_cls(self):
        return GenericQuery

    def queries_namespace(self):
        return 'trec-genomics'

    def queries_lang(self):
        return 'en'


class HighwireQrels(BaseQrels):
    def __init__(self, qrels_dlc, qrel_defs):
        self._qrels_dlc = qrels_dlc
        self._qrel_defs = qrel_defs

    def qrels_iter(self):
        rev_devs = dict((v, k) for k, v in self._qrel_defs.items())
        with self._qrels_dlc.stream() as f:
            for line in codecs.getreader('utf8')(f):
                if line.startswith('#') or line.strip() == '':
                    continue
                cols = line.split()
                if len(cols) == 6: # 2006
                    query_id, doc_id, start, length, _, rel_str = cols
                elif len(cols) == 5: # 2006
                    query_id, doc_id, start, length, rel_str = cols
                else:
                    raise RuntimeError('error parsing file')
                yield HighwireQrel(query_id, doc_id, int(start), int(length), rev_devs[rel_str])

    def qrels_defs(self):
        return self._qrel_defs

    def qrels_path(self):
        return self._qrels_dlc.path()

    def qrels_cls(self):
        return HighwireQrel


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    collection = HighwireDocs(dlc, dlc['legalspans'])
    base = Dataset(collection, documentation('_'))

    subsets['trec-genomics-2006'] = Dataset(
        collection,
        TrecGenomicsQueries(dlc['trec-genomics-2006/queries']),
        HighwireQrels(dlc['trec-genomics-2006/qrels'], QREL_DEFS_06),
        documentation('trec-genomics-2006'),
    )
    subsets['trec-genomics-2007'] = Dataset(
        collection,
        TrecGenomicsQueries(dlc['trec-genomics-2007/queries']),
        HighwireQrels(dlc['trec-genomics-2007/qrels'], QREL_DEFS_07),
        documentation('trec-genomics-2007'),
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
