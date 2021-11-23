from typing import NamedTuple
import ir_datasets
from ir_datasets.util import GzipExtract, DownloadConfig
from ir_datasets.formats import TrecQrels, TrecDocs, TrecQueries
from ir_datasets.datasets.base import Dataset, YamlDocumentation


NAME = 'trec-spanish'

class TrecDescOnlyQuery(NamedTuple):
    query_id: str
    description: str

class TrecSpanish3Query(NamedTuple):
    query_id: str
    title_es: str
    title_en: str
    description_es: str
    description_en: str
    narrative_es: str
    narrative_en: str

class TrecSpanish4Query(NamedTuple):
    query_id: str
    description_es1: str
    description_en1: str
    description_es2: str
    description_en2: str

QREL_DEFS = {
    1: 'relevant',
    0: 'not relevant',
}

QTYPE_MAP_3 = {
    '<num> *(Number:)? *SP': 'query_id', # Remove SP prefix from QIDs
    '<title> *(Topic:)?': 'title',
    '<desc> *(Description:)?': 'description',
    '<narr> *(Narrative:)?': 'narrative',
}

QTYPE_MAP_4 = {
    '<num> *(Number:)? *SP': 'query_id', # Remove SP prefix from QIDs
    '<desc> *(Description:)?': 'description',
}

# TREC Spanish has this strange convention where lines that start with ** are
# translations of the query. Rather than trying to bake this into TrecQueries,
# I'm using an adapter to apply this just for TREC Spanish.
class TrecSpanishTranslateQueries:
    def __init__(self, parent, query_cls):
        self._parent = parent
        self._query_cls = query_cls

    def __getattr__(self, attr):
        return getattr(self._parent, attr)

    def queries_iter(self):
        qcls = self._query_cls
        for query in self._parent.queries_iter():
            qid = query.query_id
            tup = [qid,]
            for value in query[1:]:
                tup.append('')
                for line in value.split('\n'):
                    if line.strip() == '':
                        tup[-1] = tup[-1].strip()
                        tup.append('')
                    # Translations begin with **
                    if line.lstrip().startswith('**'):
                        line = line.lstrip()[2:]
                    tup[-1] += line.strip() + ' '
            # Sometimes not all translations are available. Fill in remaining with blanks
            tup += [''] * (len(qcls._fields) - len(tup))
            yield qcls(*tup)

    def queries_cls(self):
        return self._query_cls


def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    collection = TrecDocs(dlc['docs'], encoding='ISO-8859-1', path_globs=['**/afp_text/af*', '**/infosel_data/ism_*'], namespace=NAME, lang='es', count_hint=ir_datasets.util.count_hint(NAME))

    base = Dataset(collection, documentation('_'))

    subsets['trec3'] = Dataset(
        TrecSpanishTranslateQueries(TrecQueries(GzipExtract(dlc['trec3/queries']), qtype_map=QTYPE_MAP_3, encoding='ISO-8859-1', namespace=NAME, lang=None), TrecSpanish3Query),
        TrecQrels(GzipExtract(dlc['trec3/qrels']), QREL_DEFS),
        collection,
        documentation('trec3'))

    subsets['trec4'] = Dataset(
        TrecSpanishTranslateQueries(TrecQueries(GzipExtract(dlc['trec4/queries']), qtype=TrecDescOnlyQuery, qtype_map=QTYPE_MAP_4, encoding='ISO-8859-1', namespace=NAME, lang=None), TrecSpanish4Query),
        TrecQrels(GzipExtract(dlc['trec4/qrels']), QREL_DEFS),
        collection,
        documentation('trec4'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
