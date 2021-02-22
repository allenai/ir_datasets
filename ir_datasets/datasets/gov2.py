import io
import os
import gzip
import codecs
from collections import Counter
from contextlib import contextmanager, ExitStack
from pathlib import Path
from typing import NamedTuple
from glob import glob
import ir_datasets
from ir_datasets.util import DownloadConfig, GzipExtract, TarExtract
from ir_datasets.formats import TrecQrels, TrecQueries, TrecColonQueries, BaseDocs, GenericQuery, BaseQrels, TrecPrels
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.indices import Docstore


_logger = ir_datasets.log.easy()


NAME = 'gov2'


QREL_DEFS = {
    2: 'Highly Relevant',
    1: 'Relevant',
    0: 'Not Relevant',
}

NAMED_PAGE_QREL_DEFS = {
    1: 'Relevant',
    0: 'Not Relevant',
}

NAMED_PAGE_QTYPE_MAP = {
    '<num> *(Number:)? *NP': 'query_id', # Remove NP prefix from QIDs
    '<title> *(Topic:)?': 'text',
}

EFF_MAP_05 = {'751': '1192', '752': '1330', '753': '5956', '754': '6303', '755': '6939', '756': '7553', '757': '8784', '758': '9121', '759': '9266', '760': '10359', '761': '10406', '762': '11597', '763': '12750', '764': '15502', '765': '16895', '766': '17279', '767': '17615', '768': '18050', '769': '18678', '770': '19280', '771': '19963', '772': '20766', '773': '21329', '774': '21513', '775': '23212', '776': '24289', '777': '24781', '778': '24813', '779': '26593', '780': '27428', '781': '28120', '782': '28627', '783': '29561', '784': '33379', '785': '33820', '786': '34135', '787': '35192', '788': '36242', '789': '36530', '790': '36616', '791': '36738', '792': '37111', '793': '41088', '794': '41192', '795': '41506', '796': '44506', '797': '45081', '798': '47993', '799': '48890', '800': '49462'}
EFF_MAP_06 = {'801': '62937', '802': '63569', '803': '63582', '804': '63641', '805': '64227', '806': '64266', '807': '64310', '808': '64642', '809': '64687', '810': '64704', '811': '64723', '812': '64741', '813': '64752', '814': '64938', '815': '65024', '816': '65070', '817': '65222', '818': '65335', '819': '65486', '820': '65504', '821': '65599', '822': '65821', '823': '65826', '824': '65950', '825': '66084', '826': '66409', '827': '66725', '828': '67326', '829': '67531', '830': '67550', '831': '67782', '832': '67961', '833': '68322', '834': '68492', '835': '68967', '836': '69028', '837': '69127', '838': '69401', '839': '69552', '840': '69564', '841': '69935', '842': '70033', '843': '70041', '844': '70285', '845': '70579', '846': '70707', '847': '70751', '848': '70815', '849': '70935', '850': '71136'}

class Gov2Doc(NamedTuple):
    doc_id: str
    url: str
    http_headers: str
    body: bytes
    body_content_type: str



class Gov2DocIter:
    def __init__(self, gov2_docs, slice):
        self.gov2_docs = gov2_docs
        self.slice = slice
        self.next_index = 0
        self.file_iter = gov2_docs._docs_iter_source_files()
        self.current_file = None
        self.current_file_start_idx = 0
        self.current_file_end_idx = 0

    def __next__(self):
        if self.slice.start >= self.slice.stop:
            raise StopIteration
        while self.next_index != self.slice.start or self.current_file is None or self.current_file_end_idx <= self.slice.start:
            if self.current_file is None or self.current_file_end_idx <= self.slice.start:
                # First iteration or no docs remaining in this file
                if self.current_file is not None:
                    self.current_file.close()
                    self.current_file = None
                # jump ahead to the file that contains the desired index
                first = True
                while first or self.current_file_end_idx < self.slice.start:
                    source_file = next(self.file_iter)
                    self.next_index = self.current_file_end_idx
                    self.current_file_start_idx = self.current_file_end_idx
                    self.current_file_end_idx = self.current_file_start_idx + self.gov2_docs._docs_file_counts()[source_file]
                    first = False
                self.current_file = self.gov2_docs._docs_ctxt_iter_gov2(source_file)
            else:
                for _ in zip(range(self.slice.start - self.next_index), self.current_file):
                    # The zip here will stop at after either as many docs we must advance, or however
                    # many docs remain in the file. In the latter case, we'll just drop out into the
                    # next iteration of the while loop and pick up the next file.
                    self.next_index += 1
        result = next(self.current_file)
        self.next_index += 1
        self.slice = slice(self.slice.start + (self.slice.step or 1), self.slice.stop, self.slice.step)
        return result

    def close(self):
        self.file_iter = None

    def __iter__(self):
        return self

    def __del__(self):
        self.close()

    def __getitem__(self, key):
        if isinstance(key, slice):
            # it[start:stop:step]
            new_slice = ir_datasets.util.apply_sub_slice(self.slice, key)
            return Gov2DocIter(self.gov2_docs, new_slice)
        elif isinstance(key, int):
            # it[index]
            new_slice = ir_datasets.util.slice_idx(self.slice, key)
            new_it = Gov2DocIter(self.gov2_docs, new_slice)
            try:
                return next(new_it)
            except StopIteration as e:
                raise IndexError((self.slice, slice(key, key+1), new_slice))
        raise TypeError('key must be int or slice')


class Gov2Docs(BaseDocs):
    def __init__(self, docs_dlc, doccount_dlc):
        super().__init__()
        self.docs_dlc = docs_dlc
        self._doccount_dlc = doccount_dlc
        self._docs_file_counts_cache = None

    def docs_path(self):
        return self.docs_dlc.path()

    def _docs_iter_source_files(self):
        dirs = sorted((Path(self.docs_dlc.path()) / 'GOV2_data').glob('GX???'))
        for source_dir in dirs:
            for source_file in sorted(source_dir.glob('*.gz')):
                yield str(source_file)

    def docs_iter(self):
        return Gov2DocIter(self, slice(0, self.docs_count()))

    def docs_cls(self):
        return Gov2Doc

    def _docs_ctxt_iter_gov2(self, gov2f):
        if isinstance(gov2f, (str, Path)):
            gov2f = gzip.open(gov2f, 'rb')
        doc = None
        for line in gov2f:
            if line == b'<DOC>\n':
                assert doc is None
                doc = line
            elif line == b'</DOC>\n':
                doc += line
                yield self._process_gov2_doc(doc)
                doc = None
            elif doc is not None:
                doc += line

    def _process_gov2_doc(self, raw_doc):
        state = 'DOCNO'
        doc_id = None
        doc_hdr = None
        doc_body = b''
        with io.BytesIO(raw_doc) as f:
            for line in f:
                if state == 'DOCNO':
                    if line.startswith(b'<DOCNO>'):
                        doc_id = line[len(b'<DOCNO>'):-len(b'</DOCNO>')-1].strip().decode()
                        state = 'DOCHDR'
                elif state == 'DOCHDR':
                    if line == b'<DOCHDR>\n':
                        doc_hdr = b''
                    elif line == b'</DOCHDR>\n':
                        state = 'BODY'
                    elif doc_hdr is not None:
                        doc_hdr += line
                elif state == 'BODY':
                    if line == b'</DOC>\n':
                        state = 'DONE'
                    else:
                        doc_body += line
        doc_url, doc_hdr = doc_hdr.decode().split('\n', 1)
        headers = [line.split(':', 1) for line in doc_hdr.split('\n') if ':' in line]
        headers = {k.lower(): v for k, v in headers}
        content_type = 'text/html' # default to text/html
        if 'content-type' in headers:
            content_type = headers['content-type']
        if ';' in content_type:
            content_type, _ = content_type.split(';', 1)
        content_type = content_type.strip()
        return Gov2Doc(doc_id, doc_url, doc_hdr, doc_body, content_type)

    def _docs_id_to_source_file(self, doc_id):
        parts = doc_id.split('-')
        if len(parts) != 3:
            return None
        s_dir, file, doc = parts
        source_file = os.path.join(self.docs_dlc.path(), 'GOV2_data', s_dir, f'{file}.gz')
        return source_file

    def _docs_file_counts(self):
        if self._docs_file_counts_cache is None:
            result = {}
            with self._doccount_dlc.stream() as f:
                f = codecs.getreader('utf8')(f)
                for line in f:
                    path, count = line.strip().split()
                    file = os.path.join(self.docs_dlc.path(), 'GOV2_data', path)
                    result[file] = int(count)
            self._docs_file_counts_cache = result
        return self._docs_file_counts_cache

    def docs_store(self):
        docstore = Gov2Docstore(self)
        return ir_datasets.indices.CacheDocstore(docstore, f'{self.docs_path()}.cache')

    def docs_count(self):
        return sum(self._docs_file_counts().values())

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'


class Gov2Docstore(Docstore):
    def __init__(self, gov2_docs):
        super().__init__(gov2_docs.docs_cls(), 'doc_id')
        self.gov2_docs = gov2_docs

    def get_many_iter(self, doc_ids):
        result = {}
        files_to_search = {}
        for doc_id in doc_ids:
            source_file = self.gov2_docs._docs_id_to_source_file(doc_id)
            if source_file is not None:
                if source_file not in files_to_search:
                    files_to_search[source_file] = []
                files_to_search[source_file].append(doc_id)
        for source_file, doc_ids in files_to_search.items():
            doc_ids = sorted(doc_ids)
            for doc in self.gov2_docs._docs_ctxt_iter_gov2(source_file):
                if doc_ids[0] == doc.doc_id:
                    yield doc
                    doc_ids = doc_ids[1:]
                    if not doc_ids:
                        break # file finished


class RewriteQids(BaseQrels):
    def __init__(self, base_qrels, qid_map):
        self._base_qrels = base_qrels
        self._qid_map = qid_map

    def qrels_iter(self):
        cls = self.qrels_cls()
        for qrel in self._base_qrels.qrels_iter():
            if qrel.query_id in self._qid_map:
                qrel = cls(self._qid_map[qrel.query_id], *qrel[1:])
            yield qrel

    def qrels_defs(self):
        return self._base_qrels.qrels_defs()

    def qrels_path(self):
        return self._base_qrels.qrels_path()

    def qrels_cls(self):
        return self._base_qrels.qrels_cls()


class Gov2DocCountFile:
    def __init__(self, path, docs_dlc):
        self._path = path
        self._docs_dlc = docs_dlc

    def path(self):
        if not os.path.exists(self._path):
            docs_urls_path = os.path.join(self._docs_dlc.path(), 'GOV2_extras/url2id.gz')
            result = Counter()
            with _logger.pbar_raw(desc='building doccounts file', total=25205179) as pbar:
                with gzip.open(docs_urls_path, 'rt') as fin:
                    for line in fin:
                        url, doc_id = line.rstrip().split()
                        d, f, i = doc_id.split('-') # formatted like: GX024-52-0546388
                        file = f'{d}/{f}.gz'
                        result[file] += 1
                        pbar.update()
                with ir_datasets.util.finialized_file(self._path, 'wt') as fout:
                    for file in sorted(result):
                        fout.write(f'{file}\t{result[file]}\n')
        return self._path

    @contextmanager
    def stream(self):
        with open(self.path(), 'rb') as f:
            yield f

def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    docs_dlc = dlc['docs']
    doccount_dlc = Gov2DocCountFile(os.path.join(base_path, 'corpus.doccounts'), docs_dlc)
    collection = Gov2Docs(docs_dlc, doccount_dlc)
    base = Dataset(collection, documentation('_'))

    subsets['trec-tb-2004'] = Dataset(
        collection,
        TrecQueries(dlc['trec-tb-2004/queries'], namespace=NAME, lang='en'),
        TrecQrels(dlc['trec-tb-2004/qrels'], QREL_DEFS),
        documentation('trec-tb-2004')
    )
    subsets['trec-tb-2005'] = Dataset(
        collection,
        TrecQueries(dlc['trec-tb-2005/queries'], namespace=NAME, lang='en'),
        TrecQrels(dlc['trec-tb-2005/qrels'], QREL_DEFS),
        documentation('trec-tb-2005')
    )
    subsets['trec-tb-2005/named-page'] = Dataset(
        collection,
        TrecQueries(dlc['trec-tb-2005/named-page/queries'], qtype=GenericQuery, qtype_map=NAMED_PAGE_QTYPE_MAP, namespace=NAME, lang='en'),
        TrecQrels(dlc['trec-tb-2005/named-page/qrels'], NAMED_PAGE_QREL_DEFS),
        documentation('trec-tb-2005/named-page')
    )
    subsets['trec-tb-2005/efficiency'] = Dataset(
        collection,
        TrecColonQueries(GzipExtract(dlc['trec-tb-2005/efficiency/queries']), encoding='latin1', namespace=NAME, lang='en'),
        RewriteQids(TrecQrels(dlc['trec-tb-2005/qrels'], QREL_DEFS), EFF_MAP_05),
        documentation('trec-tb-2005/efficiency')
    )
    subsets['trec-tb-2006'] = Dataset(
        collection,
        TrecQueries(dlc['trec-tb-2006/queries'], namespace=NAME, lang='en'),
        TrecQrels(dlc['trec-tb-2006/qrels'], QREL_DEFS),
        documentation('trec-tb-2006')
    )
    subsets['trec-tb-2006/named-page'] = Dataset(
        collection,
        TrecQueries(dlc['trec-tb-2006/named-page/queries'], qtype=GenericQuery, qtype_map=NAMED_PAGE_QTYPE_MAP, namespace=NAME, lang='en'),
        TrecQrels(dlc['trec-tb-2006/named-page/qrels'], NAMED_PAGE_QREL_DEFS),
        documentation('trec-tb-2006/named-page')
    )
    subsets['trec-tb-2006/efficiency'] = Dataset(
        collection,
        TrecColonQueries(TarExtract(dlc['trec-tb-2006/efficiency/queries'], '06.efficiency_topics.all'), encoding='latin1', namespace=NAME, lang='en'),
        RewriteQids(TrecQrels(dlc['trec-tb-2006/qrels'], QREL_DEFS), EFF_MAP_06),
        documentation('trec-tb-2006/efficiency')
    )
    subsets['trec-tb-2006/efficiency/10k'] = Dataset(
        collection,
        TrecColonQueries(TarExtract(dlc['trec-tb-2006/efficiency/queries'], '06.efficiency_topics.10k'), encoding='latin1', namespace=NAME, lang='en'),
        documentation('trec-tb-2006/efficiency/10k')
    )
    subsets['trec-tb-2006/efficiency/stream1'] = Dataset(
        collection,
        TrecColonQueries(TarExtract(dlc['trec-tb-2006/efficiency/queries'], '06.efficiency_topics.stream-1'), encoding='latin1', namespace=NAME, lang='en'),
        documentation('trec-tb-2006/efficiency/stream1')
    )
    subsets['trec-tb-2006/efficiency/stream2'] = Dataset(
        collection,
        TrecColonQueries(TarExtract(dlc['trec-tb-2006/efficiency/queries'], '06.efficiency_topics.stream-2'), encoding='latin1', namespace=NAME, lang='en'),
        documentation('trec-tb-2006/efficiency/stream2')
    )
    subsets['trec-tb-2006/efficiency/stream3'] = Dataset(
        collection,
        TrecColonQueries(TarExtract(dlc['trec-tb-2006/efficiency/queries'], '06.efficiency_topics.stream-3'), encoding='latin1', namespace=NAME, lang='en'),
        RewriteQids(TrecQrels(dlc['trec-tb-2006/qrels'], QREL_DEFS), EFF_MAP_06),
        documentation('trec-tb-2006/efficiency/stream3')
    )
    subsets['trec-tb-2006/efficiency/stream4'] = Dataset(
        collection,
        TrecColonQueries(TarExtract(dlc['trec-tb-2006/efficiency/queries'], '06.efficiency_topics.stream-4'), encoding='latin1', namespace=NAME, lang='en'),
        documentation('trec-tb-2006/efficiency/stream4')
    )

    subsets['trec-mq-2007'] = Dataset(
        collection,
        TrecColonQueries(GzipExtract(dlc['trec-mq-2007/queries']), encoding='latin1'),
        TrecPrels(dlc['trec-mq-2007/qrels'], QREL_DEFS),
        documentation('trec-mq-2007')
    )
    subsets['trec-mq-2008'] = Dataset(
        collection,
        TrecColonQueries(GzipExtract(dlc['trec-mq-2008/queries']), encoding='latin1', namespace='trec-mq', lang='en'),
        TrecPrels(TarExtract(dlc['trec-mq-2008/qrels'], '2008.RC1/prels'), QREL_DEFS),
        documentation('trec-mq-2008')
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
