import io
import codecs
import tarfile
import re
import gzip
import xml.etree.ElementTree as ET
from fnmatch import fnmatch
from pathlib import Path
from collections import namedtuple
import ir_datasets
from .base import GenericDoc, GenericScoredDoc, BaseDocs, BaseQueries, BaseScoredDocs, BaseQrels


TrecDoc = namedtuple('TrecDoc', ['doc_id', 'text', 'marked_up_doc'])
TrecQuery = namedtuple('TrecQuery', ['query_id', 'title', 'description', 'narrative'])
TrecQrel = namedtuple('TrecQrel', ['query_id', 'doc_id', 'relevance', 'iteration'])

# Default content tags from Anserini's TrecCollection
CONTENT_TAGS = 'TEXT HEADLINE TITLE HL HEAD TTL DD DATE LP LEADPARA'.split()

class TrecDocs(BaseDocs):
    def __init__(self, docs_dlc, encoding=None, path_globs=None, content_tags=CONTENT_TAGS, parser='BS4'):
        self._docs_dlc = docs_dlc
        self._encoding = encoding
        self._path_globs = path_globs
        self._content_tags = content_tags
        self._parser = {
            'BS4': self._parser_bs,
            'text': self._parser_text,
        }[parser]
        self._doc = {
            'BS4': TrecDoc,
            'text': GenericDoc,
        }[parser]

    def docs_path(self):
        return self._docs_dlc.path()

    def docs_iter(self):
        if Path(self._docs_dlc.path()).is_dir():
            if self._path_globs:
                for glob in sorted(self._path_globs):
                    for path in Path(self._docs_dlc.path()).glob(glob):
                        yield from self._docs_iter(path)
            else:
                yield from self._docs_iter(self._docs_dlc.path())
        elif Path(self._docs_dlc.path()).is_file():
            if self._path_globs:
                # tarfile, find globs, open in streaming mode (r|)
                with self._docs_dlc.stream() as stream:
                    with tarfile.open(fileobj=stream, mode='r|gz') as tarf:
                        for block in tarf:
                            if any(fnmatch(block.name, g) for g in self._path_globs):
                                file = tarf.extractfile(block)
                                if block.name.endswith('.gz'):
                                    file = gzip.GzipFile(fileobj=file)
                                yield from self._parser(file)
            else:
                with self._docs_dlc.stream() as f:
                    yield from self._parser(f)

    def _docs_iter(self, path):
        if Path(path).is_file():
            if str(path).endswith('.gz'):
                with gzip.open(path, 'rb') as f:
                    yield from self._parser(f)
            else:
                with path.open('rb') as f:
                    yield from self._parser(f)
        elif Path(path).is_dir():
            for child in path.iterdir():
                yield from self._docs_iter(child)

    def _parser_bs(self, stream):
        BeautifulSoup = ir_datasets.lazy_libs.bs4().BeautifulSoup
        f = codecs.getreader(self._encoding or 'utf8')(stream, errors='replace')
        doc_id, doc_markup = None, ''
        in_tag = False
        for line in f:
            if line.startswith('<DOCNO>'):
                doc_id = line.replace('<DOCNO>', '').replace('</DOCNO>\n', '').strip()
            elif line == '</DOC>\n':
                soup = BeautifulSoup(f'<OUTER>\n{doc_markup}\n</OUTER>', 'lxml')
                text = soup.get_text()
                yield TrecDoc(doc_id, text, doc_markup)
                doc_id, doc_markup = None, ''
            else:
                if in_tag:
                    doc_markup += line
                if line.startswith('</'):
                    if any(line.startswith(f'</{tag}>') for tag in self._content_tags):
                        in_tag -= 1
                if line.startswith('<'):
                    if any(line.startswith(f'<{tag}>') for tag in self._content_tags):
                        in_tag += 1
                        if in_tag == 1:
                            doc_markup += line

    def _parser_text(self, stream):
        f = codecs.getreader(self._encoding or 'utf8')(stream, errors='replace')
        doc_id, doc_text = None, ''
        in_tag = False
        for line in f:
            if line.startswith('<DOCNO>'):
                doc_id = line.replace('<DOCNO>', '').replace('</DOCNO>\n', '').strip()
            elif line == '</DOC>\n':
                yield GenericDoc(doc_id, doc_text)
                doc_id, doc_text = None, ''
            else:
                if line.startswith('</'):
                    if any(line.startswith(f'</{tag}>') for tag in self._content_tags):
                        in_tag = False
                if in_tag:
                    doc_text += line
                if line.startswith('<'):
                    if any(line.startswith(f'<{tag}>') for tag in self._content_tags):
                        in_tag = True

    def docs_cls(self):
        return self._doc



DEFAULT_QTYPE_MAP = {
    '<num> *(Number:)?': 'query_id',
    '<title> *(Topic:)?': 'title',
    '<desc> *(Description:)?': 'description',
    '<narr> *(Narrative:)?': 'narrative'
}
class TrecQueries(BaseQueries):
    def __init__(self, queries_dlc, qtype=TrecQuery, qtype_map=None, encoding=None):
        self._queries_dlc = queries_dlc
        self._qtype = qtype
        self._qtype_map = qtype_map or DEFAULT_QTYPE_MAP
        self._encoding = encoding

    def queries_path(self):
        return self._queries_dlc.path()

    def queries_iter(self):
        fields, reading = {}, None
        with self._queries_dlc.stream() as f:
            f = codecs.getreader(self._encoding or 'utf8')(f)
            for line in f:
                if line.startswith('</top>'):
                    assert len(fields) == len(self._qtype._fields), fields
                    yield self._qtype(*(fields[f].strip() for f in self._qtype._fields))
                    fields, reading = {}, None
                match_any = False
                for tag, target in self._qtype_map.items():
                    match = re.match(tag, line)
                    if match:
                        fields[target] = line[match.end():]
                        reading = target
                        match_any = True
                        break
                if not match_any and reading and not line.startswith('<'):
                    fields[reading] += line

    def queries_cls(self):
        return self._qtype


class TrecXmlQueries(BaseQueries):
    def __init__(self, queries_dlc, qtype=TrecQuery, qtype_map=None, encoding=None):
        self._queries_dlc = queries_dlc
        self._qtype = qtype
        self._qtype_map = qtype_map or {f: f for f in qtype._fields}
        self._encoding = encoding

    def queries_path(self):
        return self._queries_dlc.path()

    def queries_iter(self):
        with self._queries_dlc.stream() as f:
            f = codecs.getreader(self._encoding or 'utf8')(f)
            for topic_el in ET.fromstring(f.read()):
                item = [None for _ in self._qtype._fields]
                item[self._qtype._fields.index('query_id')] = topic_el.attrib['number']
                for field_el in topic_el:
                    if field_el.tag in self._qtype_map:
                        text = ''.join(field_el.itertext())
                        field = self._qtype_map[field_el.tag]
                        item[self._qtype._fields.index(field)] = text
                yield self._qtype(*item)

    def queries_cls(self):
        return self._qtype


class TrecQrels(BaseQrels):
    def __init__(self, qrels_dlc, qrels_defs):
        self._qrels_dlc = qrels_dlc
        self._qrels_defs = qrels_defs

    def qrels_path(self):
        return self._qrels_dlc.path()

    def qrels_iter(self):
        with self._qrels_dlc.stream() as f:
            f = codecs.getreader('utf8')(f)
            for line in f:
                if line == '\n':
                    continue # ignore blank lines
                cols = line.rstrip().split()
                if len(cols) != 4:
                    raise RuntimeError(f'expected 4 columns, got {len(cols)}')
                qid, it, did, score = cols
                yield TrecQrel(qid, did, int(score), it)

    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return self._qrels_defs


class TrecScoredDocs(BaseScoredDocs):
    def __init__(self, scoreddocs_dlc):
        self._scoreddocs_dlc = scoreddocs_dlc

    def scoreddocs_path(self):
        return self._scoreddocs_dlc.path()

    def scoreddocs_iter(self):
        with self._scoreddocs_dlc.stream() as f:
            f = codecs.getreader('utf8')(f)
            for line in f:
                cols = line.rstrip().split()
                if len(cols) == 6:
                    qid, _, did, _, score, _ = cols
                elif len(cols) == 2:
                    qid, did, score = *cols, '0'
                yield GenericScoredDoc(qid, did, float(score))
