import io
import codecs
import tarfile
import re
import gzip
from glob import glob as fnglob
import xml.etree.ElementTree as ET
from fnmatch import fnmatch
from pathlib import Path
from typing import NamedTuple
import ir_datasets
from ir_datasets.indices import PickleLz4FullStore
from .base import GenericDoc, GenericQuery, GenericScoredDoc, BaseDocs, BaseQueries, BaseScoredDocs, BaseQrels


class TrecDoc(NamedTuple):
    doc_id: str
    text: str
    marked_up_doc: str
    def default_text(self):
        """
        text
        """
        return self.text

class TitleUrlTextDoc(NamedTuple):
    doc_id: str
    title: str
    url: str
    text: str
    def default_text(self):
        """
        title and text
        """
        return f'{self.title} {self.text}'

class TrecParsedDoc(NamedTuple):
    doc_id: str
    title: str
    body: str
    marked_up_doc: bytes
    def default_text(self):
        """
        title and body
        """
        return f'{self.title} {self.body}'

class TrecQuery(NamedTuple):
    query_id: str
    title: str
    description: str
    narrative: str
    def default_text(self):
        """
        title
        """
        return self.title

class TrecSubtopic(NamedTuple):
    number: str
    text: str
    type: str

class TrecQrel(NamedTuple):
    query_id: str
    doc_id: str
    relevance: int
    iteration: str

class TrecSubQrel(NamedTuple):
    query_id: str
    doc_id: str
    relevance: int
    subtopic_id: str

class TrecPrel(NamedTuple):
    query_id: str
    doc_id: str
    relevance: int
    method: int
    iprob: float

# Default content tags from Anserini's TrecCollection
CONTENT_TAGS = 'TEXT HEADLINE TITLE HL HEAD TTL DD DATE LP LEADPARA'.split()

class TrecDocs(BaseDocs):
    def __init__(self, docs_dlc, encoding=None, path_globs=None, content_tags=CONTENT_TAGS, parser='BS4', namespace=None, lang=None, expected_file_count=None, docstore_size_hint=None, count_hint=None, docstore_path=None):
        self._docs_dlc = docs_dlc
        self._encoding = encoding
        self._path_globs = path_globs
        self._content_tags = content_tags
        self._parser = {
            'BS4': self._parser_bs,
            'text': self._parser_text,
            'tut': self._parser_tut,
            'sax': self._parser_sax,
        }[parser]
        self._doc = {
            'BS4': TrecDoc,
            'text': GenericDoc,
            'tut': TitleUrlTextDoc,
            'sax': TrecParsedDoc,
        }[parser]
        self._docs_namespace = namespace
        self._docs_lang = lang
        self._expected_file_count = expected_file_count
        self._docstore_size_hint = docstore_size_hint
        self._count_hint = count_hint
        self._docstore_path = docstore_path
        if expected_file_count is not None:
            assert self._path_globs is not None, "expected_file_count only supported with path_globs"

    def docs_path(self, force=True):
        return self._docs_dlc.path(force)

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        if Path(self._docs_dlc.path()).is_dir():
            if self._path_globs:
                file_count = 0
                for glob in sorted(self._path_globs):
                    glob_path = str(Path(self._docs_dlc.path())/glob)
                    # IMPORTANT: cannot use Path().glob() here because the recusive ** will not follow symlinks.
                    # Need to use glob.glob instead with recursive=True flag.
                    for path in sorted(fnglob(glob_path, recursive=True)):
                        file_count += 1
                        yield from self._docs_iter(path)
                if self._expected_file_count is not None:
                    if file_count != self._expected_file_count:
                        raise RuntimeError(f'found {file_count} files of the expected {self._expected_file_count} matching the following: {self._path_globs} under {self._docs_dlc.path()}. Make sure that directories are linked such that these globs match the correct number of files.')
            else:
                yield from self._docs_iter(self._docs_dlc.path())
        else:
            if self._path_globs:
                file_count = 0
                # tarfile, find globs, open in streaming mode (r|)
                with self._docs_dlc.stream() as stream:
                    with tarfile.open(fileobj=stream, mode='r|gz') as tarf:
                        for block in tarf:
                            if any(fnmatch(block.name, g) for g in self._path_globs):
                                file = tarf.extractfile(block)
                                if block.name.endswith('.gz'):
                                    file = gzip.GzipFile(fileobj=file)
                                yield from self._parser(file)
                                file_count += 1
                if self._expected_file_count is not None:
                    if file_count != self._expected_file_count:
                        raise RuntimeError(f'found {file_count} files of the expected {self._expected_file_count} matching the following: {self._path_globs} under {self._docs_dlc.path()}. Make sure that directories are linked such that these globs match the correct number of files.')
            else:
                with self._docs_dlc.stream() as f:
                    yield from self._parser(f)

    def _docs_iter(self, path):
        if Path(path).is_file():
            path_suffix = Path(path).suffix.lower()
            if path_suffix == '.gz':
                with gzip.open(path, 'rb') as f:
                    yield from self._parser(f)
            elif path_suffix in ['.z', '.0z', '.1z', '.2z']:
                # unix "compress" command encoding
                unlzw3 = ir_datasets.lazy_libs.unlzw3()
                with io.BytesIO(unlzw3.unlzw(Path(path))) as f:
                    yield from self._parser(f)
            else:
                with open(path, 'rb') as f:
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

    def _parser_tut(self, stream):
        f = codecs.getreader(self._encoding or 'utf8')(stream, errors='replace')
        doc_id, doc_title, doc_url, doc_text = None, None, None, ''
        in_tag = False
        for line in f:
            if line.startswith('<DOCNO>'):
                doc_id = line.replace('<DOCNO>', '').replace('</DOCNO>\n', '').strip()
            if line.startswith('<TITLE>'):
                doc_title = line.replace('<TITLE>', '').replace('</TITLE>\n', '').strip()
            if line.startswith('<URL>'):
                doc_url = line.replace('<URL>', '').replace('</URL>\n', '').strip()
            elif line == '</DOC>\n':
                yield TitleUrlTextDoc(doc_id, doc_title, doc_url, doc_text)
                doc_id, doc_title, doc_url, doc_text = None, None, None, ''
            else:
                if line.startswith('</TEXT>'):
                    in_tag = False
                if in_tag:
                    doc_text += line
                if line.startswith('<TEXT>'):
                    in_tag = True

    def _parser_sax(self, stream):
        field_defs = []
        field_defs.append({'docno'})
        field_defs.append({'headline', 'title', 'h3', 'h4'})
        field_defs.append({c.lower() for c in CONTENT_TAGS} - field_defs[-1])
        buffer = bytearray()
        while True:
            if b'\n</DOC>' not in buffer:
                chunk = stream.read1()
                if chunk == b'':
                    break
                buffer.extend(chunk)
            else:
                idx = buffer.index(b'\n</DOC>')
                full_doc = bytes(buffer[:idx+7])
                doc_id, title, body = ir_datasets.util.html_parsing.sax_html_parser(full_doc, force_encoding=self._encoding or 'utf8', fields=field_defs)
                yield TrecParsedDoc(doc_id, title, body, full_doc.strip())
                del buffer[:idx+7]

    def docs_cls(self):
        return self._doc

    def docs_store(self, field='doc_id'):
        if self._docstore_path is not None:
            ds_path = self._docstore_path
        else:
            ds_path = f'{self.docs_path(force=False)}.pklz4'
        return PickleLz4FullStore(
            path=ds_path,
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            size_hint=self._docstore_size_hint,
            count_hint=self._count_hint,
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return self._docs_namespace

    def docs_lang(self):
        return self._docs_lang


DEFAULT_QTYPE_MAP = {
    '<num> *(Number:)?': 'query_id',
    '<title> *(Topic:)?': 'title',
    '<desc> *(Description:)?': 'description',
    '<narr> *(Narrative:)?': 'narrative'
}
class TrecQueries(BaseQueries):
    def __init__(self, queries_dlc, qtype=TrecQuery, qtype_map=None, encoding=None, namespace=None, lang=None, remove_tags=('</title>',)):
        self._queries_dlc = queries_dlc
        self._qtype = qtype
        self._qtype_map = qtype_map or DEFAULT_QTYPE_MAP
        self._encoding = encoding
        self._queries_namespace = namespace
        self._queries_lang = lang
        self._remove_tags = remove_tags

    def queries_path(self):
        return self._queries_dlc.path()

    def queries_iter(self):
        fields, reading = {}, None
        with self._queries_dlc.stream() as f:
            f = codecs.getreader(self._encoding or 'utf8')(f)
            for line in f:
                if line.startswith('</top>'):
                    assert len(fields) == len(self._qtype._fields), fields
                    for tag in self._remove_tags:
                        fields = {k: v.replace(tag, '') for k, v in fields.items()}
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

    def queries_namespace(self):
        return self._queries_namespace

    def queries_lang(self):
        return self._queries_lang


class TrecXmlQueries(BaseQueries):
    def __init__(self, queries_dlc, qtype=TrecQuery, qtype_map=None, encoding=None, subtopics_key='subtopics', namespace=None, lang=None):
        self._queries_dlc = queries_dlc
        self._qtype = qtype
        self._qtype_map = qtype_map or {f: f for f in qtype._fields}
        self._encoding = encoding
        self._subtopics_key = subtopics_key
        self._queries_namespace = namespace
        self._queries_lang = lang

    def queries_path(self):
        return self._queries_dlc.path()

    def queries_iter(self):
        with self._queries_dlc.stream() as f:
            f = codecs.getreader(self._encoding or 'utf8')(f)
            for topic_el in ET.fromstring(f.read()):
                item = [None for _ in self._qtype._fields]
                if 'number' in topic_el.attrib:
                    item[self._qtype._fields.index('query_id')] = topic_el.attrib['number']
                subtopics = []
                for attr in topic_el.attrib:
                    if attr in self._qtype_map:
                        text = topic_el.attrib[attr]
                        field = self._qtype_map[attr]
                        item[self._qtype._fields.index(field)] = text
                if topic_el.tag in self._qtype_map:
                    text = ''.join(topic_el.itertext())
                    field = self._qtype_map[topic_el.tag]
                    item[self._qtype._fields.index(field)] = text
                for field_el in topic_el:
                    if field_el.tag in self._qtype_map:
                        text = ''.join(field_el.itertext())
                        field = self._qtype_map[field_el.tag]
                        item[self._qtype._fields.index(field)] = text
                    if field_el.tag == 'subtopic':
                        text = ''.join(field_el.itertext())
                        subtopics.append(TrecSubtopic(field_el.attrib['number'], text, field_el.attrib['type']))
                if self._subtopics_key in self._qtype._fields:
                    item[self._qtype._fields.index('subtopics')] = tuple(subtopics)
                qid_field = self._qtype._fields.index('query_id')
                item[qid_field] = item[qid_field].strip() # remove whitespace from query_ids
                yield self._qtype(*item)

    def queries_cls(self):
        return self._qtype

    def queries_namespace(self):
        return self._queries_namespace

    def queries_lang(self):
        return self._queries_lang


class TrecColonQueries(BaseQueries):
    def __init__(self, queries_dlc, encoding=None, namespace=None, lang=None):
        self._queries_dlc = queries_dlc
        self._encoding = encoding
        self._queries_namespace = namespace
        self._queries_lang = lang

    def queries_iter(self):
        with self._queries_dlc.stream() as f:
            f = codecs.getreader(self._encoding or 'utf8')(f)
            for line in f:
                query_id, text = line.split(':', 1)
                text = text.rstrip('\n')
                yield GenericQuery(query_id, text)

    def queries_path(self):
        return self._queries_dlc.path()

    def queries_cls(self):
        return GenericQuery

    def queries_namespace(self):
        return self._queries_namespace

    def queries_lang(self):
        return self._queries_lang


class TrecQrels(BaseQrels):
    def __init__(self, qrels_dlc, qrels_defs, format_3col=False):
        self._qrels_dlc = qrels_dlc
        self._qrels_defs = qrels_defs
        self._format_3col = format_3col

    def qrels_path(self):
        return self._qrels_dlc.path()

    def qrels_iter(self):
        if isinstance(self._qrels_dlc, list):
            for dlc in self._qrels_dlc:
                yield from self._qrels_internal_iter(dlc)
        else:
            yield from self._qrels_internal_iter(self._qrels_dlc)

    def _qrels_internal_iter(self, dlc):
        with dlc.stream() as f:
            f = codecs.getreader('utf8')(f)
            for line in f:
                if line == '\n':
                    continue # ignore blank lines
                cols = line.rstrip().split()
                if self._format_3col:
                    if len(cols) != 3:
                        raise RuntimeError(f'expected 3 columns, got {len(cols)}')
                    qid, did, score = cols
                    it = 'Q0'
                else:
                    if len(cols) != 4:
                        raise RuntimeError(f'expected 4 columns, got {len(cols)}')
                    qid, it, did, score = cols
                yield TrecQrel(qid, did, int(score), it)

    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return self._qrels_defs


class TrecPrels(TrecQrels):
    def qrels_iter(self):
        with self._qrels_dlc.stream() as f:
            f = codecs.getreader('utf8')(f)
            for line in f:
                if line == '\n':
                    continue # ignore blank lines
                cols = line.rstrip().split()
                if len(cols) != 5:
                    raise RuntimeError(f'expected 5 columns, got {len(cols)}')
                qid, did, rel, method, iprob = cols
                yield TrecPrel(qid, did, int(rel), int(method), float(iprob))

    def qrels_cls(self):
        return TrecPrel


class TrecSubQrels(BaseQrels):
    def __init__(self, qrels_dlc, qrels_defs):
        self._qrels_dlc = qrels_dlc
        self._qrels_defs = qrels_defs

    def qrels_path(self):
        return self._qrels_dlc.path()

    def qrels_iter(self):
        if isinstance(self._qrels_dlc, list):
            for dlc in self._qrels_dlc:
                yield from self._qrels_internal_iter(dlc)
        else:
            yield from self._qrels_internal_iter(self._qrels_dlc)

    def _qrels_internal_iter(self, dlc):
        with dlc.stream() as f:
            f = codecs.getreader('utf8')(f)
            for line in f:
                if line == '\n':
                    continue # ignore blank lines
                cols = line.rstrip().split()
               
                if len(cols) != 4:
                    raise RuntimeError(f'expected 4 columns, got {len(cols)}')
                qid, sid, did, score = cols
                yield TrecSubQrel(qid, did, int(score), sid)

    def qrels_cls(self):
        return TrecSubQrel

    def qrels_defs(self):
        return self._qrels_defs


class TrecScoredDocs(BaseScoredDocs):
    def __init__(self, scoreddocs_dlc, negate_score=False):
        self._scoreddocs_dlc = scoreddocs_dlc
        self._negate_score = negate_score

    def scoreddocs_path(self):
        return self._scoreddocs_dlc.path()

    def scoreddocs_iter(self):
        with self._scoreddocs_dlc.stream() as f:
            f = codecs.getreader('utf8')(f)
            for line in f:
                cols = line.rstrip().split()
                if len(cols) == 6:
                    qid, _, did, _, score, _ = cols # TREC-style (qid iteration did score rank score runtag
                elif len(cols) == 2:
                    qid, did, score = *cols, '0' # MS MARCO-style (qid did -- only)
                elif len(cols) == 3:
                    qid, did, score = cols # MMARCO-style (qid did score)
                score = float(score)
                if self._negate_score:
                    score = -score
                yield GenericScoredDoc(qid, did, score)
