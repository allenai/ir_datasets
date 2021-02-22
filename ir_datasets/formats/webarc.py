import gzip
import re
from contextlib import contextmanager, ExitStack
from typing import NamedTuple
import ir_datasets
from ir_datasets.formats import BaseDocs


class WarcDoc(NamedTuple):
    doc_id: str
    url: str
    date: str
    http_headers: bytes
    body: bytes
    body_content_type: str


class WarcDocs(BaseDocs):
    def __init__(self, id_header='WARC-TREC-ID', warc_cw09=False, lang=None):
        super().__init__()
        self.id_header = id_header
        self.warc_cw09 = warc_cw09
        self._docs_lang = lang

    def docs_iter(self):
        return ir_datasets.indices.WarcIter(self, slice(0, self.docs_count()))

    def _docs_warc_lib(self):
        if self.warc_cw09:
            return ir_datasets.lazy_libs.warc_clueweb09()
        return ir_datasets.lazy_libs.warc()

    def _docs_ctxt_iter_warc(self, warcf):
        warc = self._docs_warc_lib()
        if isinstance(warcf, str):
            warcf = gzip.open(warcf, 'rb')
        f = warc.WARCFile(fileobj=warcf)
        for doc in filter(lambda d: d.type == 'response', f):
            did = doc[self.id_header]
            url = doc['WARC-Target-URI']
            date = doc['WARC-Date']
            payload = doc.payload.read()
            split = re.split(b'\r?\n\r?\n', payload, maxsplit=1)
            if len(split) == 1:
                http_headers, body = split[0], b''
            else:
                http_headers, body = split
            content_type = re.search(b'Content-Type:(.*)', http_headers, flags=re.IGNORECASE)
            if content_type:
                try:
                    content_type = content_type.group(1).decode().strip()
                    content_type = content_type.split(';')
                    content_type = content_type[0]
                except UnicodeDecodeError:
                    content_type = ''
            else:
                content_type = ''
            yield WarcDoc(did, url, date, http_headers, body, content_type)

    def docs_path(self):
        raise NotImplementedError

    def _docs_iter_source_files(self):
        raise NotImplementedError

    def _docs_id_to_source_file(self, doc_id):
        # For Warc Docstore lookups
        raise NotImplementedError

    def _docs_warc_file_counts(self):
        raise NotImplementedError

    def _docs_source_file_to_checkpoint(self, source_file):
        # For Warc Docstore lookups
        return None

    def docs_store(self):
        docstore = ir_datasets.indices.ClueWebWarcDocstore(self)
        return ir_datasets.indices.CacheDocstore(docstore, f'{self.docs_path()}.cache')

    def docs_cls(self):
        return WarcDoc

    def docs_count(self):
        return sum(self._docs_warc_file_counts().values())

    def docs_lang(self):
        return self._docs_lang
