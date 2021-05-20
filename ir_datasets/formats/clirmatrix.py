import codecs
import json
from . import TrecQrels, TrecQrel
from .base import GenericQuery, BaseQueries


class CLIRMatrixQueries(BaseQueries):
    def __init__(self, streamer, query_lang):
        super().__init__()
        self._streamer = streamer
        self.query_lang = query_lang

    def queries_iter(self):
        with self._streamer.stream() as stream:
            f = codecs.getreader('utf-8')(stream)
            for line in f:
                if line == '\n':
                    continue #ignore blank lines

                j = json.loads(line)
                qid = j["src_id"]
                query = j["src_query"]
                yield GenericQuery(qid, query)

    def queries_namespace(self):
        return NAME

    def queries_cls(self):
        return GenericQuery

    def queries_lang(self):
        return self.query_lang


class CLIRMatrixQrels(TrecQrels):
    def qrels_iter(self):
        with self._qrels_dlc.stream() as f:
            f = codecs.getreader('utf8')(f)
            for line in f:
                if line == '\n':
                    continue # ignore blank lines

                j = json.loads(line)

                qid = j["src_id"]
                for did, score in j["tgt_results"]:
                    yield TrecQrel(qid, did, int(score), '0')
