from codecs import getreader
from typing import NamedTuple, Any, Optional, Dict
from xml.etree.ElementTree import parse, Element, ElementTree

from ir_datasets.formats import BaseQueries, BaseQrels, TrecQrel


class ToucheQuery(NamedTuple):
    query_id: str
    title: str
    description: str
    narrative: str


class ToucheTitleQuery(NamedTuple):
    query_id: str
    title: str


class ToucheQualityQrel(NamedTuple):
    query_id: str
    doc_id: str
    relevance: int
    quality: int
    iteration: str


class ToucheQueries(BaseQueries):
    _source: Any
    _namespace: Optional[str]
    _language: Optional[str]
    _has_description: bool

    def __init__(
            self,
            source: Any,
            namespace: Optional[str] = None,
            language: Optional[str] = None,
            has_description: bool = True,
    ):
        self._source = source
        self._namespace = namespace
        self._language = language
        self._has_description = has_description

    def queries_path(self):
        return self._source.path()

    def queries_iter(self):
        with self._source.stream() as file:
            tree: ElementTree = parse(file)
            root: Element = tree.getroot()
            assert root.tag == "topics"

            for element in root:
                element: Element
                assert element.tag == "topic"
                number = int(element.findtext("number").strip())
                title = element.findtext("title").strip()

                if self._has_description:
                    description = element.findtext("description").strip()
                    narrative = element.findtext("narrative").strip()
                    yield ToucheQuery(
                        str(number),
                        title,
                        description,
                        narrative,
                    )
                else:
                    yield ToucheTitleQuery(
                        str(number),
                        title,
                    )

    def queries_cls(self):
        return ToucheQuery if self._has_description else ToucheTitleQuery

    def queries_namespace(self):
        return self._namespace

    def queries_lang(self):
        return self._language


class ToucheQrels(BaseQrels):
    _source: Any
    _definitions: Dict[int, str]

    def __init__(self, source: Any, definitions: Dict[int, str]):
        self._source = source
        self._definitions = definitions

    def qrels_path(self):
        return self._source.path()

    def qrels_iter(self):
        with self._source.stream() as file:
            file = getreader("utf8")(file)
            for line in file:
                if line == "\n":
                    continue  # Ignore blank lines.
                cols = line.rstrip().split()
                if len(cols) != 4:
                    raise RuntimeError(
                        f"Expected 4 columns but got {len(cols)}."
                    )
                qid, it, did, score = cols
                yield TrecQrel(qid, did, int(float(score)), it)

    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return self._definitions


class ToucheQualityQrels(BaseQrels):
    _source_relevance: ToucheQrels
    _source_quality: ToucheQrels

    def __init__(self, source_relevance: ToucheQrels,
                 source_quality: ToucheQrels):
        self._source_relevance = source_relevance
        self._source_quality = source_quality

    def qrels_path(self):
        return self._source_relevance.path()

    def qrels_iter(self):
        iterator = zip(
            self._source_relevance.qrels_iter(),
            self._source_quality.qrels_iter(),
        )
        for qrel_relevance, qrel_quality in iterator:
            qrel_relevance: TrecQrel
            qrel_quality: TrecQrel
            assert qrel_relevance.query_id == qrel_quality.query_id
            assert qrel_relevance.doc_id == qrel_quality.doc_id
            assert qrel_relevance.iteration == qrel_quality.iteration
            yield ToucheQualityQrel(
                qrel_relevance.query_id,
                qrel_relevance.doc_id,
                qrel_relevance.relevance,
                qrel_quality.relevance,
                qrel_relevance.iteration,
            )

    def qrels_cls(self):
        return ToucheQualityQrel

    def qrels_defs(self):
        return self._source_relevance.qrels_defs()
