from enum import Enum
from io import TextIOWrapper
from json import loads
from re import compile
from typing import NamedTuple, Any, Optional, Dict, List, Tuple
from xml.etree.ElementTree import parse, Element, ElementTree
from zipfile import ZipFile

from ir_datasets.formats import BaseQueries, BaseQrels, TrecQrel, BaseDocs
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.util import Cache, use_docstore


class ToucheQuery(NamedTuple):
    query_id: str
    title: str
    description: str
    narrative: str


class ToucheTitleQuery(NamedTuple):
    query_id: str
    title: str


class ToucheComparativeQuery(NamedTuple):
    query_id: str
    title: str
    objects: Tuple[str, str]
    description: str
    narrative: str


class ToucheQualityQrel(NamedTuple):
    query_id: str
    doc_id: str
    relevance: int
    quality: int
    iteration: str


class ToucheQualityCoherenceQrel(NamedTuple):
    query_id: str
    doc_id: str
    relevance: int
    quality: int
    coherence: int
    iteration: str


class ToucheComparativeStance(Enum):
    FIRST = "FIRST"
    SECOND = "SECOND"
    NEUTRAL = "NEUTRAL"
    NO = "NO"


class ToucheQualityComparativeStanceQrel(NamedTuple):
    query_id: str
    doc_id: str
    relevance: int
    quality: int
    stance: ToucheComparativeStance
    iteration: str


class ToucheControversialStance(Enum):
    PRO = "PRO"
    CON = "CON"
    ONTOPIC = "ONTOPIC"


class ToucheControversialStanceQrel(NamedTuple):
    query_id: str
    doc_id: str
    relevance: int
    stance: ToucheControversialStance


class TouchePassageDoc(NamedTuple):
    doc_id: str
    text: str
    chatnoir_url: str


class ToucheImageRanking(NamedTuple):
    query_id: str
    query: str
    rank: int  # 1-indexed


class ToucheImageNode(NamedTuple):
    xpath: str
    visible: bool
    id: Optional[str]
    classes: List[str]
    position: Tuple[int, int, int, int]  # left top right bottom
    text: Optional[str]
    css: Dict[str, str]


class ToucheImagePage(NamedTuple):
    page_id: str
    url: str
    rankings: List[ToucheImageRanking]
    dom_html: bytes
    xpaths: List[str]
    nodes: List[ToucheImageNode]
    # screenshot_png: bytes
    text: str
    # warc_gz: bytes


class ToucheImageDoc(NamedTuple):
    doc_id: str
    png: bytes
    webp: bytes
    url: str
    phash: str  # https://www.phash.org/
    pages: List[ToucheImagePage]


class ToucheQueries(BaseQueries):
    _source: Any
    _namespace: Optional[str]
    _language: Optional[str]

    def __init__(
            self,
            source: Any,
            namespace: Optional[str] = None,
            language: Optional[str] = None,
    ):
        self._source = source
        self._namespace = namespace
        self._language = language

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
                description = element.findtext("description").strip()
                narrative = element.findtext("narrative").strip()
                yield ToucheQuery(
                    str(number),
                    title,
                    description,
                    narrative,
                )

    def queries_cls(self):
        return ToucheQuery

    def queries_namespace(self):
        return self._namespace

    def queries_lang(self):
        return self._language


class ToucheTitleQueries(BaseQueries):
    _source: Any
    _namespace: Optional[str]
    _language: Optional[str]

    def __init__(
            self,
            source: Any,
            namespace: Optional[str] = None,
            language: Optional[str] = None,
    ):
        self._source = source
        self._namespace = namespace
        self._language = language

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
                yield ToucheTitleQuery(
                    str(number),
                    title,
                )

    def queries_cls(self):
        return ToucheTitleQuery

    def queries_namespace(self):
        return self._namespace

    def queries_lang(self):
        return self._language


class ToucheComparativeQueries(BaseQueries):
    _source: Any
    _namespace: Optional[str]
    _language: Optional[str]

    def __init__(
            self,
            source: Any,
            namespace: Optional[str] = None,
            language: Optional[str] = None,
    ):
        self._source = source
        self._namespace = namespace
        self._language = language

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
                objects = element.findtext("objects").split(",")
                objects = (obj.strip() for obj in objects)
                object1, object2 = objects
                description = element.findtext("description").strip()
                narrative = element.findtext("narrative").strip()
                yield ToucheComparativeQuery(
                    str(number),
                    title,
                    (object1, object2),
                    description,
                    narrative,
                )

    def queries_cls(self):
        return ToucheComparativeQuery

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
            lines = (
                line.rstrip()
                for line in TextIOWrapper(file)
                if line != "\n"  # Ignore blank lines.
            )

            for line in lines:
                cols = line.split()
                if len(cols) != 4:
                    raise ValueError(f"Expected 4 relevance columns but got {len(cols)}.")
                qid, it, did, score = cols

                yield TrecQrel(
                    query_id=qid,
                    doc_id=did,
                    relevance=int(score),
                    iteration=it,
                )

    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return self._definitions


class ToucheQualityQrels(BaseQrels):
    _source: Any
    _source_quality: Any
    _definitions: Dict[int, str]

    def __init__(self, source: Any, source_quality: Any, definitions: Dict[int, str]):
        self._source = source
        self._source_quality = source_quality
        self._definitions = definitions

    def qrels_path(self):
        return self._source.path()

    def qrels_iter(self):
        with self._source.stream() as file, self._source_quality.stream() as file_quality:
            lines = (
                line.rstrip() 
                for line in TextIOWrapper(file)
                if line != "\n"  # Ignore blank lines.
            )
            lines_quality = (
                line.rstrip() 
                for line in TextIOWrapper(file_quality)
                if line != "\n"  # Ignore blank lines.
            )
            zipped_lines = zip(lines, lines_quality)

            for zipped_line in zipped_lines:
                line, line_quality = zipped_line

                cols = line.split()
                if len(cols) != 4:
                    raise ValueError(f"Expected 4 relevance columns but got {len(cols)}.")
                qid, it, did, score = cols

                cols_quality = line_quality.split()
                if len(cols_quality) != 4:
                    raise ValueError(f"Expected 4 quality columns but got {len(cols_quality)}.")
                qid_quality, it_quality, did_quality, score_quality = cols_quality
                if qid_quality != qid:
                    raise ValueError(f"Quality query {qid_quality} does not match relevance query {qid}.")
                if did_quality != did:
                    raise ValueError(f"Quality document {did_quality} does not match relevance document {did}.")
                if it_quality != it:
                    raise ValueError(f"Quality iteration {it_quality} does not match relevance iteration {it}.")

                yield ToucheQualityQrel(
                    query_id=qid,
                    doc_id=did,
                    relevance=int(score),
                    quality=int(score_quality),
                    iteration=it,
                )

    def qrels_cls(self):
        return ToucheQualityQrel

    def qrels_defs(self):
        return self._definitions


class ToucheQualityCoherenceQrels(BaseQrels):
    _source: Any
    _source_quality: Any
    _source_coherence: Any
    _definitions: Dict[int, str]

    def __init__(self, source: Any, source_quality: Any, source_coherence: Any, definitions: Dict[int, str]):
        self._source = source
        self._source_quality = source_quality
        self._source_coherence = source_coherence
        self._definitions = definitions

    def qrels_path(self):
        return self._source.path()

    def qrels_iter(self):
        with self._source.stream() as file, self._source_quality.stream() as file_quality, self._source_coherence.stream() as file_coherence:
            lines = (
                line.rstrip() 
                for line in TextIOWrapper(file)
                if line != "\n"  # Ignore blank lines.
            )
            lines_quality = (
                line.rstrip() 
                for line in TextIOWrapper(file_quality)
                if line != "\n"  # Ignore blank lines.
            )
            lines_coherence = (
                line.rstrip() 
                for line in TextIOWrapper(file_coherence)
                if line != "\n"  # Ignore blank lines.
            )
            zipped_lines = zip(lines, lines_quality, lines_coherence)

            for zipped_line in zipped_lines:
                line, line_quality, line_coherence = zipped_line

                cols = line.split()
                if len(cols) != 4:
                    raise ValueError(f"Expected 4 relevance columns but got {len(cols)}.")
                qid, it, did, score = cols

                cols_quality = line_quality.split()
                if len(cols_quality) != 4:
                    raise ValueError(f"Expected 4 quality columns but got {len(cols_quality)}.")
                qid_quality, it_quality, did_quality, score_quality = cols_quality
                if qid_quality != qid:
                    raise ValueError(f"Quality query {qid_quality} does not match relevance query {qid}.")
                if did_quality != did:
                    raise ValueError(f"Quality document {did_quality} does not match relevance document {did}.")
                if it_quality != it:
                    raise ValueError(f"Quality iteration {it_quality} does not match relevance iteration {it}.")

                cols_coherence = line_coherence.split()
                if len(cols_coherence) != 4:
                    raise ValueError(f"Expected 4 coherence columns but got {len(cols_coherence)}.")
                qid_coherence, it_coherence, did_coherence, score_coherence = cols_coherence
                if qid_coherence != qid:
                    raise ValueError(f"Coherence query {qid_coherence} does not match relevance query {qid}.")
                if did_coherence != did:
                    raise ValueError(f"Coherence document {did_coherence} does not match relevance document {did}.")
                if it_coherence != it:
                    raise ValueError(f"Coherence iteration {it_coherence} does not match relevance iteration {it}.")

                yield ToucheQualityCoherenceQrel(
                    query_id=qid,
                    doc_id=did,
                    relevance=int(score),
                    quality=int(score_quality),
                    coherence=int(score_coherence),
                    iteration=it,
                )

    def qrels_cls(self):
        return ToucheQualityCoherenceQrel

    def qrels_defs(self):
        return self._definitions


class ToucheQualityComparativeStanceQrels(BaseQrels):
    _source: Any
    _source_quality: Any
    _source_stance: Any
    _definitions: Dict[int, str]

    def __init__(self, source: Any, source_quality: Any, source_stance: Any, definitions: Dict[int, str]):
        self._source = source
        self._source_quality = source_quality
        self._source_stance = source_stance
        self._definitions = definitions

    def qrels_path(self):
        return self._source.path()

    def qrels_iter(self):
        with self._source.stream() as file, self._source_quality.stream() as file_quality, self._source_stance.stream() as file_stance:
            lines = (
                line.rstrip() 
                for line in TextIOWrapper(file)
                if line != "\n"  # Ignore blank lines.
            )
            lines_quality = (
                line.rstrip() 
                for line in TextIOWrapper(file_quality)
                if line != "\n"  # Ignore blank lines.
            )
            lines_stance = (
                line.rstrip() 
                for line in TextIOWrapper(file_stance)
                if line != "\n"  # Ignore blank lines.
            )
            zipped_lines = zip(lines, lines_quality, lines_stance)

            for zipped_line in zipped_lines:
                line, line_quality, line_stance = zipped_line

                cols = line.split()
                if len(cols) != 4:
                    raise ValueError(f"Expected 4 relevance columns but got {len(cols)}.")
                qid, it, did, score = cols

                cols_quality = line_quality.split()
                if len(cols_quality) != 4:
                    raise ValueError(f"Expected 4 quality columns but got {len(cols_quality)}.")
                qid_quality, it_quality, did_quality, score_quality = cols_quality
                if qid_quality != qid:
                    raise ValueError(f"Quality query {qid_quality} does not match relevance query {qid}.")
                if did_quality != did:
                    raise ValueError(f"Quality document {did_quality} does not match relevance document {did}.")
                if it_quality != it:
                    raise ValueError(f"Quality iteration {it_quality} does not match relevance iteration {it}.")

                cols_stance = line_stance.split()
                if len(cols_stance) != 4:
                    raise ValueError(f"Expected 4 stance columns but got {len(cols_stance)}.")
                qid_stance, it_stance, did_stance, score_stance = cols_stance
                if qid_stance != qid:
                    raise ValueError(f"Stance query {qid_stance} does not match relevance query {qid}.")
                if did_stance != did:
                    raise ValueError(f"Stance document {did_stance} does not match relevance document {did}.")
                if it_stance != it:
                    raise ValueError(f"Stance iteration {it_stance} does not match relevance iteration {it}.")

                yield ToucheQualityComparativeStanceQrel(
                    query_id=qid,
                    doc_id=did,
                    relevance=int(score),
                    quality=int(score_quality),
                    stance=ToucheComparativeStance(score_stance),
                    iteration=it,
                )

    def qrels_cls(self):
        return ToucheQualityComparativeStanceQrel

    def qrels_defs(self):
        return self._definitions


class ToucheControversialStanceQrels(BaseQrels):
    _source: Any
    _definitions: Dict[int, str]

    def __init__(self, source: Any, definitions: Dict[int, str]):
        self._source = source
        self._definitions = definitions

    def qrels_path(self):
        return self._source.path()

    def qrels_iter(self):
        with self._source.stream() as file:
            lines = (
                line.rstrip()
                for line in TextIOWrapper(file)
                if line != "\n"  # Ignore blank lines.
            )

            for line in lines:
                cols = line.split()
                if len(cols) != 4:
                    raise ValueError(f"Expected 4 relevance and stance columns but got {len(cols)}.")
                qid, stance, did, score = cols

                yield ToucheControversialStanceQrel(
                    query_id=qid,
                    doc_id=did,
                    relevance=int(score),
                    stance=ToucheControversialStance(stance),
                )

    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return self._definitions


class TouchePassageDocs(BaseDocs):
    _source: Cache
    _namespace: Optional[str]
    _language: Optional[str]
    _count_hint: Optional[int]

    def __init__(
            self,
            cache: Cache,
            namespace: Optional[str] = None,
            language: Optional[str] = None,
            count_hint: Optional[int] = None,
    ):
        self._source = cache
        self._namespace = namespace
        self._language = language
        self._count_hint = count_hint

    def docs_path(self):
        return self._source.path()

    @use_docstore
    def docs_iter(self):
        with self._source.stream() as file:
            lines = TextIOWrapper(file)
            for line in lines:
                json = loads(line)
                yield TouchePassageDoc(
                    doc_id=json["id"],
                    text=json["contents"],
                    chatnoir_url=json["chatNoirUrl"],
                )

    def docs_store(self, field="doc_id"):
        return PickleLz4FullStore(
            path=f"{self.docs_path()}.pklz4",
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=["doc_id"],
            count_hint=self._count_hint,
        )

    def docs_count(self):
        return self._count_hint

    def docs_cls(self):
        return TouchePassageDoc

    def docs_namespace(self):
        return self._namespace

    def docs_lang(self):
        return self._language


_PATTERN_IMAGE = compile("images/I[0-9a-f]{2}/I[0-9a-f]{16}/")
_PATTERN_PAGE = compile("images/I[0-9a-f]{2}/I[0-9a-f]{16}/pages/P[0-9a-f]{16}/")

class ToucheImageDocs(BaseDocs):
    _source: Cache
    _source_nodes: Cache
    _source_png: Cache
    _namespace: Optional[str]
    _language: Optional[str]
    _count_hint: Optional[int]

    def __init__(
            self,
            source: Cache,
            source_nodes: Cache,
            source_png: Cache,
            namespace: Optional[str] = None,
            language: Optional[str] = None,
            count_hint: Optional[int] = None,
    ):
        self._source = source
        self._source_nodes = source_nodes
        self._source_png = source_png
        self._namespace = namespace
        self._language = language
        self._count_hint = count_hint

    def docs_path(self):
        return self._source.path()

    @use_docstore
    def docs_iter(self):
        with self._source.stream() as file, self._source_nodes.stream() as file_nodes, self._source_png.stream() as file_png:
            with ZipFile(file) as zip_file, ZipFile(file_nodes) as zip_file_nodes, ZipFile(file_png) as zip_file_png:
                paths = {
                    *zip_file.namelist(),
                    *zip_file_nodes.namelist(),
                    *zip_file_png.namelist(),
                }
                image_paths = {
                    path
                    for path in paths
                    if _PATTERN_IMAGE.fullmatch(path)
                }
                for image_path in image_paths:
                    page_paths = {
                        path
                        for path in paths
                        if path.startswith(image_path) and _PATTERN_PAGE.fullmatch(path)
                    }
                    pages: List[ToucheImagePage] = []
                    for page_path in page_paths:
                        with zip_file.open(f"{page_path}rankings.jsonl") as rankings_file:
                            rankings_json = (
                                loads(line)
                                for line in TextIOWrapper(rankings_file)
                            )
                            rankings = [
                                ToucheImageRanking(
                                    query_id=json["topic"],
                                    query=json["query"],
                                    rank=int(json["rank"]),
                                )
                                for json in rankings_json
                            ]
                        with zip_file_nodes.open(f"{page_path}snapshot/nodes.jsonl") as nodes_file:
                            nodes_json = (
                                loads(line)
                                for line in TextIOWrapper(nodes_file)
                            )

                            nodes = [
                                ToucheImageNode(
                                    xpath=json["xPath"],
                                    visible=bool(json["visible"]),
                                    id=json["id"] if "id" in json else None,
                                    classes=json["classes"] if "classes" in json else [],
                                    position=(
                                        int(json["position"][0]),
                                        int(json["position"][1]),
                                        int(json["position"][2]),
                                        int(json["position"][3]),
                                    ),
                                    text=json["text"] if "text" in json else None,
                                    css=json["css"] if "css" in json else {},
                                )
                                for json in nodes_json
                            ]
                        with zip_file.open(f"{page_path}snapshot/image-xpath.txt") as xpaths_file:
                            xpaths = [
                                line
                                for line in TextIOWrapper(xpaths_file)
                            ]
                        pages.append(ToucheImagePage(
                            page_id=page_path.split("/")[-1],
                            url=zip_file.read(f"{page_path}page-url.txt").decode().strip(),
                            rankings=rankings,
                            dom_html=zip_file.read(f"{page_path}snapshot/dom.html"),
                            xpaths=xpaths,
                            nodes=nodes,
                            # Mentioned in the dataset description but not included in the Zenodo dataset.
                            # screenshot_png=zip_file_screenshots.read(f"{page_path}snapshot/screenshot.png"),
                            text=zip_file.read(f"{page_path}snapshot/text.txt").decode(),
                            # Mentioned in the dataset description but not included in the Zenodo dataset.
                            # warc_gz=zip_file_archives.read(f"{page_path}snapshot/web-archive.warc.gz"),
                        ))


                    yield ToucheImageDoc(
                        doc_id=image_path.split("/")[-1],
                        png=zip_file_png.read(f"{image_path}image.png"),
                        webp=zip_file.read(f"{image_path}image.webp"),
                        url=zip_file.read(f"{image_path}image-url.txt").decode().strip(),
                        phash=zip_file.read(f"{image_path}image-phash.txt").decode().strip(),
                        pages=pages,
                    )




    def docs_store(self, field="doc_id"):
        return PickleLz4FullStore(
            path=f"{self.docs_path()}.pklz4",
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=["doc_id"],
            count_hint=self._count_hint,
        )

    def docs_count(self):
        return self._count_hint

    def docs_cls(self):
        return ToucheImageDoc

    def docs_namespace(self):
        return self._namespace

    def docs_lang(self):
        return self._language
