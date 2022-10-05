from io import TextIOWrapper
from itertools import takewhile
from json import loads
from re import compile
from typing import NamedTuple, Optional, Dict, List, Tuple
from zipfile import ZipFile

from ir_datasets.formats import BaseDocs
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.util import Cache, use_docstore


class ToucheImageRanking(NamedTuple):
    query_id: str
    query: str
    rank: int  # 1-indexed


class ToucheImageNode(NamedTuple):
    xpath: str
    visible: bool
    id: Optional[str]
    classes: List[str]
    position: Tuple[float, float, float, float]  # left top right bottom
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


_PATTERN_IMAGE = compile("images/I[0-9a-f]{2}/I[0-9a-f]{16}/")
_PATTERN_PAGE = compile(
    "images/I[0-9a-f]{2}/I[0-9a-f]{16}/pages/P[0-9a-f]{16}/"
)


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
        with self._source.stream() as file, \
                self._source_nodes.stream() as file_nodes, \
                self._source_png.stream() as file_png:
            with ZipFile(file) as zip_file, \
                    ZipFile(file_nodes) as zip_file_nodes, \
                    ZipFile(file_png) as zip_file_png:
                paths = {
                    *zip_file.namelist(),
                    *zip_file_nodes.namelist(),
                    *zip_file_png.namelist(),
                }
                paths = sorted(paths)
                image_paths = [
                    path
                    for path in paths
                    if _PATTERN_IMAGE.fullmatch(path)
                ]

                def _parse_node(json: dict) -> ToucheImageNode:
                    position_json = json["position"]
                    if isinstance(position_json, str):
                        position_json = loads(position_json)
                    return ToucheImageNode(
                        xpath=json["xPath"],
                        visible=bool(json["visible"]),
                        id=(
                            json["id"]
                            if "id" in json else None
                        ),
                        classes=(
                            json["classes"]
                            if "classes" in json else []
                        ),
                        position=(
                            float(position_json[0]),
                            float(position_json[1]),
                            float(position_json[2]),
                            float(position_json[3]),
                        ),
                        text=(
                            json["text"]
                            if "text" in json else None
                        ),
                        css=json["css"] if "css" in json else {},
                    )

                def _parse_page(page_path: str) -> ToucheImagePage:
                    with zip_file.open(
                            f"{page_path}rankings.jsonl"
                    ) as rankings_file:
                        with TextIOWrapper(rankings_file) as lines:
                            rankings_json = (
                                loads(line)
                                for line in lines
                            )
                            rankings = [
                                ToucheImageRanking(
                                    query_id=json["topic"],
                                    query=json["query"],
                                    rank=int(json["rank"]),
                                )
                                for json in rankings_json
                            ]
                    with zip_file_nodes.open(
                            f"{page_path}snapshot/nodes.jsonl"
                    ) as nodes_file:
                        with TextIOWrapper(nodes_file) as lines:
                            nodes_json = (
                                loads(line)
                                for line in lines
                            )
                            nodes = [
                                _parse_node(json)
                                for json in nodes_json
                            ]
                    with zip_file.open(
                            f"{page_path}snapshot/image-xpath.txt"
                    ) as xpaths_file:
                        with TextIOWrapper(xpaths_file) as lines:
                            xpaths = [
                                line
                                for line in lines
                            ]
                    return ToucheImagePage(
                        page_id=page_path.split("/")[-2],
                        url=zip_file.read(
                            f"{page_path}page-url.txt"
                        ).decode().strip(),
                        rankings=rankings,
                        dom_html=zip_file.read(
                            f"{page_path}snapshot/dom.html"
                        ),
                        xpaths=xpaths,
                        nodes=nodes,
                        # Mentioned in the dataset description
                        # but not included in the Zenodo dataset.
                        # screenshot_png=zip_file_screenshots.read(
                        #     f"{page_path}snapshot/screenshot.png"
                        # ),
                        text=zip_file.read(
                            f"{page_path}snapshot/text.txt"
                        ).decode(),
                        # Mentioned in the dataset description
                        # but not included in the Zenodo dataset.
                        # warc_gz=zip_file_archives.read(
                        #     f"{page_path}snapshot/web-archive.warc.gz"
                        # ),
                    )

                for index, image_path in enumerate(image_paths):
                    page_paths = list(takewhile(
                        lambda path: path.startswith(image_path),
                        paths[index:],
                    ))
                    page_paths = [
                        path
                        for path in page_paths
                        if _PATTERN_PAGE.fullmatch(path)
                    ]
                    pages: List[ToucheImagePage] = [
                        _parse_page(page_path)
                        for page_path in page_paths
                    ]

                    yield ToucheImageDoc(
                        doc_id=image_path.split("/")[-2],
                        png=zip_file_png.read(f"{image_path}image.png"),
                        webp=zip_file.read(f"{image_path}image.webp"),
                        url=zip_file.read(
                            f"{image_path}image-url.txt"
                        ).decode().strip(),
                        phash=zip_file.read(
                            f"{image_path}image-phash.txt"
                        ).decode().strip(),
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
