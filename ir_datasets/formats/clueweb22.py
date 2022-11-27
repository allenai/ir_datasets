from collections import defaultdict
from contextlib import ExitStack, contextmanager
from csv import reader
from datetime import datetime
from enum import Enum
from functools import cached_property
from gzip import GzipFile
from io import TextIOWrapper
from itertools import chain
from json import loads
from os import PathLike
from os.path import join
from pathlib import Path
from typing import (
    NamedTuple, Sequence, TypeVar, Optional, Type, Final, Iterator, IO,
    TYPE_CHECKING, Iterable, Callable, ContextManager, Mapping, Union,
    AbstractSet, MutableSet, Tuple
)
from zipfile import ZipFile

from ir_datasets.formats import BaseDocs
from ir_datasets.indices import Docstore
from ir_datasets.lazy_libs import fastwarc
from ir_datasets.log import easy
from ir_datasets.util import Download, apply_sub_slice, slice_idx
from ir_datasets.util.io import OffsetIOWrapper

# Logging.

_logger = easy("clueweb22")

# Constants and constraints.


MAX_SUBDIRECTORIES_PER_STREAM: Final[int] = 80
MAX_FILES_PER_SUBDIRECTORY: Final[int] = 100
ENCODING = "utf8"


# Base records corresponding to the file types listed
# at https://lemurproject.org/clueweb22/docspecs.php


class _Txt(NamedTuple):
    """
    Record from the ``txt`` subdir.
    """
    doc_id: str
    url: str
    url_hash: str
    language: str
    text: str


class AnnotationType(Enum):
    NONE = "None"
    PRIMARY = "Primary"
    HEADING = "Heading"
    TITLE = "Title"
    PARAGRAPH = "Paragraph"
    TABLE = "Table"
    LIST = "List"


class _Html(NamedTuple):
    """
    Record from the ``html`` subdir.
    """
    doc_id: str
    url: str
    url_hash: str
    language: str
    date: datetime
    html: bytes
    vdom_nodes: Mapping[AnnotationType, Sequence[int]]


class Anchor(NamedTuple):
    """
    Anchor sub-record from the ``inlink`` and ``outlink`` subdirs.
    """
    url: str
    url_hash: str
    text: str
    language: str


class _Link(NamedTuple):
    """
    Record from the ``inlink`` and ``outlink`` subdirs.
    """
    doc_id: str
    url: str
    url_hash: str
    anchors: Sequence[Anchor]


class _Vdom(NamedTuple):
    """
    Record from the ``vdom`` subdirs.
    """
    # Not parsed yet because parsing would require the protobuf library.
    vdom_data: bytes


class _Jpg(NamedTuple):
    """
    Record from the ``jpg`` subdirs.
    """
    doc_id: str
    url: str
    url_hash: str
    # TODO how to parse?


_AnyRecord = TypeVar("_AnyRecord", _Txt, _Html, _Link, _Vdom, _Jpg)


# Readers for parsing the base record types from iterators of IO streams.


def _read_txt(files: Iterator[IO[bytes]]) -> Iterator[_Txt]:
    for file in files:
        with TextIOWrapper(file, encoding=ENCODING) as text_file:
            for line in text_file:
                json = loads(line)
                yield _Txt(
                    doc_id=json["ClueWeb22-ID"],
                    # Bug:
                    # URLs from txt records contain an additional new line (\n)
                    # at the end of the URL but the other records don't.
                    url=json["URL"].removesuffix("\n"),
                    url_hash=json["URL-hash"],
                    language=json["Language"],
                    text=json["Clean-Text"],
                )


def _read_html(files: Iterator[IO[bytes]]) -> Iterator[_Html]:
    # Only import the heavy warcio library for type checking,
    # otherwise load the library lazily.
    if TYPE_CHECKING:
        from fastwarc import ArchiveIterator, WarcRecordType
    else:
        # Ignore PEP8 naming here, because of lazy libs import.
        # noinspection PyPep8Naming
        ArchiveIterator = fastwarc().ArchiveIterator
        # noinspection PyPep8Naming
        WarcRecordType = fastwarc().WarcRecordType

    for file in files:
        for document in ArchiveIterator(
                file,
                record_types=WarcRecordType.response,
                parse_http=False,
        ):
            doc_id = document.headers["ClueWeb22-ID"]
            url = document.headers['WARC-Target-URI']
            url_hash = document.headers["URL-Hash"]
            language = document.headers["Language"]
            date = datetime.strptime(
                document.headers["WARC-Date"],
                "%Y-%m-%dT%H:%M:%S.%fZ",
            )
            vdom_nodes = {
                annotation_type: [
                    int(vdom)
                    for vdom in document.headers.get(
                        f"VDOM-{annotation_type.value}", ""
                    ).split()
                ]
                for annotation_type in AnnotationType
            }
            html: bytes = document.reader.read().removesuffix(b"\r\n")
            yield _Html(
                doc_id=doc_id,
                url=url,
                url_hash=url_hash,
                language=language,
                date=date,
                html=html,
                vdom_nodes=vdom_nodes,
            )


def _parse_anchor(json: Sequence[str]) -> Anchor:
    return Anchor(
        url=json[0],
        url_hash=json[1],
        text=json[2],
        language=json[4],
    )


def _read_inlink(files: Iterator[IO[bytes]]) -> Iterator[Optional[_Link]]:
    for file in files:
        with TextIOWrapper(file, encoding=ENCODING) as text_file:
            for line in text_file:
                if len(line.strip()) == 0:
                    yield None
                    continue
                json = loads(line)
                yield _Link(
                    doc_id=json["ClueWeb22-ID"],
                    url=json["url"],
                    url_hash=json["urlhash"],
                    anchors=[
                        _parse_anchor(anchor) for anchor in json["anchors"]
                    ],
                )


def _read_outlink(files: Iterator[IO[bytes]]) -> Iterator[Optional[_Link]]:
    for file in files:
        with TextIOWrapper(file, encoding=ENCODING) as text_file:
            for line in text_file:
                if len(line.strip()) == 0:
                    yield None
                    continue
                json = loads(line)
                yield _Link(
                    doc_id=json["ClueWeb22-ID"],
                    url=json["url"],
                    url_hash=json["urlhash"],
                    anchors=[
                        _parse_anchor(anchor) for anchor in json["outlinks"]
                    ],
                )


def _read_vdom(files: Iterator[IO[bytes]]) -> Iterator[_Vdom]:
    for file in files:
        yield _Vdom(
            vdom_data=file.read()
        )


def _read_jpg(files: Iterator[IO[bytes]]) -> Iterator[_Jpg]:
    raise NotImplementedError()


# Doc records corresponding to the fields available for each subset listed
# at https://lemurproject.org/clueweb22/obtain.php and in the SIRIP paper.


class ClueWeb22LDoc(NamedTuple):
    doc_id: str
    url: str
    url_hash: str
    language: str
    text: str


class ClueWeb22ADoc(NamedTuple):
    doc_id: str
    url: str
    url_hash: str
    language: str
    text: str
    date: datetime
    html: bytes
    vdom_nodes: Mapping[AnnotationType, Sequence[int]]
    vdom_data: bytes
    inlink_anchors: Sequence[Anchor]
    outlink_anchors: Sequence[Anchor]


class ClueWeb22BDoc(NamedTuple):
    doc_id: str
    url: str
    url_hash: str
    language: str
    text: str
    date: datetime
    html: bytes
    vdom_nodes: Mapping[AnnotationType, Sequence[int]]
    vdom_data: bytes
    inlink_anchors: Sequence[Anchor]
    outlink_anchors: Sequence[Anchor]


AnyDoc = TypeVar("AnyDoc", ClueWeb22LDoc, ClueWeb22ADoc, ClueWeb22BDoc)


# Combining iterators to construct documents from base records.from


def _combine_l_docs(txt_iterator: Iterator[_Txt]) -> Iterator[ClueWeb22LDoc]:
    for txt in txt_iterator:
        yield ClueWeb22LDoc(
            doc_id=txt.doc_id,
            url=txt.url,
            url_hash=txt.url_hash,
            language=txt.language,
            text=txt.text,
        )


def _combine_a_docs(
        txt_iterator: Iterator[_Txt],
        html_iterator: Iterator[_Html],
        inlink_iterator: Iterator[Optional[_Link]],
        outlink_iterator: Iterator[Optional[_Link]],
        vdom_iterator: Iterator[_Vdom],
) -> Iterator[ClueWeb22ADoc]:
    zipped = zip(
        txt_iterator,
        html_iterator,
        inlink_iterator,
        outlink_iterator,
        vdom_iterator,
    )
    for txt, html, inlink, outlink, vdom in zipped:
        assert txt.doc_id == html.doc_id
        if txt.url != html.url:
            # Bug:
            # URLs from txt records are truncated to everything
            # before the first comma (,).
            # Example: For clueweb22-de0000-00-00366, the html URL
            # is https://www.anisearch.de/manga/43556,verrueckt-nach-dir but
            # the txt URL hash is just https://www.anisearch.de/manga/43556.
            _logger.debug(
                f"URL mismatch for {txt.doc_id}: "
                f"txt URL was {txt.url} but "
                f"html URL was {html.url}"
            )
            assert "," in html.url and html.url.split(",")[0] == txt.url
        if txt.url_hash != html.url_hash:
            # Bug:
            # Sometimes, URL hashes from txt records do not match the
            # corresponding URL hashes from other records.
            # Example: For clueweb22-de0000-00-13406, the html URL hash
            # is B6956297B5EBBDFEAABF458F2FA5EADC but the txt URL hash
            # is 9D5A53C6ACCB07B2C2319A4E5E44AB76.
            _logger.warn(
                f"URL hash mismatch for {txt.doc_id}: "
                f"txt URL hash was {txt.url_hash} but "
                f"html URL hash was {html.url_hash}"
            )
        assert txt.language == html.language
        if inlink is not None:
            assert inlink.doc_id == html.doc_id
            if inlink.url != html.url:
                # Bug:
                # Sometimes, URLs from inlink records do not match the
                # corresponding URLs from other records.
                # Example: For clueweb22-de0000-01-14834, the html URL
                # is https://simon-transporte.com/ but the inlink URL
                # is https://simon.ccbcmd.edu/pls/PROD/bwskalog.p_disploginnew?in_id=&cpbl=&newid=.
                _logger.warn(
                    f"URL mismatch for {html.doc_id}: "
                    f"inlink URL was {inlink.url} but "
                    f"html URL was {html.url}"
                )
            if inlink.url_hash != html.url_hash:
                # Bug:
                # Sometimes, URL hashes from inlink records do not match the
                # corresponding URL hashes from other records.
                # Example: For clueweb22-de0000-01-14834, the html URL hash
                # is 825E120CE7F82C8B0268440A59107D04 but the inlink URL hash
                # is 612691A107701D76AD36FD32F8608F3C.
                _logger.warn(
                    f"URL hash mismatch for {txt.doc_id}: "
                    f"inlink URL hash was {txt.url_hash} but "
                    f"html URL hash was {html.url_hash}"
                )
        if outlink is not None:
            assert outlink.doc_id == html.doc_id
            if outlink.url != html.url:
                # Bug:
                # Sometimes, URLs from outlink records do not match the
                # corresponding URLs from other records.
                # Example: For clueweb22-de0000-00-13406, the html URL
                # is https://www.jovanna.de/ but the outlink URL
                # is https://www.jovanovic.com/quotidien.htm.
                _logger.warn(
                    f"URL mismatch for {html.doc_id}: "
                    f"outlink URL was {outlink.url} but "
                    f"html URL was {html.url}"
                )
            if outlink.url_hash != html.url_hash:
                # Bug:
                # Sometimes, URL hashes from outlink records do not match the
                # corresponding URL hashes from other records.
                # Example: For clueweb22-de0000-00-13406, the html URL hash
                # is B6956297B5EBBDFEAABF458F2FA5EADC but the outlink URL hash
                # is 9D5A53C6ACCB07B2C2319A4E5E44AB76.
                _logger.warn(
                    f"URL hash mismatch for {txt.doc_id}: "
                    f"outlink URL hash was {txt.url_hash} but "
                    f"html URL hash was {html.url_hash}"
                )
        yield ClueWeb22ADoc(
            doc_id=html.doc_id,
            url=html.url,
            url_hash=html.url_hash,
            language=html.language,
            text=txt.text,
            date=html.date,
            html=html.html,
            vdom_nodes=html.vdom_nodes,
            vdom_data=vdom.vdom_data,
            inlink_anchors=inlink.anchors if inlink is not None else [],
            outlink_anchors=outlink.anchors if outlink is not None else [],
        )


def _combine_b_docs(
        txt_iterator: Iterator[_Txt],
        html_iterator: Iterator[_Html],
        inlink_iterator: Iterator[Optional[_Link]],
        outlink_iterator: Iterator[Optional[_Link]],
        vdom_iterator: Iterator[_Vdom],
        jpg_iterator: Iterator[_Jpg],
) -> Iterator[ClueWeb22BDoc]:
    zipped = zip(
        txt_iterator,
        html_iterator,
        inlink_iterator,
        outlink_iterator,
        vdom_iterator,
        jpg_iterator,
    )
    for txt, html, inlink, outlink, vdom, jpg in zipped:
        assert txt.doc_id == html.doc_id
        if not txt.url == html.url:
            # Bug in ClueWeb22:
            # The URL in the txt record does not match the URL in the html
            # record but is a prefix thereof.
            # The txt URL seems to be split by comma.
            # Example:
            # - txt: https://www.anisearch.de/manga/43556
            # - html: https://www.anisearch.de/manga/43556,verrueckt-nach-dir
            assert "," in html.url and html.url.split(",")[0] == txt.url
        assert txt.url_hash == html.url_hash
        assert txt.language == html.language
        if inlink is not None:
            assert html.doc_id == inlink.doc_id
            assert html.url == inlink.url
            assert html.url_hash == inlink.url_hash
        if outlink is not None:
            assert html.doc_id == outlink.doc_id
            assert html.url == outlink.url
            assert html.url_hash == outlink.url_hash
        yield ClueWeb22BDoc(
            doc_id=html.doc_id,
            url=html.url,
            url_hash=html.url_hash,
            language=html.language,
            text=txt.text,
            date=html.date,
            html=html.html,
            vdom_nodes=html.vdom_nodes,
            vdom_data=vdom.vdom_data,
            inlink_anchors=inlink.anchors if inlink is not None else [],
            outlink_anchors=outlink.anchors if outlink is not None else [],
        )


# Type-safe configuration for the subsets, describing the characteristics
# of files and compression types.


class ClueWeb22Compression(Enum):
    GZIP = 1
    ZIP = 2


class _FormatInfo(NamedTuple):
    id: str
    """
    ClueWeb22 format as described 
    at https://lemurproject.org/clueweb22/docspecs.php#Organization
    """
    extension: str
    """
    Offset file extension.
    """
    offset_extension: Optional[str]
    """
    File extension of a single compressed file.
    """
    compression: ClueWeb22Compression
    """
    Compression form as described 
    at https://lemurproject.org/clueweb22/docspecs.php#Compression
    """
    compression_extension: Optional[str]
    """
    File extension of files within the compressed archive.
    """
    reader: Callable[[Iterator[IO[bytes]]], Iterator[_AnyRecord]]
    """
    Function for reading records from the decompressed files.
    """


class ClueWeb22Format(Enum):
    value: _FormatInfo

    HTML = _FormatInfo(
        id="html",
        extension=".warc.gz",
        offset_extension=".warc.offset",
        compression=ClueWeb22Compression.GZIP,
        compression_extension=None,
        reader=_read_html,
    )
    INLINK = _FormatInfo(
        id="inlink",
        extension=".json.gz",
        offset_extension=".offset",
        compression=ClueWeb22Compression.GZIP,
        compression_extension=None,
        reader=_read_inlink,
    )
    OUTLINK = _FormatInfo(
        id="outlink",
        extension=".json.gz",
        offset_extension=".offset",
        compression=ClueWeb22Compression.GZIP,
        compression_extension=None,
        reader=_read_outlink,
    )
    TXT = _FormatInfo(
        id="txt",
        extension=".json.gz",
        offset_extension=".offset",
        compression=ClueWeb22Compression.GZIP,
        compression_extension=None,
        reader=_read_txt,
    )
    JPG = _FormatInfo(
        id="jpg",
        extension=NotImplemented,
        offset_extension=NotImplemented,
        compression=NotImplemented,
        compression_extension=NotImplemented,
        reader=_read_jpg,
    )
    VDOM = _FormatInfo(
        id="vdom",
        extension=".zip",
        offset_extension=None,
        compression=ClueWeb22Compression.ZIP,
        compression_extension=".bin",
        reader=_read_vdom,
    )


class _LanguageInfo(NamedTuple):
    id: str
    """
    ClueWeb22 language ID as described 
    at https://lemurproject.org/clueweb22/docspecs.php#Organization
    """
    tag: str
    """
    Shorthand tag to be used as suffix in the dataset ID.
    """


class ClueWeb22Language(Enum):
    value: _LanguageInfo

    DE = _LanguageInfo(id="de", tag="de")
    EN = _LanguageInfo(id="en", tag="en")
    ES = _LanguageInfo(id="es", tag="es")
    FR = _LanguageInfo(id="fr", tag="fr")
    IT = _LanguageInfo(id="it", tag="it")
    JA = _LanguageInfo(id="ja", tag="ja")
    NL = _LanguageInfo(id="nl", tag="nl")
    PO = _LanguageInfo(id="po", tag="po")
    PT = _LanguageInfo(id="pt", tag="pt")
    ZH = _LanguageInfo(id="zh_chs", tag="zh")
    OTHER = _LanguageInfo(id="other", tag="other-languages")


class _SubsetInfo(NamedTuple):
    id: str
    """
    ClueWeb22 subset name as described 
    at https://lemurproject.org/clueweb22/index.php#Specs
    """
    tag: str
    """
    Shorthand to be used as suffix in the dataset ID.
    """
    formats: Sequence[ClueWeb22Format]
    """
    Required formats for constructing a document for this subset.
    """
    doc_type: Type[AnyDoc]
    """
    Type of one single document.
    """
    combiner: Callable[[...], Iterator[AnyDoc]]
    """
    Function for combining iterables of the different format records 
    to documents. The record iterators are passed to the function 
    in the same order as specified in the ``formats`` field.
    """
    extends: Optional[str]
    """
    Subset ID that this subset extends, meaning that it supports 
    all fields from the extended subset.
    """


class ClueWeb22Subset(Enum):
    value: _SubsetInfo

    L = _SubsetInfo(
        id="L",
        tag="l",
        formats=[ClueWeb22Format.TXT],
        doc_type=ClueWeb22LDoc,
        combiner=_combine_l_docs,
        extends=None,
    )
    A = _SubsetInfo(
        id="A",
        tag="a",
        formats=[
            ClueWeb22Format.TXT,
            ClueWeb22Format.HTML,
            ClueWeb22Format.INLINK,
            ClueWeb22Format.OUTLINK,
            ClueWeb22Format.VDOM
        ],
        doc_type=ClueWeb22ADoc,
        combiner=_combine_a_docs,
        extends="L",
    )
    B = _SubsetInfo(
        id="B",
        tag="b",
        formats=[
            ClueWeb22Format.TXT,
            ClueWeb22Format.HTML,
            ClueWeb22Format.INLINK,
            ClueWeb22Format.OUTLINK,
            ClueWeb22Format.VDOM,
            ClueWeb22Format.JPG
        ],
        doc_type=ClueWeb22BDoc,
        combiner=_combine_b_docs,
        extends="A",
    )

    @property
    def compatible_subsets(self) -> AbstractSet["ClueWeb22Subset"]:
        if self.value.extends is not None:
            extends = next(
                s for s in ClueWeb22Subset
                if s.value.id == self.value.extends
            )
            return {self} | extends.compatible_subsets
        return {self}


# Utility classes


class _ClueWeb22DocId:
    """
    ClueWeb22 document ID as described
    at https://lemurproject.org/clueweb22/docspecs.php#DocIds.

    This class can be used to check the ID format and decompose it
    into its individual components, e.g., to construct a file path.
    """

    language: str
    stream: int
    subdirectory: int
    file: int
    doc: int

    def __init__(self, doc_id: str):
        parts = doc_id.split("-")
        if len(parts) != 4:
            raise ValueError(f"Invalid ClueWeb22 ID: {doc_id}")
        dataset, subdirectory, file, doc_index = parts
        if dataset != "clueweb22":
            raise ValueError(f"Invalid ClueWeb22 ID: {doc_id}")

        self.language = subdirectory[:-4]
        if not any(
                self.language == language.value.id
                for language in ClueWeb22Language
        ):
            raise ValueError(f"Invalid ClueWeb22 ID: {doc_id}")

        self.stream_id = int(subdirectory[-4:-2])

        self.subdirectory = int(subdirectory[-2:])
        if self.subdirectory > MAX_SUBDIRECTORIES_PER_STREAM:
            raise ValueError(f"Invalid ClueWeb22 ID: {doc_id}")

        self.file = int(file)
        if self.file > MAX_FILES_PER_SUBDIRECTORY:
            raise ValueError(f"Invalid ClueWeb22 ID: {doc_id}")

        self.doc = int(doc_index)

    @cached_property
    def path(self) -> str:
        language_path = self.language
        stream_path = f"{language_path}{self.stream_id:0>2}"
        subdirectory_path = f"{stream_path}{self.subdirectory:0>2}"
        file_path = f"{subdirectory_path}-{self.file:0>2}"
        return join(
            language_path,
            stream_path,
            subdirectory_path,
            file_path
        )

    def __str__(self) -> str:
        return "-".join([
            "clueweb22",
            f"{self.language}{self.stream_id:0>2}{self.subdirectory:0>2}",
            f"{self.file:0>2}",
            f"{self.doc:0>5}",
        ])


# Docs dataset class.


class ClueWeb22Docs(BaseDocs):
    name: Final[str]
    source: Final[Download]
    subset: Final[ClueWeb22Subset]
    language: Final[Optional[ClueWeb22Language]]

    # TODO Filter files by subset

    def __init__(
            self,
            name: str,
            source: Download,
            subset: ClueWeb22Subset,
            language: Optional[ClueWeb22Language] = None,
    ):
        super().__init__()
        self.name = name
        self.source = source
        self.subset = subset
        self.language = language
        subset, major, minor = self.version
        assert self.subset in subset.compatible_subsets
        assert major >= 1

    def docs_path(self, force: bool = True) -> Union[str, PathLike[str]]:
        return self.source.path(force)

    @cached_property
    def path(self) -> Path:
        return Path(self.source.path())

    @cached_property
    def readme(self) -> str:
        readme_path = self.path / "README.txt"
        with readme_path.open("rt", encoding=ENCODING) as file:
            return file.read()

    @cached_property
    def version(self) -> Tuple[ClueWeb22Subset, int, int]:
        version_files = list(self.path.glob("version_*"))
        assert len(version_files) == 1
        version_file = version_files[0]
        version_file_name = version_file.name
        _, subset_id, version = version_file_name.split("_")
        assert len(subset_id) == 1
        subset = next(s for s in ClueWeb22Subset if s.value.id == subset_id)
        major, minor = version.split(".")
        return subset, int(major), int(minor)

    @cached_property
    def record_counts(self) -> Mapping[
        Tuple[str, str, int, int, int],
        int
    ]:
        """
        Mapping from the format ID, language ID, stream, subdirectory, and file
        to the number of documents within the file.
        """

        counts_dir = self.path / "record_counts" / "record_counts"
        counts = {}
        for format_type in self.subset.value.formats:
            format_counts_dir = counts_dir / format_type.value.id
            format_counts_files = format_counts_dir.glob(
                "*_counts.csv"
            )
            for format_counts_file in format_counts_files:
                tag = format_counts_file.name \
                    .removesuffix("_counts.csv")
                language = tag[:-2]
                stream = int(tag[-2:])
                with open(format_counts_file, "rt", encoding=ENCODING) as file:
                    csv_reader = reader(file)
                    for file_name, count in csv_reader:
                        subdirectory_tag, file_tag = file_name.split("-")
                        file = int(file_tag)
                        subdirectory = int(subdirectory_tag[-2:])
                        counts[
                            format_type.value.id,
                            language,
                            stream,
                            subdirectory,
                            file,
                        ] = int(count)
        return counts

    def docs_iter(self) -> Iterator[AnyDoc]:
        return _ClueWeb22Iterator(self)

    def docs_store(self) -> "ClueWeb22Docstore":
        return ClueWeb22Docstore(self)

    def docs_cls(self) -> Type[AnyDoc]:
        return self.subset.value.doc_type

    def docs_count(self) -> int:
        return sum(self.record_counts.values())

    def docs_namespace(self) -> str:
        names = [self.name, self.subset.value.tag]
        if self.language is not None:
            names.append(self.language.value.tag)
        return "/".join(names)

    def docs_lang(self) -> Optional[str]:
        if self.language is None:
            return None
        return self.language.value.tag


# Iterators and doc store classes for accessing multiple documents efficiently.


class _ClueWeb22Iterable(Iterable[AnyDoc]):
    subset: Final[ClueWeb22Subset]
    files: Final[Callable[
        [ClueWeb22Format], ContextManager[Iterator[IO[bytes]]]
    ]]

    def __init__(
            self,
            subset: ClueWeb22Subset,
            files: Callable[
                [ClueWeb22Format], ContextManager[Iterator[IO[bytes]]]
            ],
    ):
        self.subset = subset
        self.files = files

    def __iter__(self) -> Iterator[AnyDoc]:
        formats = self.subset.value.formats
        with ExitStack() as stack:
            format_files: Iterator[Iterator[IO[bytes]]] = (
                stack.enter_context(self.files(format_type))
                for format_type in formats
            )
            format_records: Iterator[Iterator[_AnyRecord]] = (
                format_type.value.reader(files)
                for format_type, files in zip(formats, format_files)
            )
            documents = self.subset.value.combiner(*format_records)
            yield from documents


class _ClueWeb22Iterator(Iterator[AnyDoc]):
    docs: Final[ClueWeb22Docs]
    full_iterator: Final[Iterator[AnyDoc]]

    def __init__(self, docs: ClueWeb22Docs):
        self.docs = docs
        self.full_iterator = iter(
            _ClueWeb22Iterable(self.docs.subset, self._all_files)
        )

    def __next__(self) -> AnyDoc:
        return next(self.full_iterator)

    @staticmethod
    def _in_slice(
            index: int,
            start: int,
            stop: int,
            step: int,
    ) -> bool:
        if not (start <= index < stop):
            return False
        return (index - start) % step == 0

    def _file_paths(self, format_type: ClueWeb22Format) -> Sequence[Path]:
        languages: Iterable[ClueWeb22Language]
        if self.docs.language is not None:
            languages = {self.docs.language}
        else:
            languages = {language for language in ClueWeb22Language}
        language_ids = {language.value.id for language in languages}
        patterns = (
            join(
                format_type.value.id,
                language_id,
                f"{language_id}[0-9][0-9]",
                f"{language_id}[0-9][0-9][0-9][0-9]",
                f"{language_id}[0-9][0-9][0-9][0-9]-[0-9][0-9]"
                f"{format_type.value.extension}",
            )
            for language_id in language_ids
        )
        paths = chain.from_iterable(
            self.docs.path.glob(pattern)
            for pattern in patterns
        )
        return sorted(paths)

    @contextmanager
    def _all_files(self, format_type: ClueWeb22Format) -> Iterator[IO[bytes]]:
        compression = format_type.value.compression
        compression_extension = format_type.value.compression_extension

        def generator() -> Iterator[IO[bytes]]:
            file_paths = self._file_paths(format_type)
            for file_path in file_paths:
                with file_path.open("rb") as file:
                    if compression == ClueWeb22Compression.GZIP:
                        assert compression_extension is None

                        # Read offsets:
                        offsets_name = (
                                file_path.name.removesuffix(
                                    format_type.value.extension
                                ) + format_type.value.offset_extension
                        )
                        offsets_file_path = file_path.with_name(offsets_name)
                        with offsets_file_path.open(
                                "rt", encoding=ENCODING) as offsets_file:
                            first_offset = next(
                                int(offset)
                                for offset in offsets_file
                            )

                        file.seek(first_offset)

                        with GzipFile("rb", fileobj=file) as gzip_file:
                            yield gzip_file
                    elif compression == ClueWeb22Compression.ZIP:
                        assert compression_extension is not None
                        with ZipFile(file, "r") as zip_file:
                            for name in zip_file.namelist():
                                if name.endswith(compression_extension):
                                    yield zip_file.open(name, "r")
                    else:
                        raise ValueError(
                            f"Unknown compression format: {compression}"
                        )

        yield generator()

    @contextmanager
    def _slice_files(
            self,
            format_type: ClueWeb22Format,
            start: int,
            stop: int,
            step: int,
    ) -> Iterator[IO[bytes]]:
        compression = format_type.value.compression
        compression_extension = format_type.value.compression_extension

        def generator() -> Iterator[IO[bytes]]:
            file_paths = self._file_paths(format_type)
            file_index_offset = 0
            for file_path in file_paths:
                with file_path.open("rb") as file:
                    if compression == ClueWeb22Compression.GZIP:
                        assert compression_extension is None

                        # Read offsets:
                        offsets_name = (
                                file_path.name.removesuffix(
                                    format_type.value.extension
                                ) + format_type.value.offset_extension
                        )
                        offsets_file_path = file_path.with_name(offsets_name)
                        with offsets_file_path.open(
                                "rt", encoding=ENCODING) as offsets_file:
                            offsets = [
                                int(offset)
                                for offset in offsets_file
                            ]

                            # Map global indices to file indices.
                            num_indices = len(offsets) - 1
                            file_indices = range(num_indices)
                            index_file_indices = (
                                (file_index_offset + file_index, file_index)
                                for file_index in file_indices
                            )

                            # Determine local indices to read.
                            file_indices = {
                                file_index
                                for index, file_index in index_file_indices
                                if self._in_slice(index, start, stop, step)
                            }
                            if len(file_indices) == 0:
                                # No indices in this file.
                                continue

                            # Wrap file to skip unneeded offsets.
                            file = OffsetIOWrapper.from_offsets(
                                file, offsets, file_indices
                            )

                            # Decompress the wrapped file.
                            with GzipFile(
                                    mode="rb",
                                    fileobj=file,
                            ) as gzip_file:
                                yield gzip_file

                            file_index_offset += num_indices

                    elif compression == ClueWeb22Compression.ZIP:
                        assert compression_extension is not None
                        with ZipFile(file, "r") as zip_file:
                            names = zip_file.namelist()
                            num_names = len(names)

                            # Map global indices to ZIP names.
                            index_names = (
                                (file_index_offset + index, name)
                                for index, name in
                                enumerate(zip_file.namelist())
                            )

                            # Determine ZIP names to read.
                            names = (
                                name
                                for index, name in index_names
                                if self._in_slice(index, start, stop, step)
                            )

                            for name in names:
                                yield zip_file.open(name, "r")

                            file_index_offset += num_names
                    else:
                        raise ValueError(
                            f"Unknown compression format: {compression}"
                        )

        yield generator()

    def __getitem__(self, key: Union[int, slice]) -> Union[
        AnyDoc, Iterator[AnyDoc]
    ]:
        docs_count = self.docs.docs_count()
        full_slice = slice(0, docs_count)
        processed_slice: slice
        if isinstance(key, slice):
            processed_slice = apply_sub_slice(full_slice, key)
        elif isinstance(key, int):
            processed_slice = slice_idx(full_slice, key)
        else:
            raise TypeError("key must be int or slice")

        start, stop, step = processed_slice.indices(docs_count)

        @contextmanager
        def filter_files(format_type: ClueWeb22Format) -> Iterator[IO[bytes]]:
            with self._slice_files(format_type, start, stop, step) as files:
                yield files

        iterator = iter(_ClueWeb22Iterable(self.docs.subset, filter_files))
        if isinstance(key, slice):
            return iterator

        try:
            return next(iterator)
        except StopIteration:
            raise IndexError(
                (full_slice, slice(key, key + 1), processed_slice))


class ClueWeb22Docstore(Docstore):
    docs: Final[ClueWeb22Docs]

    def __init__(self, docs: ClueWeb22Docs):
        super().__init__(docs.docs_cls(), "doc_id")
        self.docs = docs

    def _file_paths(
            self,
            format_type: ClueWeb22Format,
            doc_ids: Iterable[_ClueWeb22DocId],
    ) -> Iterator[Tuple[Path, AbstractSet[_ClueWeb22DocId]]]:
        if self.docs.language is not None:
            invalid_doc_ids = {
                doc_id
                for doc_id in doc_ids
                if doc_id.language != self.docs.language.value.id
            }
            if len(invalid_doc_ids) > 0:
                raise ValueError(
                    f"The following document IDs don't match "
                    f"the dataset's language ({self.docs.language.value.id}): "
                    f"{invalid_doc_ids}"
                )

        format_path = self.docs.path / format_type.value.id

        file_path_to_doc_ids: Mapping[
            Path, MutableSet[_ClueWeb22DocId]
        ] = defaultdict(
            lambda: set()
        )
        for doc_id in doc_ids:
            file_path = format_path / f"{doc_id.path}{format_type.value.extension}"
            file_path_to_doc_ids[file_path].add(doc_id)

        return (
            (file_path, file_path_doc_ids)
            for file_path, file_path_doc_ids in file_path_to_doc_ids.items()
        )

    @contextmanager
    def _files(
            self,
            format_type: ClueWeb22Format,
            doc_ids: Iterable[_ClueWeb22DocId],
    ) -> Iterator[IO[bytes]]:
        compression = format_type.value.compression
        compression_extension = format_type.value.compression_extension

        def generator() -> Iterator[IO[bytes]]:
            file_paths = self._file_paths(format, doc_ids)
            for file_path, file_path_doc_ids in file_paths:
                with file_path.open("rb") as file:
                    if compression == ClueWeb22Compression.GZIP:
                        assert compression_extension is None

                        # Determine indices of documents within the file.
                        indices = {
                            doc_id.doc
                            for doc_id in file_path_doc_ids
                        }

                        # Read offsets:
                        offsets_name = (
                                file_path.name.removesuffix(
                                    format_type.value.extension
                                ) + format_type.value.offset_extension
                        )
                        offsets_file_path = file_path.with_name(offsets_name)
                        with offsets_file_path.open(
                                "rt", encoding=ENCODING) as offsets_file:
                            offsets = (
                                int(line)
                                for line in offsets_file
                            )
                            # Wrap file to skip unneeded offsets.
                            file = OffsetIOWrapper.from_offsets(
                                file, offsets, indices
                            )

                            # Decompress the wrapped file.
                            with GzipFile(
                                    mode="rb",
                                    fileobj=file,
                            ) as gzip_file:
                                yield gzip_file

                    elif compression == ClueWeb22Compression.ZIP:
                        assert compression_extension is not None
                        with ZipFile(file, "r") as zip_file:
                            names = {
                                f"{doc_id}{compression_extension}"
                                for doc_id in file_path_doc_ids
                            }
                            missing_names = names.difference(
                                zip_file.namelist()
                            )
                            if len(missing_names) > 0:
                                raise RuntimeError(
                                    f"Zip archive at {file_path} is missing "
                                    f"files: {missing_names}"
                                )

                            for name in names:
                                yield zip_file.open(name, "r")
                    else:
                        raise ValueError(
                            f"Unknown compression format: {compression}"
                        )

        yield generator()

    def get_many_iter(self, doc_ids: Iterable[str]) -> Iterator[AnyDoc]:
        doc_ids: AbstractSet[_ClueWeb22DocId] = {
            _ClueWeb22DocId(doc_id)
            for doc_id in doc_ids
        }

        @contextmanager
        def filter_files(format_type: ClueWeb22Format) -> Iterator[IO[bytes]]:
            with self._files(format_type, doc_ids) as files:
                yield files

        return iter(_ClueWeb22Iterable(self.docs.subset, filter_files))
