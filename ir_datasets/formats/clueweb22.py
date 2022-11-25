from collections import defaultdict
from contextlib import ExitStack, contextmanager
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
    NamedTuple, Sequence, TypeVar, Optional, Type, Any, Final, Iterator, IO,
    TYPE_CHECKING, Iterable, Callable, ContextManager, Mapping, Union,
    AbstractSet, MutableSet, Tuple
)
from zipfile import ZipFile

from ir_datasets.formats import BaseDocs
from ir_datasets.indices import Docstore
from ir_datasets.lazy_libs import warc
from ir_datasets.util import Download
from ir_datasets.util.io import ConcatIOWrapper, OffsetIOWrapper

# Constants and constraints.


MAX_SUBDIRECTORIES_PER_STREAM: Final[int] = 80
MAX_FILES_PER_SUBDIRECTORY: Final[int] = 100
OFFSETS_FILE_EXTENSION: Final[str] = ".offset"
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
    vdom_heading: Sequence[int]
    vdom_list: Sequence[int]
    vdom_passage: Sequence[int]
    vdom_primary: Sequence[int]
    vdom_table: Sequence[int]
    vdom_title: Sequence[int]
    vdom_paragraph: Sequence[int]


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
    doc_id: str
    url: str
    url_hash: str
    # TODO how to parse?


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
    with ConcatIOWrapper.from_iterable(files) as file:
        with TextIOWrapper(file, encoding=ENCODING) as text_file:
            for line in text_file:
                json = loads(line)
                yield _Txt(
                    doc_id=json["ClueWeb22-ID"],
                    url=json["URL"],
                    url_hash=json["URL-hash"],
                    language=json["Language"],
                    text=json["Clean-Text"],
                )


# Only import the heavy warc library for type checking.
if TYPE_CHECKING:
    # noinspection PyPackageRequirements (listed as warc3-wet)
    from warc import WARCRecord
else:
    WARCRecord = Any


def _parse_vdom_list(document: WARCRecord, key: str) -> Sequence[int]:
    vdom_list = document.header.get(key, "").split()
    return [int(vdom) for vdom in vdom_list]


def _read_html(files: Iterator[IO[bytes]]) -> Iterator[_Html]:
    # Only import the heavy warc library for type checking,
    # otherwise load the library lazily.
    if TYPE_CHECKING:
        # noinspection PyPackageRequirements (listed as warc3-wet)
        from warc import WARCFile
    else:
        # noinspection PyPep8Naming (due to export from lazy lib)
        WARCFile = warc().WARCFile

    with ConcatIOWrapper.from_iterable(files) as file:
        with WARCFile(fileobj=file) as warc_file:
            documents: Iterable[WARCRecord] = warc_file
            documents = (
                document for document in documents
                if document.type == "response"
            )
            for document in documents:
                doc_id = document["ClueWeb22-ID"]
                url = document.url
                url_hash = document["URL-Hash"]
                language = document["Language"]
                date = datetime.fromisoformat(document.date)
                vdom_heading = _parse_vdom_list(document, "VDOM-Heading")
                vdom_list = _parse_vdom_list(document, "VDOM-List")
                vdom_passage = _parse_vdom_list(document, "VDOM-Passage")
                vdom_primary = _parse_vdom_list(document, "VDOM-Primary")
                vdom_table = _parse_vdom_list(document, "VDOM-Table")
                vdom_title = _parse_vdom_list(document, "VDOM-Title")
                vdom_paragraph = _parse_vdom_list(document, "VDOM-Paragraph")
                html: bytes = document.payload.read()
                yield _Html(
                    doc_id=doc_id,
                    url=url,
                    url_hash=url_hash,
                    language=language,
                    date=date,
                    html=html,
                    vdom_heading=vdom_heading,
                    vdom_list=vdom_list,
                    vdom_passage=vdom_passage,
                    vdom_primary=vdom_primary,
                    vdom_table=vdom_table,
                    vdom_title=vdom_title,
                    vdom_paragraph=vdom_paragraph,
                )


def _parse_anchor(json: Sequence[str]) -> Anchor:
    return Anchor(
        url=json[0],
        url_hash=json[1],
        text=json[2],
        language=json[4],
    )


def _read_link(files: Iterator[IO[bytes]]) -> Iterator[_Link]:
    with ConcatIOWrapper.from_iterable(files) as file:
        with TextIOWrapper(file, encoding=ENCODING) as text_file:
            for line in text_file:
                json = loads(line)
                yield _Link(
                    doc_id=json["ClueWeb22-ID"],
                    url=json["url"],
                    url_hash=json["urlhash"],
                    anchors=[
                        _parse_anchor(anchor) for anchor in json["anchors"]
                    ],
                )


def _read_vdom(files: Iterator[IO[bytes]]) -> Iterator[_Vdom]:
    raise NotImplementedError()


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
    vdom_heading: Sequence[int]
    vdom_list: Sequence[int]
    vdom_passage: Sequence[int]
    vdom_primary: Sequence[int]
    vdom_table: Sequence[int]
    vdom_title: Sequence[int]
    vdom_paragraph: Sequence[int]
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
    vdom_heading: Sequence[int]
    vdom_list: Sequence[int]
    vdom_passage: Sequence[int]
    vdom_primary: Sequence[int]
    vdom_table: Sequence[int]
    vdom_title: Sequence[int]
    vdom_paragraph: Sequence[int]
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
        inlink_iterator: Iterator[_Link],
        outlink_iterator: Iterator[_Link],
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
        assert txt.doc_id == html.doc_id == inlink.doc_id == \
               outlink.doc_id == vdom.doc_id
        assert txt.url == html.url == inlink.url == outlink.url == vdom.url
        assert txt.url_hash == html.url_hash == inlink.url_hash == \
               outlink.url_hash == vdom.url_hash
        assert txt.language == html.language
        yield ClueWeb22ADoc(
            doc_id=txt.doc_id,
            url=txt.url,
            url_hash=txt.url_hash,
            language=txt.language,
            text=txt.text,
            date=html.date,
            html=html.html,
            vdom_heading=html.vdom_heading,
            vdom_list=html.vdom_list,
            vdom_passage=html.vdom_passage,
            vdom_primary=html.vdom_primary,
            vdom_table=html.vdom_table,
            vdom_title=html.vdom_title,
            vdom_paragraph=html.vdom_paragraph,
            inlink_anchors=inlink.anchors,
            outlink_anchors=outlink.anchors,
        )


def _combine_b_docs(
        txt_iterator: Iterator[_Txt],
        html_iterator: Iterator[_Html],
        inlink_iterator: Iterator[_Link],
        outlink_iterator: Iterator[_Link],
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
        yield ClueWeb22BDoc(
            doc_id=txt.doc_id,
            url=txt.url,
            url_hash=txt.url_hash,
            language=txt.language,
            text=txt.text,
            date=html.date,
            html=html.html,
            vdom_heading=html.vdom_heading,
            vdom_list=html.vdom_list,
            vdom_passage=html.vdom_passage,
            vdom_primary=html.vdom_primary,
            vdom_table=html.vdom_table,
            vdom_title=html.vdom_title,
            vdom_paragraph=html.vdom_paragraph,
            inlink_anchors=inlink.anchors,
            outlink_anchors=outlink.anchors,
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
        compression=ClueWeb22Compression.GZIP,
        compression_extension=None,
        reader=_read_html,
    )
    INLINK = _FormatInfo(
        id="inlink",
        extension=".json.gz",
        compression=ClueWeb22Compression.GZIP,
        compression_extension=None,
        reader=_read_link,
    )
    OUTLINK = _FormatInfo(
        id="outlink",
        extension=".json.gz",
        compression=ClueWeb22Compression.GZIP,
        compression_extension=None,
        reader=_read_link,
    )
    TXT = _FormatInfo(
        id="txt",
        extension=".json.gz",
        compression=ClueWeb22Compression.GZIP,
        compression_extension=None,
        reader=_read_txt,
    )
    JPG = _FormatInfo(
        id="jpg",
        extension=NotImplemented,
        compression=NotImplemented,
        compression_extension=NotImplemented,
        reader=_read_jpg,
    )
    VDOM = _FormatInfo(
        id="vdom",
        extension=".zip",
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


class ClueWeb22Subset(Enum):
    value: _SubsetInfo

    L = _SubsetInfo(
        id="L",
        tag="l",
        formats=[ClueWeb22Format.TXT],
        doc_type=ClueWeb22LDoc,
        combiner=_combine_l_docs
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
        combiner=_combine_a_docs
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
        combiner=_combine_b_docs
    )


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


# Iterator classes for accessing multiple documents.


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
            format_files: Sequence[Iterator[IO[bytes]]] = [
                stack.enter_context(self.files(format))
                for format in formats
            ]
            format_records: Sequence[Iterator[_AnyRecord]] = [
                format.value.reader(files)
                for format, files in zip(formats, format_files)
            ]
            documents = self.subset.value.combiner(*format_records)
            yield from documents


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
    def version(self) -> int:
        version_files = list(self.path.glob("version_*"))
        assert len(version_files) == 1
        return int(version_files[0].name.split("_")[1])

    @cached_property
    def _checksums(self) -> NotImplemented:
        checksums_dir = self.path / "checksums"
        raise NotImplementedError()

    @cached_property
    def _record_counts(self) -> Mapping[Path, int]:
        record_counts_dir = self.path / "recordcounts"
        result = {}
        for counts_file in record_counts_dir.glob("*_counts.txt"):
            dir_name = counts_file.name[:-len("_counts.txt")]
            dir_path = self.path / dir_name
            with open(counts_file, "rt", encoding=ENCODING) as file:
                for line in file:
                    file, count = line.strip().split()
                    path = dir_path / file[2:]
                    result[path] = int(count)
        return result

    def _file_paths(self, format: ClueWeb22Format) -> Sequence[Path]:
        languages: Iterable[ClueWeb22Language]
        if self.language is not None:
            languages = {self.language}
        else:
            languages = {language for language in ClueWeb22Language}
        language_ids = {language.value.id for language in languages}
        return sorted(
            chain.from_iterable(
                self.path.glob(join(
                    format.value.id,
                    language_id,
                    f"{language_id}[0-9][0-9]",
                    f"{language_id}[0-9][0-9][0-9][0-9]",
                    f"{language_id}[0-9][0-9][0-9][0-9]-[0-9][0-9]"
                    f".{format.value.extension}",
                ))
                for language_id in language_ids
            )
        )

    @contextmanager
    def _files(self, format: ClueWeb22Format) -> Iterator[IO[bytes]]:
        def generator() -> Iterator[IO[bytes]]:
            file_paths = self._file_paths(format)
            for file_path in file_paths:
                with file_path.open("rb") as file:
                    compression = format.value.compression
                    compression_extension = format.value.compression_extension
                    if compression == ClueWeb22Compression.GZIP:
                        assert compression_extension is None
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

    def docs_iter(self) -> Iterator[AnyDoc]:
        return iter(_ClueWeb22Iterable(self.subset, self._files))

    def docs_store(self) -> "ClueWeb22Docstore":
        return ClueWeb22Docstore(self)

    def docs_cls(self) -> Type[AnyDoc]:
        return self.subset.value.doc_type

    def docs_count(self) -> int:
        return sum(self._record_counts.values())

    def docs_namespace(self) -> str:
        names = [self.name, self.subset.value.tag]
        if self.language is not None:
            names.append(self.language.value.tag)
        return "/".join(names)

    def docs_lang(self) -> Optional[str]:
        if self.language is None:
            return None
        return self.language.value.tag


class ClueWeb22Docstore(Docstore):
    docs: Final[ClueWeb22Docs]

    def __init__(self, docs: ClueWeb22Docs):
        super().__init__(docs.docs_cls(), "doc_id")
        self.docs = docs

    def _file_paths(
            self,
            format: ClueWeb22Format,
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

        format_path = self.docs.path / format.value.id

        file_path_to_doc_ids: Mapping[
            Path, MutableSet[_ClueWeb22DocId]
        ] = defaultdict(
            lambda: set()
        )
        for doc_id in doc_ids:
            file_path = format_path / f"{doc_id.path}{format.value.extension}"
            file_path_to_doc_ids[file_path].add(doc_id)

        return (
            (file_path, file_path_doc_ids)
            for file_path, file_path_doc_ids in file_path_to_doc_ids.items()
        )

    @contextmanager
    def _files(
            self,
            format: ClueWeb22Format,
            doc_ids: Iterable[_ClueWeb22DocId],
    ) -> Iterator[IO[bytes]]:
        def generator() -> Iterator[IO[bytes]]:
            file_paths = self._file_paths(format, doc_ids)
            for file_path, file_path_doc_ids in file_paths:
                with file_path.open("rb") as file:
                    compression = format.value.compression
                    compression_extension = format.value.compression_extension
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
                                    format.value.extension
                                ) + OFFSETS_FILE_EXTENSION
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

        def files(format: ClueWeb22Format) -> Iterator[IO[bytes]]:
            return self._files(format, doc_ids)

        return iter(_ClueWeb22Iterable(self.docs.subset, self._files))
