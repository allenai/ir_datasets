from datetime import datetime
from enum import Enum
from typing import NamedTuple, Sequence, TypeVar, Optional, Type, Any


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


# Doc records corresponding to the fields available for each subset listed
# at https://lemurproject.org/clueweb22/obtain.php and in the SIRIP paper.


class LDoc(NamedTuple):
    doc_id: str
    url: str
    url_hash: str
    language: str
    text: str


class ADoc(NamedTuple):
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


class BDoc(NamedTuple):
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


AnyDoc = TypeVar("AnyDoc", LDoc, ADoc, BDoc)


# Type-safe configuration for the subsets, describing the characteristics
# of files and compression types.


class Compression(Enum):
    GZIP = 1
    ZIP = 2


class FormatInfo(NamedTuple):
    id: str
    """
    ClueWeb22 format as described 
    at https://lemurproject.org/clueweb22/docspecs.php#Organization
    """
    extension: str
    """
    File extension of a single compressed file.
    """
    compression: Compression
    """
    Compression form as described 
    at https://lemurproject.org/clueweb22/docspecs.php#Compression
    """
    compression_extension: Optional[str]
    """
    File extension of files within the compressed archive.
    """
    record_type: Type[Any]
    """
    Type of a record for one single document.
    """


class Format(Enum):
    value: FormatInfo

    HTML = FormatInfo(
        id="html",
        extension=".warc.gz",
        compression=Compression.GZIP,
        compression_extension=None,
        record_type=_Html,
    )
    INLINK = FormatInfo(
        id="inlink",
        extension=".json.gz",
        compression=Compression.GZIP,
        compression_extension=None,
        record_type=_Link,
    )
    OUTLINK = FormatInfo(
        id="outlink",
        extension=".json.gz",
        compression=Compression.GZIP,
        compression_extension=None,
        record_type=_Link,
    )
    TXT = FormatInfo(
        id="txt",
        extension=".json.gz",
        compression=Compression.GZIP,
        compression_extension=None,
        record_type=_Txt,
    )
    JPG = FormatInfo(
        id="jpg",
        extension=NotImplemented,
        compression=NotImplemented,
        compression_extension=NotImplemented,
        record_type=_Jpg,
    )
    VDOM = FormatInfo(
        id="vdom",
        extension=".zip",
        compression=Compression.ZIP,
        compression_extension=".bin",
        record_type=_Vdom,
    )


class LanguageInfo(NamedTuple):
    id: str
    """
    ClueWeb22 language ID as described 
    at https://lemurproject.org/clueweb22/docspecs.php#Organization
    """
    tag: str
    """
    Shorthand tag to be used as suffix in the dataset ID.
    """


class Language(Enum):
    value: LanguageInfo

    DE = LanguageInfo(id="de", tag="de")
    EN = LanguageInfo(id="en", tag="en")
    ES = LanguageInfo(id="es", tag="es")
    FR = LanguageInfo(id="fr", tag="fr")
    IT = LanguageInfo(id="it", tag="it")
    JA = LanguageInfo(id="ja", tag="ja")
    NL = LanguageInfo(id="nl", tag="nl")
    PO = LanguageInfo(id="po", tag="po")
    PT = LanguageInfo(id="pt", tag="pt")
    ZH = LanguageInfo(id="zh_chs", tag="zh")
    OTHER = LanguageInfo(id="other", tag="other-languages")


class SubsetInfo(NamedTuple):
    id: str
    """
    ClueWeb22 subset name as described 
    at https://lemurproject.org/clueweb22/index.php#Specs
    """
    tag: str
    """
    Shorthand to be used as suffix in the dataset ID.
    """
    formats: Sequence[Format]
    """
    Required formats for constructing a document for this subset.
    """
    doc_type: Type[AnyDoc]
    """
    Type of one single document.
    """


class Subset(Enum):
    value: SubsetInfo
    L = SubsetInfo(
        id="L",
        tag="l",
        formats=[Format.TXT],
        doc_type=LDoc
    ),
    A = SubsetInfo(
        id="A",
        tag="a",
        formats=[Format.TXT, Format.HTML, Format.INLINK, Format.OUTLINK,
                 Format.VDOM],
        doc_type=ADoc
    ),
    B = SubsetInfo(
        id="B",
        tag="b",
        formats=[Format.TXT, Format.HTML, Format.INLINK, Format.OUTLINK,
                 Format.VDOM, Format.JPG],
        doc_type=BDoc
    ),
