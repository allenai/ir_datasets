from datetime import datetime
from typing import NamedTuple, Sequence, TypeVar


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
