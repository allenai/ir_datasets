import ir_datasets

from ir_datasets.formats import TsvDocs
from ir_datasets.formats import TrecQrels
from ir_datasets.formats import TsvQueries
from ir_datasets.formats.tsv import _TsvBase
from ir_datasets.formats.base import BaseQueries
from ir_datasets.formats.base import GenericQuery

from .base import Dataset
from .base import YamlDocumentation

from ir_datasets.util import DownloadConfig

from typing import NamedTuple

NAME = "pt-image-ir-dataset"


class PtImageIrArticle(NamedTuple):
    doc_id: str
    url: str
    title: str
    text: str  # content field
    date: str
    images: str


class PtImageIrImage(NamedTuple):
    doc_id: str
    text: str  # url field


# Custom TsvQueries class that supports skipping the first line (header)
class TsvQueriesWithHeader(TsvQueries):
    def __init__(
        self,
        queries_dlc,
        query_cls=None,
        namespace=None,
        lang=None,
        skip_first_line=False,
    ):
        if query_cls is None:
            query_cls = GenericQuery
        # Call the _TsvBase constructor directly with skip_first_line
        _TsvBase.__init__(
            self, queries_dlc, query_cls, "queries", skip_first_line=skip_first_line
        )
        BaseQueries.__init__(self)
        self._queries_namespace = namespace
        self._queries_lang = lang


# What do the relevance levels in qrels mean?
QREL_DEFS = {
    1: "relevant - the image is relevant to the query",
    0: "not relevant - the image is not relevant to the query",
}

# This message is shown to the user before downloads are started
DUA = (
    "This work is licensed under the Creative Commons Attribution 4.0 International License. "
    "To view a copy of this license, visit https://creativecommons.org/licenses/by/4.0/. "
    "By using this dataset, you agree to the terms and conditions of this license."
)


def _init():
    # The directory where this dataset's data files will be stored
    base_path = ir_datasets.util.home_path() / NAME

    # Load an object that is used for providing the documentation
    documentation = YamlDocumentation(f"docs/{NAME}.yaml")

    # A reference to the downloads file, under the key "pt-image-ir". (DLC stands for DownLoadable Content)
    dlc = DownloadConfig.context(NAME, base_path, dua=DUA)

    # How to process the documents (articles). Since they are in a TSV format with 6 fields, we'll use TsvDocs with custom doc class.
    articles = TsvDocs(
        dlc["articles"],
        doc_cls=PtImageIrArticle,
        namespace=NAME,
        lang="pt",
        count_hint=4678,
        skip_first_line=True,  # Skip header row
    )

    # How to process the images. TSV format with 2 fields, using custom doc class.
    images = TsvDocs(
        dlc["images"],
        doc_cls=PtImageIrImage,
        namespace=f"{NAME}/images",
        lang="pt",
        count_hint=42333,
        skip_first_line=True,  # Skip header row
    )

    # How to process the queries. Using the custom class that can skip header.
    queries = TsvQueriesWithHeader(
        dlc["queries"], namespace=NAME, lang="pt", skip_first_line=True
    )

    # Qrels: The qrels file is in the TREC format, so we'll use TrecQrels to process them
    qrels = TrecQrels(dlc["qrels"], QREL_DEFS)

    # Package the docs, queries, qrels, and documentation into a Dataset object
    dataset = Dataset(articles, queries, qrels, documentation("_"))

    # Also create a dataset just for images
    images_dataset = Dataset(images, queries, qrels, documentation("images"))

    # Register the dataset in ir_datasets
    ir_datasets.registry.register(NAME, dataset)
    ir_datasets.registry.register(f"{NAME}/images", images_dataset)

    return dataset, images_dataset


dataset, images_dataset = _init()
