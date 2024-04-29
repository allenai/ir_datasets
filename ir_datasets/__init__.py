from enum import Enum
class EntityType(Enum):
    docs = "docs"
    queries = "queries"
    qrels = "qrels"
    scoreddocs = "scoreddocs"
    docpairs = "docpairs"
    qlogs = "qlogs"
from . import lazy_libs
from . import log
from . import util
from . import formats
registry = util.Registry()
from . import datasets
from . import indices
from . import wrappers
from . import commands

Dataset = datasets.base.Dataset

def load(name):
    return registry[name]


def parent_id(dataset_id: str, entity_type: EntityType) -> str:
    """
    Maps a dataset_id to a more general ID that shares the same entity handler (e.g., docs_handler). For example,
    for docs, "msmarco-document/trec-dl-2019/judged" -> "msmarco-document" or "wikir/en1k/test" -> "wikir/en1k".
    This is useful when creating shared document resources among multiple subsets, such as an index.

    Note: At this time, this function operates by convention; it finds the lowest dataset_id in the
    hierarchy that has the same docs_handler instance. This function may be updated in the future to
    also use explicit links added when datasets are registered.
    """
    entity_type = EntityType(entity_type) # validate & allow strings
    ds = load(dataset_id)
    segments = dataset_id.split("/")
    handler = getattr(ds, f'{entity_type.value}_handler')()
    parent_ds_id = dataset_id
    while len(segments) > 1:
        segments.pop()
        try:
            parent_ds = load("/".join(segments))
            if parent_ds.has(entity_type.value) and getattr(parent_ds, f'{entity_type.value}_handler')() == handler:
                parent_ds_id = "/".join(segments)
        except KeyError:
            pass # this dataset doesn't exist
    return parent_ds_id


def docs_parent_id(dataset_id: str) -> str:
    return parent_id(dataset_id, EntityType.docs)
corpus_id = docs_parent_id # legacy


def queries_parent_id(dataset_id: str) -> str:
    return parent_id(dataset_id, EntityType.queries)


def qrels_parent_id(dataset_id: str) -> str:
    return parent_id(dataset_id, EntityType.qrels)


def scoreddocs_parent_id(dataset_id: str) -> str:
    return parent_id(dataset_id, EntityType.scoreddocs)


def docpairs_parent_id(dataset_id: str) -> str:
    return parent_id(dataset_id, EntityType.docpairs)


def qlogs_parent_id(dataset_id: str) -> str:
    return parent_id(dataset_id, EntityType.qlogs)


def create_dataset(docs_tsv=None, queries_tsv=None, qrels_trec=None):
    LocalDownload = util.LocalDownload
    TsvDocs = formats.TsvDocs
    TsvQueries = formats.TsvQueries
    TrecQrels = formats.TrecQrels
    components = []
    if docs_tsv is not None:
        components.append(TsvDocs(LocalDownload(docs_tsv)))
    if queries_tsv is not None:
        components.append(TsvQueries(LocalDownload(queries_tsv)))
    if qrels_trec is not None:
        components.append(TrecQrels(LocalDownload(qrels_trec), {}))
    return datasets.base.Dataset(*components)


def main(args):
    import sys
    if len(args) < 1 or args[0] not in commands.COMMANDS:
        cmds = ','.join(commands.COMMANDS.keys())
        sys.stderr.write(f'Usage: ir_datasets {{{cmds}}} ...\n')
        sys.exit(1)
    commands.COMMANDS[args[0]](args[1:])


def main_cli():
    import sys
    main(sys.argv[1:])

__version__ = "0.5.7" # NOTE: keep this in sync with setup.py
