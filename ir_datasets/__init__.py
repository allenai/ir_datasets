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


def corpus_id(dataset_id: str) -> str:
    """
    Maps a dataset_id to a more general ID that shares the same corpus (i.e., docs_handler). For example,
    "msmarco-document/trec-dl-2019/judged" -> "msmarco-document" or "wikir/en1k/test" -> "wikir/en1k".
    This is useful when creating shared document resources among multiple subsets, such as
    an index.

    Note: At this time, this function operates by convention; it finds the lowest dataset_id in the
    hierarchy that has the same docs_handler instance. This function may be updated in the future to
    also use explicit links added when datasets are registered.
    """
    # adapted from https://github.com/Georgetown-IR-Lab/OpenNIR/blob/master/onir/datasets/irds.py#L47
    ds = load(dataset_id)
    segments = dataset_id.split("/")
    docs_handler = ds.docs_handler()
    parent_corpus_ds = dataset_id
    while len(segments) > 1:
        segments.pop()
        try:
            parent_ds = load("/".join(segments))
            if parent_ds.has_docs() and parent_ds.docs_handler() == docs_handler:
                parent_corpus_ds = "/".join(segments)
        except KeyError:
            pass # this dataset doesn't exist
    return parent_corpus_ds


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

__version__ = "0.4.2" # NOTE: keep this in sync with setup.py
