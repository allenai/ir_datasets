from . import lazy_libs
from . import log
from . import util
from . import formats
registry = util.Registry()
from . import datasets
from . import indices
from . import wrappers
from . import commands


def load(name):
    return registry[name]


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
