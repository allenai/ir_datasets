import sys
import argparse
import ir_datasets
from ir_datasets.commands.export import DEFAULT_EXPORTERS


_logger = ir_datasets.log.easy()


def qid_lookup(dataset, args):
    assert hasattr(dataset, 'queries_handler')
    exporter = DEFAULT_EXPORTERS[args.format]
    exporter = exporter(dataset.queries_cls(), args.out, args.fields)
    dataset = ir_datasets.wrappers.DocstoreWrapper(dataset)
    store = dataset.queries_store()
    for qid in args.ids:
        try:
            query = store.get(qid)
            exporter.next(query)
        except KeyError:
            _logger.warn(f'query_id {qid} not found')


def did_lookup(dataset, args):
    assert hasattr(dataset, 'docs_handler')
    exporter = DEFAULT_EXPORTERS[args.format]
    exporter = exporter(dataset.docs_cls(), args.out, args.fields)
    dataset = ir_datasets.wrappers.DocstoreWrapper(dataset)
    store = dataset.docs_store()
    for did in args.ids:
        try:
            doc = store.get(did)
            exporter.next(doc)
        except KeyError:
            _logger.warn(f'doc_id {did} not found')


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets lookup', description='Provides fast lookups of documents and queries '
        'using DocstoreWrapper. Unlike using the exporter and grep (or similar), this tool builds '
        'an index for O(log(n)) lookups.')
    parser.add_argument('dataset')
    parser.set_defaults(out=sys.stdout)
    parser.add_argument('--format', choices=DEFAULT_EXPORTERS.keys(), default='tsv')
    parser.add_argument('--fields', nargs='+')
    parser.add_argument('--qid', '--query_id', '-q', action='store_true')
    parser.add_argument('ids', nargs='+')

    args = parser.parse_args(args)
    try:
        dataset = ir_datasets.load(args.dataset)
    except KeyError:
        sys.stderr.write(f"Dataset {args.dataset} not found.\n")
        sys.exit(1)
    if args.qid:
        qid_lookup(dataset, args)
    else:
        did_lookup(dataset, args)


if __name__ == '__main__':
    main(sys.argv[1:])
