import sys
import argparse
import ir_datasets
from ir_datasets.commands.export import DEFAULT_EXPORTERS


_logger = ir_datasets.log.easy()


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
    parser = argparse.ArgumentParser(prog='ir_datasets list', description='Lists available datasets.')
    parser.set_defaults(out=sys.stdout)
    args = parser.parse_args(args)

    for dataset in sorted(ir_datasets.registry):
        args.out.write(f'{dataset}\n')


if __name__ == '__main__':
    main(sys.argv[1:])
