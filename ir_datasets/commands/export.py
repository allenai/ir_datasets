import sys
import json
import argparse
import ir_datasets


_logger = ir_datasets.log.easy()


def main_docs(dataset, args):
    assert hasattr(dataset, 'docs_handler'), f"{args.dataset} does not provide docs"
    exporter = DEFAULT_EXPORTERS[args.format]
    exporter = exporter(dataset.docs_cls(), args.out, args.fields)
    for doc in dataset.docs_iter():
        exporter.next(doc)


def main_queries(dataset, args):
    assert hasattr(dataset, 'queries_handler'), f"{args.dataset} does not provide queries"
    exporter = DEFAULT_EXPORTERS[args.format]
    exporter = exporter(dataset.queries_cls(), args.out, args.fields)
    for query in dataset.queries_iter():
        exporter.next(query)


def main_qrels(dataset, args):
    assert hasattr(dataset, 'qrels_handler'), f"{args.dataset} does not provide qrels"
    for qrel in dataset.qrels_iter():
        args.out.write(f'{qrel.query_id} {qrel.iteration} {qrel.doc_id} {qrel.relevance}\n')


def main_scoreddocs(dataset, args):
    assert hasattr(dataset, 'scoreddocs_handler'), f"{args.dataset} does not provide scoreddocs"
    query_id = None
    query_scores = []
    for scoreddoc in dataset.scoreddocs_iter():
        if scoreddoc.query_id != query_id:
            _scoreddocs_flush(args, query_scores)
            query_id, query_scores = scoreddoc.query_id, []
        query_scores.append(scoreddoc)
    _scoreddocs_flush(args, query_scores)


def _scoreddocs_flush(args, query_scores):
    for i, scoreddoc in enumerate(sorted(query_scores, key=lambda x: (-x.score, x.doc_id))):
        args.out.write(f'{scoreddoc.query_id} Q0 {scoreddoc.doc_id} {i} {scoreddoc.score} {args.runtag}\n')


class TsvExporter:
    def __init__(self, data_cls, out, fields=None):
        self.data_cls = data_cls
        self.out = out
        if fields is None:
            fields = data_cls._fields
            if len(fields) > 2:
                # This message is only really needed if there's more than 1 field
                _logger.info(f'No fields supplied. Exporting all fields: {fields}')
        self.idxs = []
        for field in fields:
            assert field in data_cls._fields
            self.idxs.append(data_cls._fields.index(field))

    def next(self, record):
        output = []
        for idx in self.idxs:
            output.append(str(record[idx]).replace('\t', ' ').replace('\n', ' ').replace('\r', ' '))
        self.out.write('\t'.join(output) + '\n')


class JsonlExporter:
    def __init__(self, data_cls, out, fields=None):
        self.data_cls = data_cls
        self.out = out
        self.fields = fields or data_cls._fields
        self.idxs = []
        for field in self.fields:
            assert field in data_cls._fields
            self.idxs.append(data_cls._fields.index(field))

    def next(self, record):
        json.dump({self.fields[i]: record[i] for i in self.idxs}, self.out)
        self.out.write('\n')



DEFAULT_EXPORTERS = {
    'tsv': TsvExporter,
    'jsonl': JsonlExporter,
}


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets export', description='Exports documents, queries, qrels, and scoreddocs in various formats.')
    parser.add_argument('dataset')
    parser.set_defaults(out=sys.stdout)
    subparsers = parser.add_subparsers(dest='data')
    subparsers.required = True

    subparser = subparsers.add_parser('docs')
    subparser.add_argument('--format', choices=DEFAULT_EXPORTERS.keys(), default='tsv')
    subparser.add_argument('--fields', nargs='+')
    subparser.set_defaults(fn=main_docs)

    subparser = subparsers.add_parser('queries')
    subparser.add_argument('--format', choices=DEFAULT_EXPORTERS.keys(), default='tsv')
    subparser.add_argument('--fields', nargs='+')
    subparser.set_defaults(fn=main_queries)

    subparser = subparsers.add_parser('qrels')
    subparser.add_argument('--format', choices=['trec'], default='trec')
    subparser.set_defaults(fn=main_qrels)

    subparser = subparsers.add_parser('scoreddocs')
    subparser.add_argument('--format', choices=['trec'], default='trec')
    subparser.add_argument('--runtag', default='run')
    subparser.set_defaults(fn=main_scoreddocs)

    args = parser.parse_args(args)
    dataset = ir_datasets.load(args.dataset)
    try:
        dataset = ir_datasets.load(args.dataset)
    except KeyError:
        sys.stderr.write(f"Dataset {args.dataset} not found.\n")
        sys.exit(1)
    try:
        args.fn(dataset, args)
    except BrokenPipeError:
        sys.stderr.close()
    except KeyboardInterrupt:
        sys.stderr.close()
    except AssertionError as e:
        if str(e):
            sys.stderr.write(str(e) + '\n')
        else:
            raise


if __name__ == '__main__':
    main(sys.argv[1:])
