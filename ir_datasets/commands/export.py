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
    exporter.flush()


def main_queries(dataset, args):
    assert hasattr(dataset, 'queries_handler'), f"{args.dataset} does not provide queries"
    exporter = DEFAULT_EXPORTERS[args.format]
    exporter = exporter(dataset.queries_cls(), args.out, args.fields)
    for query in dataset.queries_iter():
        exporter.next(query)
    exporter.flush()


def main_qrels(dataset, args):
    assert hasattr(dataset, 'qrels_handler'), f"{args.dataset} does not provide qrels"
    exporter = QRELS_EXPORTERS[args.format]
    exporter = exporter(dataset.qrels_cls(), args.out, args.fields)
    for qrel in dataset.qrels_iter():
        exporter.next(qrel)
    exporter.flush()


def main_scoreddocs(dataset, args):
    assert hasattr(dataset, 'scoreddocs_handler'), f"{args.dataset} does not provide scoreddocs"
    exporter = SCOREDDOCS_EXPORTERS[args.format]
    exporter = exporter(dataset.scoreddocs_cls(), args.out, args.fields)
    if hasattr(exporter, 'runtag'):
        exporter.runtag = args.runtag
    for scoreddoc in dataset.scoreddocs_iter():
        exporter.next(scoreddoc)
    exporter.flush()



class TsvExporter:
    def __init__(self, data_cls, out, fields=None):
        self.data_cls = data_cls
        self.out = out
        if fields is None:
            fields = data_cls._fields
            if len(fields) > 2:
                # This message is only really needed if there's more than 2 fields
                _logger.info(f'No fields supplied. Using all fields: {fields}')
        field_conflicts = [f for f in fields if data_cls.__annotations__[f] not in (str, int, float)]
        if len(field_conflicts) == 1:
            # special case: if there's only one Tuple[X, ...], we can export unambiguously with variable number of columns
            if is_tuple_elip(data_cls.__annotations__[field_conflicts[0]]):
                _logger.info(f'Exporting variable number of columns for {field_conflicts[0]}')
                field_conflicts = []
        if len(field_conflicts) > 0:
            fields = [f for f in fields if f not in field_conflicts]
            field_conflicts = ', '.join([repr((f, data_cls.__annotations__[f])) for f in field_conflicts])
            _logger.info(f'Skipping the following fields due to unsupported data types: {field_conflicts}')
        self.idxs = []
        for field in fields:
            assert field in data_cls._fields
            self.idxs.append(data_cls._fields.index(field))

    def next(self, record):
        output = []
        for idx in self.idxs:
            if isinstance(record[idx], (list, tuple)):
                for sub_rec in record[idx]:
                    if hasattr(sub_rec, '_fields'):
                        for value in sub_rec:
                            output.append(str(value).replace('\t', ' ').replace('\n', ' ').replace('\r', ' '))
                    else:
                        output.append(str(sub_rec).replace('\t', ' ').replace('\n', ' ').replace('\r', ' '))
            elif hasattr(record[idx], '_fields'):
                for value in record[idx]:
                    output.append(str(value).replace('\t', ' ').replace('\n', ' ').replace('\r', ' '))
            else:
                output.append(str(record[idx]).replace('\t', ' ').replace('\n', ' ').replace('\r', ' '))
        self.out.write('\t'.join(output) + '\n')

    def flush(self):
        pass


class JsonlExporter:
    def __init__(self, data_cls, out, fields=None):
        self.data_cls = data_cls
        self.out = out
        fields = fields or data_cls._fields
        if fields is None:
            fields = data_cls._fields
            if len(fields) > 2:
                # This message is only really needed if there's more than 2 fields
                _logger.info(f'No fields supplied. Using all fields: {fields}')
        field_conflicts = [f for f in fields if data_cls.__annotations__[f] not in (str, int, float) and not is_tuple_elip(data_cls.__annotations__[f])]
        if len(field_conflicts) > 0:
            fields = [f for f in fields if f not in field_conflicts]
            field_conflicts = ', '.join([repr((f, data_cls.__annotations__[f])) for f in field_conflicts])
            _logger.info(f'Skipping the following fields due to unsupported data types: {field_conflicts}')
        self.fields = fields
        self.idxs = []
        for field in self.fields:
            assert field in data_cls._fields
            self.idxs.append(data_cls._fields.index(field))

    def next(self, record):
        json.dump({f: self.encode(record[i]) for f, i in zip(self.fields, self.idxs)}, self.out)
        self.out.write('\n')

    def encode(self, value):
        if isinstance(value, (list, tuple)):
            return [self.encode(v) for v in value]
        if hasattr(value, '_fields'):
            return {k: self.encode(v) for k, v in value._asdict()}
        return value

    def flush(self):
        pass

def is_tuple_elip(annotation):
    if hasattr(annotation, '_name') and annotation._name == 'Tuple' and len(annotation.__args__) == 2 and annotation.__args__[1] is Ellipsis:
        if annotation.__args__[0] in (str, int, float) or (hasattr(annotation.__args__[0], '_fields') and all(f in (str, int, float) for f in annotation.__args__[0].__annotations__.values())):
            return True
    return False


class TrecQrelsExporter:
    def __init__(self, data_cls, out, fields=None):
        self.data_cls = data_cls
        self.out = out
        assert 'query_id' in data_cls._fields, f"unsupported dataset cls {data_cls} (missing query_id)"
        assert 'doc_id' in data_cls._fields, f"unsupported dataset cls {data_cls} (missing doc_id)"
        self.has_iteration = 'iteration' in data_cls._fields
        if fields is None:
            remaining_fields = set(data_cls._fields) - {'query_id', 'doc_id', 'iteration'}
            fields = sorted(remaining_fields, key=lambda f: data_cls._fields.index(f))
            if fields != ['relevance']:
                _logger.info(f'exporting fields {fields}')
        self.rel_field_idxs = []
        for field in fields:
            assert field in data_cls._fields, f"missing field {repr(field)}; choose --fields from {data_cls._fields}"
            self.rel_field_idxs.append(data_cls._fields.index(field))
        if len(self.rel_field_idxs) > 1:
            _logger.info(f'exporting multiple relevance fields; may not work with some evaluation scripts. Specify fields with --fields')

    def next(self, record):
        rel_fields = ' '.join(str(record[i]) for i in self.rel_field_idxs)
        self.out.write(f'{record.query_id} {record.iteration if self.has_iteration else "0"} {record.doc_id} {rel_fields}\n')

    def flush(self):
        pass


class TrecRunExporter:
    def __init__(self, data_cls, out, fields=None):
        self.data_cls = data_cls
        self.out = out
        assert fields is None, "fields not supported for TREC Run exporter"
        self.query_id = None
        self.query_scores = []
        self.runtag = 'run'

    def next(self, record):
        if record.query_id != self.query_id:
            self.flush()
            query_id = record.query_id
        self.query_scores.append(record)

    def flush(self):
        for i, scoreddoc in enumerate(sorted(self.query_scores, key=lambda x: (-x.score, x.doc_id))):
            self.out.write(f'{scoreddoc.query_id} Q0 {scoreddoc.doc_id} {i} {scoreddoc.score} {self.runtag}\n')
        self.query_scores = []


DEFAULT_EXPORTERS = {
    'tsv': TsvExporter,
    'jsonl': JsonlExporter,
}

QRELS_EXPORTERS = {**DEFAULT_EXPORTERS, 'trec': TrecQrelsExporter}

SCOREDDOCS_EXPORTERS = {**DEFAULT_EXPORTERS, 'trec': TrecRunExporter}


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
    subparser.add_argument('--format', choices=QRELS_EXPORTERS.keys(), default='trec')
    subparser.add_argument('--fields', nargs='+')
    subparser.set_defaults(fn=main_qrels)

    subparser = subparsers.add_parser('scoreddocs')
    subparser.add_argument('--format', choices=SCOREDDOCS_EXPORTERS.keys(), default='trec')
    subparser.add_argument('--fields', nargs='+')
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
