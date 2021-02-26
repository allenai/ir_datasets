import sys
import os
import select
import tempfile
import contextlib
import json
import argparse
import multiprocessing
from collections import deque
import ir_datasets


_logger = ir_datasets.log.easy()


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets doc_fifos', description='Starts a process that exports documents in parallel to several named pipes as json. This is useful as inputs to indexers like Anserini.')
    parser.add_argument('dataset')
    parser.add_argument('--count', type=int, default=max(multiprocessing.cpu_count() - 1, 1))
    parser.add_argument('--fields', nargs='+')
    parser.add_argument('--dir')

    args = parser.parse_args(args)
    dataset = ir_datasets.load(args.dataset)
    try:
        dataset = ir_datasets.load(args.dataset)
    except KeyError:
        sys.stderr.write(f"Dataset {args.dataset} not found.\n")
        sys.exit(1)

    if not dataset.has_docs():
        sys.stderr.write(f"Dataset {args.dataset} does not have docs.\n")
        sys.exit(1)

    docs_cls = dataset.docs_cls()
    field_idxs = []
    if args.fields:
        for field in args.fields:
            if field not in docs_cls._fields:
                sys.stderr.write(f"Field {field} not found ind {args.dataset}. Available fields: {docs_cls._fields}\n")
                sys.exit(1)
            field_idxs.append(docs_cls._fields.index(field))
    else:
        if len(docs_cls._fields) == 2:
            # there's only one field, silently use it
            field_idxs.append(1)
        else:
            # more than 1 field, let the user know everything is used.
            sys.stderr.write(f"Exporting all fields as document content: {docs_cls._fields[1:]}. Use --fields to specify fields.\n")
            field_idxs = list(range(1, len(docs_cls._fields)))

    with contextlib.ExitStack() as stack:
        if args.dir is not None:
            d = args.dir
        else:
            d = stack.enter_context(tempfile.TemporaryDirectory())
        fifos = []
        for i in range(args.count):
            fifo = os.path.join(d, f'{i}.json')
            os.mkfifo(fifo)
            fifos.append(fifo)

        docs_iter = dataset.docs_iter()
        docs_iter = _logger.pbar(docs_iter, total=dataset.docs_count())

        print(f'Ready at {d}')
        print(f'To index with Anserini, run:\nIndexCollection -collection JsonCollection -input {d} -threads {args.count} -index <your_index_path> <other_anserini_args>')

        fifos = [stack.enter_context(open(f, 'wt')) for f in fifos]

        ready = None
        for doc in docs_iter:
            if not ready: # first or no more ready
                _, ready, _ = select.select([], fifos, [])
                ready = deque(ready)
            fifo = ready.popleft()
            doc = {'id': doc.doc_id, 'contents': '\n'.join(str(doc[i]) for i in field_idxs)}
            json.dump(doc, fifo)
            fifo.write('\n')


if __name__ == '__main__':
    main(sys.argv[1:])
