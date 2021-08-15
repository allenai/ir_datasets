import sys
import os
import json
import argparse
from pathlib import Path
from fnmatch import fnmatch
import ir_datasets
from ir_datasets.util import DownloadConfig


_logger = ir_datasets.log.easy()


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets compute_metadata', description='Computes metadata for the specified datasets')
    parser.add_argument('file', help='output file', type=Path)
    parser.add_argument('--datasets', nargs='+', help='dataset IDs for which to compute metadata (supports globs)')

    args = parser.parse_args(args)
    if args.file.is_file():
        with args.file.open('rb') as f:
            data = json.load(f)
    else:
        data = {}

    for dsid in ir_datasets.registry._registered:
        if not args.datasets or any(fnmatch(dsid, ds) for ds in args.datasets):
            dataset = ir_datasets.load(dsid)
            data.setdefault(dsid, {})
            for e in ['docs', 'queries', 'qrels', 'scoreddocs', 'docpairs']:
                try:
                    if getattr(dataset, f'has_{e}')():
                        if e not in data[dsid]:
                            parent_id = getattr(ir_datasets, f'{e}_parent_id')(dsid)
                            if parent_id != dsid:
                                data[dsid][e] = {'_ref': parent_id}
                            else:
                                with _logger.duration(f'{dsid} {e}'):
                                    data[dsid][e] = getattr(dataset, f'{e}_metadata')()
                            _logger.info(f'{dsid} {e}: {data[dsid][e]}')
                        else:
                            _logger.info(f'{dsid} {e} [cached]: {data[dsid][e]}')
                except Exception as ex:
                    _logger.info(f'{dsid} {e} [error]: {ex}')
            with args.file.open('wt') as f:
                json.dump(data, f)


if __name__ == '__main__':
    main(sys.argv[1:])
