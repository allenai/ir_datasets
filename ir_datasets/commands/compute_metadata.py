import time
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
            brk = False
            for e in ir_datasets.ENTITY_TYPES:
                try:
                    if getattr(dataset, f'has_{e}')():
                        if e not in data[dsid]:
                            parent_id = getattr(ir_datasets, f'{e}_parent_id')(dsid)
                            if parent_id != dsid:
                                data[dsid][e] = {'_ref': parent_id}
                            else:
                                with _logger.duration(f'{dsid} {e}'):
                                    data[dsid][e] = getattr(dataset, f'{e}_calc_metadata')()
                            _logger.info(f'{dsid} {e}: {data[dsid][e]}')
                except Exception as ex:
                    _logger.info(f'{dsid} {e} [error]: {ex}')
                except KeyboardInterrupt:
                    _logger.info(f'KeyboardInterrupt; skipping. ctrl+c within 0.5sec to stop compute_metadata.')
                    try:
                        time.sleep(0.5)
                    except KeyboardInterrupt:
                        brk = True
                        break
            with args.file.open('wt') as f:
                # partially-formatted data; one dataset per line
                f.write('{\n')
                for i, key in enumerate(sorted(data.keys())):
                    if i != 0:
                        f.write(',\n')
                    f.write(f'  "{key}":{json.dumps(data[key])}')
                f.write('\n}\n')
            if brk:
                break


if __name__ == '__main__':
    main(sys.argv[1:])
