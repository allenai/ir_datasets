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


def dataset2metadata(args):
    dsid, data = args
    try:
        dataset = ir_datasets.load(dsid)
    except KeyError:
        return dsid, None
    try:
        for e in ir_datasets.EntityType:
            if dataset.has(e):
                if e.value not in data:
                    parent_id = getattr(ir_datasets, f'{e.value}_parent_id')(dsid)
                    if parent_id != dsid:
                        data[e.value] = {'_ref': parent_id}
                    else:
                        with _logger.duration(f'{dsid} {e.value}'):
                            data[e.value] = getattr(dataset, f'{e.value}_calc_metadata')()
                    _logger.info(f'{dsid} {e.value}: {data[e.value]}')
    except Exception as ex:
        _logger.info(f'{dsid} {e.value} [error]: {ex}')
        return dsid, None
    return dsid, data


def write_metadata_file(data, file):
    with file.open('wt') as f:
        # partially-formatted data; one dataset per line
        f.write('{\n')
        for i, key in enumerate(sorted(data.keys())):
            if i != 0:
                f.write(',\n')
            f.write(f'  "{key}": {json.dumps(data[key])}')
        f.write('\n}\n')


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets generate_metadata', description='Generates metadata for the specified datasets')
    parser.add_argument('--file', help='output file', type=Path, default=Path('ir_datasets/etc/metadata.json'))
    parser.add_argument('--datasets', nargs='+', help='dataset IDs for which to compute metadata. If omitted, generates for all datasets present in the registry (skipping patterns)')

    args = parser.parse_args(args)
    if args.file.is_file():
        with args.file.open('rb') as f:
            data = json.load(f)
    else:
        data = {}

    if args.datasets:
        def _ds_iter():
            for dsid in args.datasets:
                yield dsid, data.get(dsid, {})
        import multiprocessing
        with multiprocessing.Pool(10) as pool:
            for dsid, dataset_metadata in _logger.pbar(pool.imap_unordered(dataset2metadata, _ds_iter()), desc='datasets', total=len(args.datasets)):
                if dataset_metadata is not None:
                    data[dsid] = dataset_metadata
        write_metadata_file(data, args.file)
    else:
        for dsid in ir_datasets.registry._registered:
            dataset = ir_datasets.load(dsid)
            brk = False
            try:
                _, dataset_metadata = dataset2metadata((dsid, data.get(dsid, {})))
                if dataset_metadata is not None:
                    data[dsid] = dataset_metadata
            except KeyboardInterrupt:
                _logger.info(f'KeyboardInterrupt; skipping. ctrl+c within 0.5sec to stop compute_metadata.')
                try:
                    time.sleep(0.5)
                except KeyboardInterrupt:
                    brk = True
                    break
            write_metadata_file(data, args.file)
            if brk:
                break


if __name__ == '__main__':
    main(sys.argv[1:])
