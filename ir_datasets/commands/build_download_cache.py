import sys
import time
import io
import os
import argparse
import json
from contextlib import contextmanager
import ir_datasets


_logger = ir_datasets.log.easy()


@contextmanager
def tmp_environ(**kwargs):
    orig_values = {}
    for key, value in kwargs.items():
        orig_values[key] = os.environ.get(key)
        os.environ[key] = value
    try:
        yield
    finally:
        for key, value in kwargs.items():
            orig_value = orig_values[key]
            if orig_value is not None:
                os.environ[key] = orig_value
            else:
                del os.environ[key]


def _build_cache(data, dir, prefix=''):
    if 'url' in data and 'expected_md5' in data:
        cache_path = f'{dir}/{data["expected_md5"]}'
        if os.path.exists(cache_path):
            _logger.info(f'skipping {prefix}; already exists')
            return
        try:
            with ir_datasets.util.finialized_file(cache_path, 'wb') as fout, _logger.duration(prefix):
                download = ir_datasets.util.Download([ir_datasets.util.RequestsDownload(data['url'])], expected_md5=data['expected_md5'], stream=True)
                with download.stream() as stream:
                    inp = stream.read(io.DEFAULT_BUFFER_SIZE)
                    while len(inp) > 0:
                        fout.write(inp)
                        inp = stream.read(io.DEFAULT_BUFFER_SIZE)
        except KeyboardInterrupt:
            _logger.info('download skipped by user (ctrl+c again in the next 0.5 seconds to exit)')
            try:
                time.sleep(0.5)
            except KeyboardInterrupt:
                sys.exit(1)
        except Exception as ex:
            _logger.warn(f'error: {ex}')
    elif 'instructions' in data:
        pass
    else:
        for key in data.keys():
            _build_cache(data[key], dir, prefix=f'{prefix}/{key}' if prefix else key)


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets build_download_cache', description='Builds a cache of downloadable content')
    parser.add_argument('--dir', default=f'{ir_datasets.util.home_path()}/downloads')
    parser.add_argument('--retries', default='10')
    args = parser.parse_args(args)

    with open('ir_datasets/etc/downloads.json') as f:
        data = json.load(f)
    with tmp_environ(IR_DATASETS_DL_TRIES=args.retries):
        _build_cache(data, args.dir)


if __name__ == '__main__':
    main(sys.argv[1:])
