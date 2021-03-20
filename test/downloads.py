import io
import sys
import json
import time
import datetime
from contextlib import contextmanager
import re
import os
import unittest
import argparse
import yaml
import ir_datasets


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


class TestDownloads(unittest.TestCase):
    dlc_filter = None
    output_path = None
    output_data = []

    def test_downloads(self):
        with open('ir_datasets/etc/downloads.yaml') as f:
            data = yaml.load(f, Loader=yaml.BaseLoader)
        try:
            self._test_download_iter(data)
        finally:
            if self.output_path is not None:
                with open(self.output_path, 'wt') as f:
                    json.dump(self.output_data, f)

    def _test_download_iter(self, data, prefix=''):
        with tmp_environ(IR_DATASETS_DL_TRIES='10'): # give the test up to 10 attempts to download
            if 'url' in data and 'expected_md5' in data:
                if self.dlc_filter is None or re.search(self.dlc_filter, prefix) and not data.get('skip_test', False):
                    with self.subTest(prefix):
                        record = {
                            'name': prefix,
                            'url': data['url'],
                            'time': datetime.datetime.now().isoformat(),
                            'duration': None,
                            'result': 'IN_PROGRESS',
                            'md5': data['expected_md5'],
                            'size': 0,
                        }
                        self.output_data.append(record)
                        start = time.time()
                        try:
                            download = ir_datasets.util.Download([ir_datasets.util.RequestsDownload(data['url'])], expected_md5=data['expected_md5'], stream=True)
                            with download.stream() as stream:
                                inp = stream.read(io.DEFAULT_BUFFER_SIZE)
                                while len(inp) > 0:
                                    record['size'] += len(inp)
                                    inp = stream.read(io.DEFAULT_BUFFER_SIZE)
                            record['duration'] = time.time() - start
                            record['result'] = 'PASS'
                        except KeyboardInterrupt:
                            record['duration'] = time.time() - start
                            record['result'] = 'USER_SKIP'
                            self.skipTest('Test skipped by user')
                            self.output_data.append({})
                        except:
                            record['duration'] = time.time() - start
                            record['result'] = 'FAIL'
                            raise
            elif 'instructions' in data:
                pass
            else:
                for key in data.keys():
                    self._test_download_iter(data[key], prefix=f'{prefix}/{key}' if prefix else key)


if __name__ == '__main__':
    argv = sys.argv
    for i, arg in enumerate(argv):
        if arg == '--filter':
            TestDownloads.dlc_filter = argv[i+1]
            argv = argv[:i] + argv[i+2:]
    for i, arg in enumerate(argv):
        if arg == '--output':
            TestDownloads.output_path = argv[i+1]
            argv = argv[:i] + argv[i+2:]
    unittest.main(argv=argv)
