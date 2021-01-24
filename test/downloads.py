import sys
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

    def test_downloads(self):
        with open('ir_datasets/etc/downloads.yaml') as f:
            data = yaml.load(f, Loader=yaml.BaseLoader)
        self._test_download_iter(data)

    def _test_download_iter(self, data, prefix=''):
        with tmp_environ(IR_DATASETS_DL_TRIES='10'): # give the test up to 10 attempts to download
            if 'url' in data and 'expected_md5' in data:
                if self.dlc_filter is None or re.search(self.dlc_filter, prefix):
                    with self.subTest(prefix):
                        try:
                            download = ir_datasets.util.Download([ir_datasets.util.RequestsDownload(data['url'])], expected_md5=data['expected_md5'], cache_path=os.devnull)
                            download.path()
                        except KeyboardInterrupt:
                            self.skipTest('Test skipped by user')
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
    unittest.main(argv=argv)
