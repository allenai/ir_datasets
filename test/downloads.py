import sys
import re
import os
import unittest
import argparse
import yaml
import ir_datasets


class TestDownloads(unittest.TestCase):
    dlc_filter = None

    def test_downloads(self):
        with open('ir_datasets/etc/downloads.yaml') as f:
            data = yaml.load(f, Loader=yaml.BaseLoader)
        self._test_download_iter(data)

    def _test_download_iter(self, data, prefix=''):
        if 'url' in data and 'expected_md5' in data:
            if self.dlc_filter is None or re.search(self.dlc_filter, prefix):
                with self.subTest(prefix):
                    try:
                        download = ir_datasets.util.Download([ir_datasets.util.RequestsDownload(data['url'])], expected_md5=data['expected_md5'])
                        path = None
                        try:
                            path = download.path()
                        finally:
                            if path and os.path.exists(path):
                                os.remove(path)
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
