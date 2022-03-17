import requests
import gzip
import io
import random
import sys
import json
import time
import datetime
from contextlib import contextmanager
import re
import os
import unittest
import argparse
import json
import ir_datasets


logger = ir_datasets.log.easy()


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
    rand_delay = None # useful for being nice to servers when running tests by adding a random delay between tests
    output_data = []
    head_precheck = False

    def test_downloads(self):
        with open('ir_datasets/etc/downloads.json') as f:
            data = json.load(f)
        try:
            self._test_download_iter(data)

            # clirmatrix - there's a ton of files, test a few random ones each time
            if self.dlc_filter == '^clirmatrix/':
                dlc = ir_datasets.datasets.clirmatrix.DownloadConfig.context('clirmatrix', ir_datasets.util.home_path()/'clirmatrix')
                with dlc['downloads'].stream() as s:
                    with gzip.open(s) as g:
                        clir_dlc = json.load(g)
                for top_key in clir_dlc.keys():
                    sub_keys = list(clir_dlc[top_key].keys())
                    for sub_key in random.sample(sub_keys, 10):
                        res = self._test_download(clir_dlc[top_key][sub_key], f'clirmatrix/{top_key}/{sub_key}')
                        if res['result'] != 'HEAD_SKIP':
                            self.output_data.append(res)
        finally:
            if self.output_path is not None:
                with open(self.output_path, 'wt') as f:
                    json.dump(self.output_data, f)

    def _test_download_iter(self, data, prefix=''):
        with tmp_environ(IR_DATASETS_DL_TRIES='10'): # give the test up to 10 attempts to download
            if 'url' in data and 'expected_md5' in data:
                if self.dlc_filter is None or re.search(self.dlc_filter, prefix) and not data.get('skip_test', False) and not data.get('auth', False):
                    res = self._test_download(data, prefix)
                    if res['result'] != 'HEAD_SKIP':
                        self.output_data.append(res)
            elif 'instructions' in data:
                pass
            else:
                for key in data.keys():
                    self._test_download_iter(data[key], prefix=f'{prefix}/{key}' if prefix else key)

    def _test_download(self, data, download_id):
        record = {
            'name': download_id,
            'url': data['url'],
            'time': datetime.datetime.now().isoformat(),
            'duration': None,
            'result': 'IN_PROGRESS',
            'fail_messagae': None,
            'md5': data['expected_md5'],
            'size': 0,
        }
        with self.subTest(download_id):
            if self.rand_delay is not None:
                # sleep in range of [0.5, 1.5] * rand_delay seconds
                time.sleep(random.uniform(self.rand_delay * 0.5, self.rand_delay * 1.5))
            start = time.time()
            if self.head_precheck:
                try:
                    requests.head(data['url'], allow_redirects=True).raise_for_status()
                    logger.info('HEAD request for {url} successful'.format(**data))
                    record['result'] = 'HEAD_SKIP'
                    return record # skip if HEAD request passes
                except:
                    logger.info('HEAD request for {url} failed; verifying download'.format(**data))
            try:
                download = ir_datasets.util.Download([ir_datasets.util.RequestsDownload(data['url'], **data.get('download_args', {}))], expected_md5=data['expected_md5'], stream=True)
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
            except Exception as ex:
                record['duration'] = time.time() - start
                record['result'] = 'FAIL' if not data.get('irds_mirror') else 'FAIL_BUT_HAS_MIRROR'
                record['fail_messagae'] = str(ex)
                raise
        return record


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
    for i, arg in enumerate(argv):
        if arg == '--randdelay':
            TestDownloads.rand_delay = float(argv[i+1])
            argv = argv[:i] + argv[i+2:]
    for i, arg in enumerate(argv):
        if arg == '--head_precheck':
            TestDownloads.head_precheck = True
            argv = argv[:i] + argv[i+1:]
    unittest.main(argv=argv)
