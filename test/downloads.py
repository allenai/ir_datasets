import os
import unittest
import signal
import yaml
import ir_datasets


class TestTimeout(Exception):
    pass


class Timeout:
    def __init__(self, seconds, error_message=None):
        if error_message is None:
            error_message = f'Timeout after {seconds}s'
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TestTimeout(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)


class TestDownloads(unittest.TestCase):
    def test_downloads(self):
        data = yaml.load(open('ir_datasets/etc/downloads.yaml'))
        self._test_download_iter(data)

    def _test_download_iter(self, data, prefix=''):
        if 'url' in data and 'expected_md5' in data:
            with self.subTest(prefix), Timeout(seconds=60*15): # 10 minute timeout
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
    unittest.main()
