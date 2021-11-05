from unittest import main

from test.integration.base import DatasetIntegrationTest


class TestArgsMe(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('argsme/1.0', count=387692, items={
        })
        self._test_docs('argsme/1.0-cleaned', count=382545, items={
        })
        self._test_docs('argsme/2020-04-01', count=387740, items={
        })
        self._test_docs('argsme/2020-04-01/debateorg', count=338620, items={
        })
        self._test_docs('argsme/2020-04-01/debatepedia', count=21197, items={
        })
        self._test_docs('argsme/2020-04-01/debatewise', count=14353, items={
        })
        self._test_docs('argsme/2020-04-01/idebate', count=13522, items={
        })
        self._test_docs('argsme/2020-04-01/parliamentary', count=48, items={
        })


if __name__ == '__main__':
    main()
