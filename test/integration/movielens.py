import re
import unittest
import ir_datasets
from ir_datasets.datasets.movielens import MovieLensAction
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestMovielens(DatasetIntegrationTest):
    def test_actions(self):
        self._test_actions('movielens/100k', count=100000, items={
            0: MovieLensAction('196', '242', 3, 881250949),
            9: MovieLensAction('6', '86', 3, 883603013),
            99999: MovieLensAction('12', '203', 3, 879959583),
        })
        self._test_actions('movielens/100k/fold1/train', count=80000, items={
            0: MovieLensAction('1', '1', 5, 874965758),
            9: MovieLensAction('1', '13', 5, 875071805),
            79999: MovieLensAction('943', '1330', 3, 888692465),
        })
        self._test_actions('movielens/100k/fold1/test', count=20000, items={
            0: MovieLensAction('1', '6', 5, 887431973),
            9: MovieLensAction('1', '31', 3, 875072144),
            19999: MovieLensAction('462', '682', 5, 886365231),
        })
        self._test_actions('movielens/100k/fold2/train', count=80000, items={
            0: MovieLensAction('1', '3', 4, 878542960),
            9: MovieLensAction('1', '14', 5, 874965706),
            79999: MovieLensAction('943', '1330', 3, 888692465),
        })
        self._test_actions('movielens/100k/fold2/test', count=20000, items={
            0: MovieLensAction('1', '1', 5, 874965758),
            9: MovieLensAction('1', '34', 2, 878542869),
            19999: MovieLensAction('658', '276', 4, 875145572),
        })
        self._test_actions('movielens/100k/fold3/train', count=80000, items={
            0: MovieLensAction('1', '1', 5, 874965758),
            9: MovieLensAction('1', '12', 5, 878542960),
            79999: MovieLensAction('943', '1330', 3, 888692465),
        })
        self._test_actions('movielens/100k/fold3/test', count=20000, items={
            0: MovieLensAction('1', '5', 3, 889751712),
            9: MovieLensAction('1', '50', 5, 874965954),
            19999: MovieLensAction('877', '382', 3, 882677012),
        })
        self._test_actions('movielens/100k/fold4/train', count=80000, items={
            0: MovieLensAction('1', '1', 5, 874965758),
            9: MovieLensAction('1', '12', 5, 878542960),
            79999: MovieLensAction('943', '1330', 3, 888692465),
        })
        self._test_actions('movielens/100k/fold4/test', count=20000, items={
            0: MovieLensAction('1', '4', 3, 876893119),
            9: MovieLensAction('1', '124', 5, 875071484),
            19999: MovieLensAction('943', '1188', 3, 888640250),
        })
        self._test_actions('movielens/100k/fold5/train', count=80000, items={
            0: MovieLensAction('1', '1', 5, 874965758),
            9: MovieLensAction('1', '11', 2, 875072262),
            79999: MovieLensAction('943', '1188', 3, 888640250),
        })
        self._test_actions('movielens/100k/fold5/test', count=20000, items={
            0: MovieLensAction('1', '3', 4, 878542960),
            9: MovieLensAction('1', '83', 3, 875072370),
            19999: MovieLensAction('943', '1330', 3, 888692465),
        })
        self._test_actions('movielens/100k/split-a/train', count=90570, items={
            0: MovieLensAction('1', '1', 5, 874965758),
            9: MovieLensAction('1', '10', 3, 875693118),
            90569: MovieLensAction('943', '1330', 3, 888692465),
        })
        self._test_actions('movielens/100k/split-a/test', count=9430, items={
            0: MovieLensAction('1', '20', 4, 887431883),
            9: MovieLensAction('1', '265', 4, 878542441),
            9429: MovieLensAction('943', '1067', 2, 875501756),
        })
        self._test_actions('movielens/100k/split-b/train', count=90570, items={
            0: MovieLensAction('1', '1', 5, 874965758),
            9: MovieLensAction('1', '10', 3, 875693118),
            90569: MovieLensAction('943', '1330', 3, 888692465),
        })
        self._test_actions('movielens/100k/split-b/test', count=9430, items={
            0: MovieLensAction('1', '17', 3, 875073198),
            9: MovieLensAction('1', '253', 5, 874965970),
            9429: MovieLensAction('943', '1011', 2, 875502560),
        })


if __name__ == '__main__':
    unittest.main()
