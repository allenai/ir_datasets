import re
import unittest
from ir_datasets.datasets.tweets2013_ia import TrecMb13Query, TrecMb14Query, TweetDoc
from ir_datasets.formats import TrecQrel, TrecDoc
from .base import DatasetIntegrationTest


class TestTweets2013Ia(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('tweets2013-ia', items={
            0: TweetDoc('297237850620035072', re.compile('RT @xoneedirecti.*yBirthdayHarry ♥', flags=16), '361751733', 'Fri Feb 01 07:00:00 +0000 2013', 'vi', None, '297235934536142848', re.compile(b'^\\{"created_at":"Fri Feb 01 07:00:00 \\+0000 2013","id":297237850620035072,"id_str":"297237850620035072".{4165}92909891,"id_str":"592909891","indices":\\[3,19\\]\\}\\]\\},"favorited":false,"retweeted":false,"lang":"vi"\\}\\\r\n$', flags=16), 'application/json'),
            9: TweetDoc('297237850628423681', re.compile('Today stats: 6 new followers and .*tp://t.co/Wkb8Rdcm', flags=16), '240617244', 'Fri Feb 01 07:00:00 +0000 2013', 'en', None, None, re.compile(b'^\\{"created_at":"Fri Feb 01 07:00:00 \\+0000 2013","id":297237850628423681,"id_str":"297237850628423681".{1957}\\}\\],"user_mentions":\\[\\]\\},"favorited":false,"retweeted":false,"possibly_sensitive":false,"lang":"en"\\}\\\r\n$', flags=16), 'application/json'),
            1000: TweetDoc('297237951287537665', re.compile('ドバイでコンドームの宅配サー.*tp://t.co/VRviUIJ1', flags=16), '239760031', 'Fri Feb 01 07:00:24 +0000 2013', 'ja', None, None, re.compile(b'^\\{"created_at":"Fri Feb 01 07:00:24 \\+0000 2013","id":297237951287537665,"id_str":"297237951287537665".{1972}\\}\\],"user_mentions":\\[\\]\\},"favorited":false,"retweeted":false,"possibly_sensitive":false,"lang":"ja"\\}\\\r\n$', flags=16), 'application/json'),
        })

    def test_queries(self):
        self._test_queries('tweets2013-ia/trec-mb-2013', count=60, items={
            0: TrecMb13Query('111', 'water shortages', 'Fri Mar 29 18:56:02 +0000 2013', '317711766815653888'),
            9: TrecMb13Query('120', "Argentina's Inflation", 'Tue Mar 19 15:37:48 +0000 2013', '314038001112076290'),
            59: TrecMb13Query('170', 'Tony Mendez', 'Sun Mar 31 14:12:52 +0000 2013', '318365281321881600'),
        })
        self._test_queries('tweets2013-ia/trec-mb-2014', count=55, items={
            0: TrecMb14Query('171', 'Ron Weasley birthday', 'Sat Mar 02 10:43:45 EST 2013', '307878904759201794', "Find tweets regarding the birthday of fictional character Ron Weasley, Harry Potter's sidekick."),
            9: TrecMb14Query('180', 'Sherlock Elementary BBC CBS', 'Sun Mar 31 10:21:28 EDT 2013', '318367445586939904', 'Find opinions on either the BBC "Sherlock" series or "Elementary" on CBS, or comparisons of the shows or characters.'),
            54: TrecMb14Query('225', 'Barbara Walters, chicken pox', 'Tue Mar 12 13:19:59 EDT 2013', '311527001297137664', 'Find information on Barbara Walters having chicken pox and her subsequent\nreturn to the TV show "The View".'),
        })

    def test_qrels(self):
        self._test_qrels('tweets2013-ia/trec-mb-2013', count=71279, items={
            0: TrecQrel('111', '297136541426397184', 0, 'Q0'),
            9: TrecQrel('111', '299374475248537602', 0, 'Q0'),
            71278: TrecQrel('170', '317942407385726976', 0, 'Q0'),
        })
        self._test_qrels('tweets2013-ia/trec-mb-2014', count=57985, items={
            0: TrecQrel('171', '305851659194609664', 0, 'Q0'),
            9: TrecQrel('171', '304392188215836672', 0, 'Q0'),
            57984: TrecQrel('225', '299257357664387072', 0, 'Q0'),
        })


if __name__ == '__main__':
    unittest.main()
