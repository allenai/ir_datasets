import ir_datasets
from ir_datasets.formats import GenericQuery, GenericDoc, TrecQrel
from ir_datasets.datasets.sara import SaraDoc
from .base import DatasetIntegrationTest
import unittest
import re

class TestSara(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('sara', count=129821, items={
            55555: SaraDoc('gladly-united-eagle', "10/8 - NY trans id 5088192 for 62 mw missing in enpower. I take it your not using the download? Jim Cashion Enron Volume Management 713-345-2714", 0),
            12345: SaraDoc('daily-hip-bass', "David: Enclosed is a new draft of the Kennecott amendment letter. Mark and I have discussed the counterparty termination language and have come up with the language that I have high;lighted. Upon your approval, I plan to use the same language for the Duke and Cinnergy letters. Carol St. Clair EB 3892 713-853-3989 (Phone) 713-646-3393 (Fax)", 0),
            5432: SaraDoc('badly-sound-marmot', "Dear UT Team: As you know, we conducted our intern interviews at UT last week. We interviewed almost 50 candidates during round 1 (thanks to Chris Sherman, Jim Cole, Hunter Shively, Dwight Fruge', Stan Dowell, Rick Carson, and Kim Chick), and our round 2 interviewers (Rick Causey, Brent Price, Mark Lindsey, and Mike Deville) have selected the following 11 candidates for a summer internship: Cathy Wang 512-479-7264 Ameet Rane 512-505-2045 Michelle Yee 512-495-3264 Jessica Payne 512-499-8729 Wesley Thoman 512-343-8895 Pranav Gandhi 512-294-4311 (active in student government - elections to be held next Wednesday and Thursday) Daniel Payne 512-472-6739 Kruti Patel 512-356-2321 Rachel Ravanzo 512-689-3814 (also interested in the tax group - will decide between the 2 options, but was extended 1 offer) Vivek Shah 512-495-4066 Vini Adenwala 512-457-8744 I will send cultivation assignments next week, but in the mean time please feel free to call and congratulate these candidates. I have extended verbal offers to each of them and they should receive their offer letters by the end of next week. Thanks to each of our interviewers for their help and long hours! lexi 3-4585", 0),
        })

    def test_queries(self):
        # Test that the dataset 'dummy' has 4 queries, and test the specific queries at indices 0 and 3
        self._test_queries('sara', count=150, items={
            0: GenericQuery('1', 'Politicians that decide the plans from state to state'),
            87: GenericQuery('88', 'Enron procurement'),
            122: GenericQuery('123', 'Senators linked to enron')
        })

    def test_qrels(self):
        # Test that the dataset 'dummy' has 60 qrels, and test the specific qrels at indices 0, 9, and 59
        self._test_qrels('sara', count=34413, items={
            0: TrecQrel('6', 'badly-able-alien', 0, '1'),
            1: TrecQrel('7', 'badly-able-alien', 0, '0'),
            2: TrecQrel('12', 'badly-able-alien', 0, '0')
        })

if __name__ == '__main__':
    unittest.main()