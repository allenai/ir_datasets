import re
import unittest
import ir_datasets
from ir_datasets.datasets.msmarco_passage_v2 import MsMarcoV2Passage
from ir_datasets.formats import GenericQuery, TrecQrel, GenericScoredDoc
from .base import DatasetIntegrationTest


class TestMsMarcoPassageV2(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('msmarco-passage-v2', count=138364198, items={
            0: MsMarcoV2Passage('msmarco_passage_00_0', '0-60 Times - 0-60 | 0 to 60 Times & 1/4 Mile Times | Zero to 60 Car Reviews.', ((0, 75),), 'msmarco_doc_00_0'),
            9: MsMarcoV2Passage('msmarco_passage_00_3346', re.compile('^Engineers and designers work tirelessly to provide better and better numbers with each progressive m.{69} or muscle car enthusiast can determine the 0\\-60 times of their cars and make moves to improve them\\.$', flags=48), ((1653, 1789), (1790, 1922)), 'msmarco_doc_00_0'),
            138364197: MsMarcoV2Passage('msmarco_passage_69_159748475', re.compile('^When it asks "What item would you like to create a shortcut for\\?", paste in the URL you want to use .{100} like to name the shortcut\\?", type the name of the meeting \\(i\\.e\\. "Standup Meeting"\\)\\. Click "Finish"\\.$', flags=48), ((1794, 1951), (1952, 1965), (1966, 2078), (2079, 2094)), 'msmarco_doc_59_1043776256'),
        })
        self._test_docs('msmarco-passage-v2/dedup', count=119582876, items={
            0: MsMarcoV2Passage('msmarco_passage_00_0', '0-60 Times - 0-60 | 0 to 60 Times & 1/4 Mile Times | Zero to 60 Car Reviews.', ((0, 75),), 'msmarco_doc_00_0'),
            9: MsMarcoV2Passage('msmarco_passage_00_3346', re.compile('^Engineers and designers work tirelessly to provide better and better numbers with each progressive m.{69} or muscle car enthusiast can determine the 0\\-60 times of their cars and make moves to improve them\\.$', flags=48), ((1653, 1789), (1790, 1922)), 'msmarco_doc_00_0'),
            119582875: MsMarcoV2Passage('msmarco_passage_69_159748475', re.compile('^When it asks "What item would you like to create a shortcut for\\?", paste in the URL you want to use .{100} like to name the shortcut\\?", type the name of the meeting \\(i\\.e\\. "Standup Meeting"\\)\\. Click "Finish"\\.$', flags=48), ((1794, 1951), (1952, 1965), (1966, 2078), (2079, 2094)), 'msmarco_doc_59_1043776256'),
        })
        # the following doc_id is a duplicate and shouldn't be returned in the dedup version
        self.assertRaises(KeyError, lambda: ir_datasets.load('msmarco-passage-v2/dedup').docs.lookup('msmarco_passage_00_9218'))
        self.assertEqual({}, ir_datasets.load('msmarco-passage-v2/dedup').docs.lookup(['msmarco_passage_00_9218']))
        self.assertNotEqual(None, ir_datasets.load('msmarco-passage-v2').docs.lookup('msmarco_passage_00_9218'))
        self.assertEqual(1, len(ir_datasets.load('msmarco-passage-v2').docs.lookup(['msmarco_passage_00_9218'])))

    def test_queries(self):
        self._test_queries('msmarco-passage-v2/train', count=277144, items={
            0: GenericQuery('121352', 'define extreme'),
            9: GenericQuery('80926', 'can you use wallapop on your computer'),
            277143: GenericQuery('50393', 'benefits of boiling lemons and drinking juice.'),
        })
        self._test_queries('msmarco-passage-v2/dev1', count=3903, items={
            0: GenericQuery('2', ' Androgen receptor define'),
            9: GenericQuery('1049200', 'who recorded loving you'),
            3902: GenericQuery('1048565', 'who plays sebastian michaelis'),
        })
        self._test_queries('msmarco-passage-v2/dev2', count=4281, items={
            0: GenericQuery('1048579', 'what is pcnt'),
            9: GenericQuery('1048779', 'what is ott media'),
            4280: GenericQuery('1092262', ';liter chemistry definition'),
        })
        self._test_queries('msmarco-passage-v2/trec-dl-2021', count=477, items={
            0: GenericQuery('787021', 'what is produced by muscle'),
            9: GenericQuery('1052368', 'who stabbed dr. martin luther king'),
            476: GenericQuery('855410', 'what is theraderm used for'),
        })
        self._test_queries('msmarco-passage-v2/trec-dl-2021/judged', count=53, items={
            0: GenericQuery('2082', 'At about what age do adults normally begin to lose bone mass?'),
            9: GenericQuery('1107704', 'what was the main benefit of a single european currency?'),
            52: GenericQuery('1040198', 'who is the final arbiter of florida law in instances where there is no federal authority?'),
        })
        self._test_queries('msmarco-passage-v2/trec-dl-2022', count=500, items={
            0: GenericQuery('588', '1099 b cost basis i sell specific shares'),
            9: GenericQuery('77640', "can you get a master's degree in tefl"),
            499: GenericQuery('2056473', 'is a dairy farm considered as an agriculture'),
        })
        self._test_queries('msmarco-passage-v2/trec-dl-2022/judged', count=76, items={
            0: GenericQuery('2000511', 'average bahamas temperature at the end of october'),
            9: GenericQuery('2003157', 'how to cook frozen ham steak on nuwave oven'),
            75: GenericQuery('2056323', 'how does magic leap optics work'),
        })
        self._test_queries('msmarco-passage-v2/trec-dl-2023', count=700, items={
            0: GenericQuery('2000138', 'How does the process of digestion and metabolism of carbohydrates start'),
            9: GenericQuery('2001686', 'good food and bad food for high cholesterol'),
            699: GenericQuery('3100949', 'How do birth control and hormone levels affect menstrual cycle variations?'),
        })

    def test_qrels(self):
        self._test_qrels('msmarco-passage-v2/train', count=284212, items={
            0: TrecQrel('1185869', 'msmarco_passage_08_840101254', 1, '0'),
            9: TrecQrel('186154', 'msmarco_passage_02_556351008', 1, '0'),
            284211: TrecQrel('697642', 'msmarco_passage_05_512118117', 1, '0'),
        })
        self._test_qrels('msmarco-passage-v2/dev1', count=4009, items={
            0: TrecQrel('763878', 'msmarco_passage_33_459057644', 1, '0'),
            9: TrecQrel('290779', 'msmarco_passage_10_301562908', 1, '0'),
            4008: TrecQrel('1091692', 'msmarco_passage_23_330102695', 1, '0'),
        })
        self._test_qrels('msmarco-passage-v2/dev2', count=4411, items={
            0: TrecQrel('419507', 'msmarco_passage_04_254301507', 1, '0'),
            9: TrecQrel('1087630', 'msmarco_passage_18_685926585', 1, '0'),
            4410: TrecQrel('961297', 'msmarco_passage_18_858458289', 1, '0'),
        })
        self._test_qrels('msmarco-passage-v2/trec-dl-2021', count=10828, items={
            0: TrecQrel('2082', 'msmarco_passage_01_552803451', 0, '0'),
            9: TrecQrel('2082', 'msmarco_passage_02_437070914', 3, '0'),
            10827: TrecQrel('1129560', 'msmarco_passage_68_639912287', 0, '0'),
        })
        self._test_qrels('msmarco-passage-v2/trec-dl-2021/judged', count=10828, items={
            0: TrecQrel('2082', 'msmarco_passage_01_552803451', 0, '0'),
            9: TrecQrel('2082', 'msmarco_passage_02_437070914', 3, '0'),
            10827: TrecQrel('1129560', 'msmarco_passage_68_639912287', 0, '0'),
        })
        self._test_qrels('msmarco-passage-v2/trec-dl-2022', count=386416, items={
            0: TrecQrel('2000511', 'msmarco_passage_00_491550793', 0, '0'),
            9: TrecQrel('2000511', 'msmarco_passage_00_491585086', 0, '0'),
            386415: TrecQrel('2056323', 'msmarco_passage_68_715747739', 1, '0'),
        })
        self._test_qrels('msmarco-passage-v2/trec-dl-2022/judged', count=386416, items={
            0: TrecQrel('2000511', 'msmarco_passage_00_491550793', 0, '0'),
            9: TrecQrel('2000511', 'msmarco_passage_00_491585086', 0, '0'),
            386415: TrecQrel('2056323', 'msmarco_passage_68_715747739', 1, '0'),
        })

    def test_scoreddocs(self):
        self._test_scoreddocs('msmarco-passage-v2/train', count=27713673, items={
            0: GenericScoredDoc('5', 'msmarco_passage_49_25899182', 12.1278),
            9: GenericScoredDoc('5', 'msmarco_passage_53_503988399', 11.2986),
            27713672: GenericScoredDoc('1185869', 'msmarco_passage_41_540702769', 9.739399),
        })
        self._test_scoreddocs('msmarco-passage-v2/dev1', count=390300, items={
            0: GenericScoredDoc('2', 'msmarco_passage_30_389397788', 14.5301),
            9: GenericScoredDoc('2', 'msmarco_passage_05_830539414', 13.1325),
            390299: GenericScoredDoc('1102390', 'msmarco_passage_29_705182521', 9.148),
        })
        self._test_scoreddocs('msmarco-passage-v2/dev2', count=428100, items={
            0: GenericScoredDoc('1325', 'msmarco_passage_35_295199374', 20.979799),
            9: GenericScoredDoc('1325', 'msmarco_passage_68_757687820', 18.208799),
            428099: GenericScoredDoc('1102413', 'msmarco_passage_07_48510484', 10.8093),
        })
        self._test_scoreddocs('msmarco-passage-v2/trec-dl-2021', count=47700, items={
            0: GenericScoredDoc('2082', 'msmarco_passage_45_623131157', 19.8207),
            9: GenericScoredDoc('2082', 'msmarco_passage_30_709623997', 17.350901),
            47699: GenericScoredDoc('1136769', 'msmarco_passage_06_68704200', 14.8941),
        })
        self._test_scoreddocs('msmarco-passage-v2/trec-dl-2022', count=50000, items={
            0: GenericScoredDoc('588', 'msmarco_passage_30_337959223', 18.762699),
            9: GenericScoredDoc('588', 'msmarco_passage_10_355039123', 17.7628),
            49999: GenericScoredDoc('2056473', 'msmarco_passage_17_225374709', 12.147499),
        })
        self._test_scoreddocs('msmarco-passage-v2/trec-dl-2022/judged', count=7600, items={
            0: GenericScoredDoc('2000511', 'msmarco_passage_05_149863652', 17.3363),
            9: GenericScoredDoc('2000511', 'msmarco_passage_10_615816159', 13.9977),
            7599: GenericScoredDoc('2056323', 'msmarco_passage_42_417977141', 9.592),
        })
        self._test_scoreddocs('msmarco-passage-v2/trec-dl-2023', count=70000, items={
            0: GenericScoredDoc('2000138', 'msmarco_passage_04_207262207', 17.8992),
            9: GenericScoredDoc('2000138', 'msmarco_passage_35_358067216', 15.2805),
            69999: GenericScoredDoc('3100949', 'msmarco_passage_30_84437641', 18.801701),
        })


if __name__ == '__main__':
    unittest.main()
