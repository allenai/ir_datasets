import re
import unittest
from ir_datasets.formats import TrecQrel, GenericDoc, GenericScoredDoc
from ir_datasets.datasets.trec_cast import Cast2019Query, Cast2020Query
from .base import DatasetIntegrationTest


class TestTrecCast(DatasetIntegrationTest):
    # def test_docs(self):
    #     self._test_docs('trec-cast/2019', count=47696605, items={
    #         0: GenericDoc('WAPO_b2e89334-33f9-11e1-825f-dabc29fd7071-1', re.compile('^NEW ORLEANS — Whenever a Virginia Tech offensive coach is asked how the most prolific receiving duo .{1}n school history came to be, inevitably the first road game in 2008 against North Carolina comes up\\.$', flags=48)),
    #         9: GenericDoc('WAPO_b2e89334-33f9-11e1-825f-dabc29fd7071-10', re.compile('^“There’s just some things that we were held back from being able to show,” Boykin said, “that we’re .{102}n Blackmon\\. I feel like they’re great athletes, but at the same time we’re right up there with them\\.$', flags=48)),
    #         9074161: GenericDoc('MARCO_0', re.compile('^The presence of communication amid scientific minds was equally important to the success of the Manh.{125}nd engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated\\.$', flags=48)),
    #         9074170: GenericDoc('MARCO_9', re.compile("^One of the main reasons Hanford was selected as a site for the Manhattan Project's B Reactor was its.{13} the Columbia River, the largest river flowing into the Pacific Ocean from the North American coast\\.$", flags=48)),
    #         47696604: GenericDoc('CAR_ffffffb9eec6224bef5da06e829eef59a37748c6', re.compile('^Fisher recommended Louis as First Sea Lord: "He is the most capable administrator in the Admiralty\'s.{472}that would prepare the navy\'s plans in case of war\\. He was promoted to full admiral on 13 July 1912\\.$', flags=48)),
    #     })
    #     self._test_docs('trec-cast/2020', count=38622444, items={
    #         0: GenericDoc('MARCO_0', re.compile('^The presence of communication amid scientific minds was equally important to the success of the Manh.{125}nd engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated\\.$', flags=48)),
    #         9: GenericDoc('MARCO_9', re.compile("^One of the main reasons Hanford was selected as a site for the Manhattan Project's B Reactor was its.{13} the Columbia River, the largest river flowing into the Pacific Ocean from the North American coast\\.$", flags=48)),
    #         38622443: GenericDoc('CAR_ffffffb9eec6224bef5da06e829eef59a37748c6', re.compile('^Fisher recommended Louis as First Sea Lord: "He is the most capable administrator in the Admiralty\'s.{472}that would prepare the navy\'s plans in case of war\\. He was promoted to full admiral on 13 July 1912\\.$', flags=48)),
    #     })

    def test_queries(self):
        self._test_queries('trec-cast/2019/train', count=269, items={
            0: Cast2019Query('1_1', "What is a physician's assistant?", 1, 1, "Career choice for Nursing and Physician's Assistant", "Considering career options for becoming a physician's assistant vs a nurse.  Discussion topics include required education (including time, cost), salaries, and which is better overall."),
            9: Cast2019Query('1_10', 'Is a PA above a NP?', 1, 10, "Career choice for Nursing and Physician's Assistant", "Considering career options for becoming a physician's assistant vs a nurse.  Discussion topics include required education (including time, cost), salaries, and which is better overall."),
            268: Cast2019Query('30_7', 'Tell me about how I can share files.', 30, 7, 'Linux and Windows', 'A comparison of Windows and Linux, followed by some tips regarding software installation etc.'),
        })
        self._test_queries('trec-cast/2019/train/judged', count=120, items={
            0: Cast2019Query('1_1', "What is a physician's assistant?", 1, 1, "Career choice for Nursing and Physician's Assistant", "Considering career options for becoming a physician's assistant vs a nurse.  Discussion topics include required education (including time, cost), salaries, and which is better overall."),
            9: Cast2019Query('1_10', 'Is a PA above a NP?', 1, 10, "Career choice for Nursing and Physician's Assistant", "Considering career options for becoming a physician's assistant vs a nurse.  Discussion topics include required education (including time, cost), salaries, and which is better overall."),
            119: Cast2019Query('30_7', 'Tell me about how I can share files.', 30, 7, 'Linux and Windows', 'A comparison of Windows and Linux, followed by some tips regarding software installation etc.'),
        })
        self._test_queries('trec-cast/2019/eval', count=479, items={
            0: Cast2019Query('31_1', 'What is throat cancer?', 31, 1, 'head and neck cancer', 'A person is trying to compare and contrast types of cancer in the throat, esophagus, and lungs.'),
            9: Cast2019Query('32_1', 'What are the different types of sharks?', 32, 1, 'sharks', 'Information about sharks including several of the main types of sharks, their biological properties including size (whether they have teeth), as well as adaptations.  This includes difference between sharks and whales.'),
            478: Cast2019Query('80_10', 'What was the impact of the expedition?', 80, 10, 'Lewis and Clark expedition', 'Information about the Lewis and Clark expedition, findings, and its significance in US history.'),
        })
        self._test_queries('trec-cast/2019/eval/judged', count=173, items={
            0: Cast2019Query('31_1', 'What is throat cancer?', 31, 1, 'head and neck cancer', 'A person is trying to compare and contrast types of cancer in the throat, esophagus, and lungs.'),
            9: Cast2019Query('32_1', 'What are the different types of sharks?', 32, 1, 'sharks', 'Information about sharks including several of the main types of sharks, their biological properties including size (whether they have teeth), as well as adaptations.  This includes difference between sharks and whales.'),
            172: Cast2019Query('79_9', 'What are modern examples of conflict theory?', 79, 9, 'sociology', 'Information about the field of sociology including important people, theories, and how they relate to one another.'),
        })
        self._test_queries('trec-cast/2020', count=216, items={
            0: Cast2020Query('81_1', 'How do you know when your garage door opener is going bad?', 'How do you know when your garage door opener is going bad?', 'How do you know when your garage door opener is going bad?', 'MARCO_5498474', 81, 1),
            9: Cast2020Query('82_2', 'What are the pros and cons?', 'What are the pros and cons of GMO Food labeling?', 'What are the pros and cons of GMO food labeling?', 'CAR_bafb3c1c72e23c444e182cac4e0ea9e4330d21c9', 82, 2),
            215: Cast2020Query('105_9', 'What else motivates the Black Lives Matter movement?', 'What else motivates the Black Lives Matter movement?', 'What else motivates the Black Lives Matter movement?', 'MARCO_801480', 105, 9),
        })
        self._test_queries('trec-cast/2020/judged', count=208, items={
            0: Cast2020Query('81_1', 'How do you know when your garage door opener is going bad?', 'How do you know when your garage door opener is going bad?', 'How do you know when your garage door opener is going bad?', 'MARCO_5498474', 81, 1),
            9: Cast2020Query('82_2', 'What are the pros and cons?', 'What are the pros and cons of GMO Food labeling?', 'What are the pros and cons of GMO food labeling?', 'CAR_bafb3c1c72e23c444e182cac4e0ea9e4330d21c9', 82, 2),
            207: Cast2020Query('105_9', 'What else motivates the Black Lives Matter movement?', 'What else motivates the Black Lives Matter movement?', 'What else motivates the Black Lives Matter movement?', 'MARCO_801480', 105, 9),
        })

    def test_qrels(self):
        self._test_qrels('trec-cast/2019/train', count=2399, items={
            0: TrecQrel('1_1', 'MARCO_955948', 2, '0'),
            9: TrecQrel('1_1', 'MARCO_4903530', 0, '0'),
            2398: TrecQrel('30_7', 'MARCO_4250016', 0, '0'),
        })
        self._test_qrels('trec-cast/2019/train/judged', count=2399, items={
            0: TrecQrel('1_1', 'MARCO_955948', 2, '0'),
            9: TrecQrel('1_1', 'MARCO_4903530', 0, '0'),
            2398: TrecQrel('30_7', 'MARCO_4250016', 0, '0'),
        })
        self._test_qrels('trec-cast/2019/eval', count=29350, items={
            0: TrecQrel('31_1', 'CAR_116d829c4c800c2fc70f11692fec5e8c7e975250', 0, 'Q0'),
            9: TrecQrel('31_1', 'CAR_40c64256e988c8103550008f4e9b7ce436d9536d', 2, 'Q0'),
            29349: TrecQrel('79_9', 'MARCO_8795237', 3, 'Q0'),
        })
        self._test_qrels('trec-cast/2019/eval/judged', count=29350, items={
            0: TrecQrel('31_1', 'CAR_116d829c4c800c2fc70f11692fec5e8c7e975250', 0, 'Q0'),
            9: TrecQrel('31_1', 'CAR_40c64256e988c8103550008f4e9b7ce436d9536d', 2, 'Q0'),
            29349: TrecQrel('79_9', 'MARCO_8795237', 3, 'Q0'),
        })
        self._test_qrels('trec-cast/2020', count=40451, items={
            0: TrecQrel('81_1', 'CAR_3add84966af079ed84e8b2fc412ad1dc27800127', 1, '0'),
            9: TrecQrel('81_1', 'MARCO_1381086', 1, '0'),
            40450: TrecQrel('105_9', 'MARCO_8757526', 0, '0'),
        })
        self._test_qrels('trec-cast/2020/judged', count=40451, items={
            0: TrecQrel('81_1', 'CAR_3add84966af079ed84e8b2fc412ad1dc27800127', 1, '0'),
            9: TrecQrel('81_1', 'MARCO_1381086', 1, '0'),
            40450: TrecQrel('105_9', 'MARCO_8757526', 0, '0'),
        })

    def test_scoreddocs(self):
        self._test_scoreddocs('trec-cast/2019/train', count=269000, items={
            0: GenericScoredDoc('1_1', 'MARCO_955948', -5.32579),
            9: GenericScoredDoc('1_1', 'CAR_87772d4208721133d00d7d62f4eaaf164da5b4e3', -5.44505),
            268999: GenericScoredDoc('30_7', 'WAPO_595c1be2ba9e3b1e66d552a174219c12-3', -7.07828),
        })
        self._test_scoreddocs('trec-cast/2019/train/judged', count=120000, items={
            0: GenericScoredDoc('1_1', 'MARCO_955948', -5.32579),
            9: GenericScoredDoc('1_1', 'CAR_87772d4208721133d00d7d62f4eaaf164da5b4e3', -5.44505),
            119999: GenericScoredDoc('30_7', 'WAPO_595c1be2ba9e3b1e66d552a174219c12-3', -7.07828),
        })
        self._test_scoreddocs('trec-cast/2019/eval', count=479000, items={
            0: GenericScoredDoc('31_1', 'MARCO_789620', -5.71312),
            9: GenericScoredDoc('31_1', 'MARCO_291004', -5.88053),
            478999: GenericScoredDoc('80_10', 'CAR_268dcb1c6bc4326f81500513e0ad9d11acb2a693', -5.23496),
        })
        self._test_scoreddocs('trec-cast/2019/eval/judged', count=173000, items={
            0: GenericScoredDoc('31_1', 'MARCO_789620', -5.71312),
            9: GenericScoredDoc('31_1', 'MARCO_291004', -5.88053),
            172999: GenericScoredDoc('79_9', 'MARCO_1431776', -6.75024),
        })


if __name__ == '__main__':
    unittest.main()
