import re
import unittest
from ir_datasets.formats import TrecQrel, GenericDoc, GenericScoredDoc
from ir_datasets.datasets.trec_cast import Cast2019Query, Cast2020Query, Cast2021Query, Cast2022Query, CastDoc, CastPassage, CastPassageDoc
from .base import DatasetIntegrationTest


class TestTrecCast(DatasetIntegrationTest):
    def test_docs(self):
        pass
        # self._test_docs('trec-cast/v0', count=47696605, items={
        #     0: GenericDoc('WAPO_b2e89334-33f9-11e1-825f-dabc29fd7071-1', re.compile('^NEW ORLEANS — Whenever a Virginia Tech offensive coach is asked how the most prolific receiving duo .{1}n school history came to be, inevitably the first road game in 2008 against North Carolina comes up\\.$', flags=48)),
        #     9: GenericDoc('WAPO_b2e89334-33f9-11e1-825f-dabc29fd7071-10', re.compile('^“There’s just some things that we were held back from being able to show,” Boykin said, “that we’re .{102}n Blackmon\\. I feel like they’re great athletes, but at the same time we’re right up there with them\\.$', flags=48)),
        #     9074161: GenericDoc('MARCO_0', re.compile('^The presence of communication amid scientific minds was equally important to the success of the Manh.{125}nd engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated\\.$', flags=48)),
        #     9074170: GenericDoc('MARCO_9', re.compile("^One of the main reasons Hanford was selected as a site for the Manhattan Project's B Reactor was its.{13} the Columbia River, the largest river flowing into the Pacific Ocean from the North American coast\\.$", flags=48)),
        #     47696604: GenericDoc('CAR_ffffffb9eec6224bef5da06e829eef59a37748c6', re.compile('^Fisher recommended Louis as First Sea Lord: "He is the most capable administrator in the Admiralty\'s.{472}that would prepare the navy\'s plans in case of war\\. He was promoted to full admiral on 13 July 1912\\.$', flags=48)),
        # })
        # self._test_docs('trec-cast/v1', count=38622444, items={
        #     0: GenericDoc('MARCO_0', re.compile('^The presence of communication amid scientific minds was equally important to the success of the Manh.{125}nd engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated\\.$', flags=48)),
        #     9: GenericDoc('MARCO_9', re.compile("^One of the main reasons Hanford was selected as a site for the Manhattan Project's B Reactor was its.{13} the Columbia River, the largest river flowing into the Pacific Ocean from the North American coast\\.$", flags=48)),
        #     38622443: GenericDoc('CAR_ffffffb9eec6224bef5da06e829eef59a37748c6', re.compile('^Fisher recommended Louis as First Sea Lord: "He is the most capable administrator in the Admiralty\'s.{472}that would prepare the navy\'s plans in case of war\\. He was promoted to full admiral on 13 July 1912\\.$', flags=48)),
        # })
        # self._test_docs('trec-cast/v3', count=106400940, items={
        #     0: CastPassageDoc('MARCO_00_0-1', '0-60 Times - 0-60 | 0 to 60 Times & 1/4 Mile Times | Zero to 60 Car Reviews', 'http://0-60.reviews/0-60-times/', re.compile('^0\\-60 Times \\- 0\\-60 \\| 0 to 60 Times \\& 1/4 Mile Times \\| Zero to 60 Car Reviews 0\\-60 Times There are man.{955}y miler per hour mark\\. The quarter mile time can often have more variable such as driver experience\\.$', flags=48)),
        #     9: CastPassageDoc('MARCO_00_4806-6', 'Ethel Percy Andrus Gerontology Center [WorldCat Identities]', 'http://0-www.worldcat.org.novacat.nova.edu/identities/lccn-n79036869/', re.compile('^Conclusions are provided that deal with  future research in this area of gerontology training, as we.{1011}such as General safety, Vehicle accidents, and Statistics\\. Entry gives bibliographical  information\\.$', flags=48)),
        #     106400939: CastPassageDoc('KILT_5141410-55', 'Origin of the domestic dog', 'https://en.wikipedia.org/w/index.php?title=Origin%20of%20the%20domestic%20dog&oldid=908221312', re.compile('^Intentional dog burials together with ungulate hunting is also found in other early Holocene deciduo.{78}s the Holarctic temperate zone hunting dogs were a widespread adaptation to forest ungulate hunting\\.$', flags=48)),
        # })
        self._build_test_docs('trec-cast/v2')

    def test_queries(self):
    #     self._test_queries('trec-cast/v0/train', count=269, items={
    #         0: Cast2019Query('1_1', "What is a physician's assistant?", 1, 1, "Career choice for Nursing and Physician's Assistant", "Considering career options for becoming a physician's assistant vs a nurse.  Discussion topics include required education (including time, cost), salaries, and which is better overall."),
    #         9: Cast2019Query('1_10', 'Is a PA above a NP?', 1, 10, "Career choice for Nursing and Physician's Assistant", "Considering career options for becoming a physician's assistant vs a nurse.  Discussion topics include required education (including time, cost), salaries, and which is better overall."),
    #         268: Cast2019Query('30_7', 'Tell me about how I can share files.', 30, 7, 'Linux and Windows', 'A comparison of Windows and Linux, followed by some tips regarding software installation etc.'),
    #     })
    #     self._test_queries('trec-cast/v0/train/judged', count=120, items={
    #         0: Cast2019Query('1_1', "What is a physician's assistant?", 1, 1, "Career choice for Nursing and Physician's Assistant", "Considering career options for becoming a physician's assistant vs a nurse.  Discussion topics include required education (including time, cost), salaries, and which is better overall."),
    #         9: Cast2019Query('1_10', 'Is a PA above a NP?', 1, 10, "Career choice for Nursing and Physician's Assistant", "Considering career options for becoming a physician's assistant vs a nurse.  Discussion topics include required education (including time, cost), salaries, and which is better overall."),
    #         119: Cast2019Query('30_7', 'Tell me about how I can share files.', 30, 7, 'Linux and Windows', 'A comparison of Windows and Linux, followed by some tips regarding software installation etc.'),
    #     })
    #     self._test_queries('trec-cast/v1/2019', count=479, items={
    #         0: Cast2019Query('31_1', 'What is throat cancer?', 31, 1, 'head and neck cancer', 'A person is trying to compare and contrast types of cancer in the throat, esophagus, and lungs.'),
    #         9: Cast2019Query('32_1', 'What are the different types of sharks?', 32, 1, 'sharks', 'Information about sharks including several of the main types of sharks, their biological properties including size (whether they have teeth), as well as adaptations.  This includes difference between sharks and whales.'),
    #         478: Cast2019Query('80_10', 'What was the impact of the expedition?', 80, 10, 'Lewis and Clark expedition', 'Information about the Lewis and Clark expedition, findings, and its significance in US history.'),
    #     })
    #     self._test_queries('trec-cast/v1/2019/judged', count=173, items={
    #         0: Cast2019Query('31_1', 'What is throat cancer?', 31, 1, 'head and neck cancer', 'A person is trying to compare and contrast types of cancer in the throat, esophagus, and lungs.'),
    #         9: Cast2019Query('32_1', 'What are the different types of sharks?', 32, 1, 'sharks', 'Information about sharks including several of the main types of sharks, their biological properties including size (whether they have teeth), as well as adaptations.  This includes difference between sharks and whales.'),
    #         172: Cast2019Query('79_9', 'What are modern examples of conflict theory?', 79, 9, 'sociology', 'Information about the field of sociology including important people, theories, and how they relate to one another.'),
    #     })
    #     self._test_queries('trec-cast/v1/2020', count=216, items={
    #         0: Cast2020Query('81_1', 'How do you know when your garage door opener is going bad?', 'How do you know when your garage door opener is going bad?', 'How do you know when your garage door opener is going bad?', 'MARCO_5498474', 81, 1),
    #         9: Cast2020Query('82_2', 'What are the pros and cons?', 'What are the pros and cons of GMO Food labeling?', 'What are the pros and cons of GMO food labeling?', 'CAR_bafb3c1c72e23c444e182cac4e0ea9e4330d21c9', 82, 2),
    #         215: Cast2020Query('105_9', 'What else motivates the Black Lives Matter movement?', 'What else motivates the Black Lives Matter movement?', 'What else motivates the Black Lives Matter movement?', 'MARCO_801480', 105, 9),
    #     })
    #     self._test_queries('trec-cast/v1/2020/judged', count=208, items={
    #         0: Cast2020Query('81_1', 'How do you know when your garage door opener is going bad?', 'How do you know when your garage door opener is going bad?', 'How do you know when your garage door opener is going bad?', 'MARCO_5498474', 81, 1),
    #         9: Cast2020Query('82_2', 'What are the pros and cons?', 'What are the pros and cons of GMO Food labeling?', 'What are the pros and cons of GMO food labeling?', 'CAR_bafb3c1c72e23c444e182cac4e0ea9e4330d21c9', 82, 2),
    #         207: Cast2020Query('105_9', 'What else motivates the Black Lives Matter movement?', 'What else motivates the Black Lives Matter movement?', 'What else motivates the Black Lives Matter movement?', 'MARCO_801480', 105, 9),
    #     })
        self._test_queries('trec-cast/v3/2022', count=408, items={
            0: Cast2022Query('132_1-1', None, 'User', 'I remember Glasgow hosting COP26 last year, but unfortunately I was out of the loop. What was it about?', 'I remember Glasgow hosting COP26 last year, but unfortunately I was out of the loop. What was the conference about?', None, [], 132, '1-1'),
            9: Cast2022Query('132_2-2', '2-1_2-2', 'System', None, None, 'For several years, there have been concerns that climate change negotiations will essentially ignore a key principle of climate change negotiation frameworks: the common but differentiated responsibilities. Realizing that greenhouse emissions remain in the atmosphere for a very long time, this principle recognizes that historically: Industrialized nations have emitted far more greenhouse gas emissions (even if some developing nations are only now increasing theirs); Rich countries, therefore, face the biggest responsibility and burden for action to address climate change; and Rich countries, therefore, must support developing nations adapt—through financing and technology transfer, for example. This notion of climate justice is typically ignored by many rich nations and their mainstream media, making it easy to blame China, India and other developing countries for failures in climate change mitigation negotiations.', ['MARCO_06_772219573-9'], 132, '2-2'),
            407: Cast2022Query('149_4-2', '4-1_4-2', 'System', None, None, 'Consider using a number of different search engines. While Google is a globally recognized search engine and an industry giant, in fact, even the second biggest “search engine” is Google Images, according to this study: Search engine market share Even if it’s the biggest and most well known, it doesn’t mean it’s your only choice. One of the main reasons that people choose to use an alternative search engine instead is for increased privacy, as Google is known to track user data both for its own and third-party use. If you’ve only ever used Google, check out some of the other search engines and you might find something that you prefer. Bing Bing search engine Microsoft’s Bing is the second largest search engine after Google. It’s easy to use and provides a more visual experience with beautiful daily background photos. Bing is great for video searches, as it displays results as large thumbnails that can be previewed with sound by hovering over them. DuckDuckGo is a popular search engine for those who value their privacy and are put off by the thought of their every query being tracked and logged.It has a very clean interface with minimal ads and infinite scrolling, so the user experience is nice and streamlined. There’s absolutely zero user tracking, and you can even add DuckDuckGo’s extension to your browser to keep your activity private.', ['MARCO_23_183358734-1', 'MARCO_23_183358734-2', 'MARCO_23_183358734-31'], 149, '4-2'),
        })
        self._test_queries('trec-cast/v2/2021', count=239, items={
            0: Cast2021Query('106_1', 'I just had a breast biopsy for cancer. What are the most common types?', 'What are the most common types of cancer in regards to breast biopsy?', 'I just had a breast biopsy for cancer. What are the most common types of breast cancer?', 'MARCO_D59865', 106, 1),
            9: Cast2021Query('106_10', 'Does freezing work?', 'Does freezing work for breast cancer?', 'Does freezing tumors work as an alternative to surgery for stage 1 invasive lobular cancer?', 'MARCO_D909677', 106, 10),
            238: Cast2021Query('131_10', 'How is it different from a heat pump?', 'How is an AC compressor different from a heat pump?', 'How is an AC system different from a heat pump?', 'MARCO_D981398', 131, 10),
        })

    def test_qrels(self):
    #     self._test_qrels('trec-cast/v0/train', count=2399, items={
    #         0: TrecQrel('1_1', 'MARCO_955948', 2, '0'),
    #         9: TrecQrel('1_1', 'MARCO_4903530', 0, '0'),
    #         2398: TrecQrel('30_7', 'MARCO_4250016', 0, '0'),
    #     })
    #     self._test_qrels('trec-cast/v0/train/judged', count=2399, items={
    #         0: TrecQrel('1_1', 'MARCO_955948', 2, '0'),
    #         9: TrecQrel('1_1', 'MARCO_4903530', 0, '0'),
    #         2398: TrecQrel('30_7', 'MARCO_4250016', 0, '0'),
    #     })
    #     self._test_qrels('trec-cast/v1/2019', count=29350, items={
    #         0: TrecQrel('31_1', 'CAR_116d829c4c800c2fc70f11692fec5e8c7e975250', 0, 'Q0'),
    #         9: TrecQrel('31_1', 'CAR_40c64256e988c8103550008f4e9b7ce436d9536d', 2, 'Q0'),
    #         29349: TrecQrel('79_9', 'MARCO_8795237', 3, 'Q0'),
    #     })
    #     self._test_qrels('trec-cast/v1/2019/judged', count=29350, items={
    #         0: TrecQrel('31_1', 'CAR_116d829c4c800c2fc70f11692fec5e8c7e975250', 0, 'Q0'),
    #         9: TrecQrel('31_1', 'CAR_40c64256e988c8103550008f4e9b7ce436d9536d', 2, 'Q0'),
    #         29349: TrecQrel('79_9', 'MARCO_8795237', 3, 'Q0'),
    #     })
    #     self._test_qrels('trec-cast/v1/2020', count=40451, items={
    #         0: TrecQrel('81_1', 'CAR_3add84966af079ed84e8b2fc412ad1dc27800127', 1, '0'),
    #         9: TrecQrel('81_1', 'MARCO_1381086', 1, '0'),
    #         40450: TrecQrel('105_9', 'MARCO_8757526', 0, '0'),
    #     })
    #     self._test_qrels('trec-cast/v1/2020/judged', count=40451, items={
    #         0: TrecQrel('81_1', 'CAR_3add84966af079ed84e8b2fc412ad1dc27800127', 1, '0'),
    #         9: TrecQrel('81_1', 'MARCO_1381086', 1, '0'),
    #         40450: TrecQrel('105_9', 'MARCO_8757526', 0, '0'),
    #     })
        self._test_qrels('trec-cast/v3/2022', count=42196, items={
            0: TrecQrel('132_1-1', 'KILT_1168284-2', 0, '0'),
            9: TrecQrel('132_1-1', 'KILT_39457508-1', 0, '0'),
            42195: TrecQrel('149_3-9', 'MARCO_56_987506629-2', 2, '0'),
        })
        self._test_qrels('trec-cast/v2/2021', count=19334, items={
            0: TrecQrel('106_1', 'KILT_105219', 0, '0'),
            9: TrecQrel('106_1', 'KILT_30271975', 0, '0'),
            19333: TrecQrel('131_10', 'MARCO_D981403', 4, '0'),
        })

    # def test_scoreddocs(self):
    #     self._test_scoreddocs('trec-cast/v0/train', count=269000, items={
    #         0: GenericScoredDoc('1_1', 'MARCO_955948', -5.32579),
    #         9: GenericScoredDoc('1_1', 'CAR_87772d4208721133d00d7d62f4eaaf164da5b4e3', -5.44505),
    #         268999: GenericScoredDoc('30_7', 'WAPO_595c1be2ba9e3b1e66d552a174219c12-3', -7.07828),
    #     })
    #     self._test_scoreddocs('trec-cast/v0/train/judged', count=120000, items={
    #         0: GenericScoredDoc('1_1', 'MARCO_955948', -5.32579),
    #         9: GenericScoredDoc('1_1', 'CAR_87772d4208721133d00d7d62f4eaaf164da5b4e3', -5.44505),
    #         119999: GenericScoredDoc('30_7', 'WAPO_595c1be2ba9e3b1e66d552a174219c12-3', -7.07828),
    #     })
    #     self._test_scoreddocs('trec-cast/v1/2019', count=479000, items={
    #         0: GenericScoredDoc('31_1', 'MARCO_789620', -5.71312),
    #         9: GenericScoredDoc('31_1', 'MARCO_291004', -5.88053),
    #         478999: GenericScoredDoc('80_10', 'CAR_268dcb1c6bc4326f81500513e0ad9d11acb2a693', -5.23496),
    #     })
    #     self._test_scoreddocs('trec-cast/v1/2019/judged', count=173000, items={
    #         0: GenericScoredDoc('31_1', 'MARCO_789620', -5.71312),
    #         9: GenericScoredDoc('31_1', 'MARCO_291004', -5.88053),
    #         172999: GenericScoredDoc('79_9', 'MARCO_1431776', -6.75024),
    #     })


if __name__ == '__main__':
    unittest.main()
