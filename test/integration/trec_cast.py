import re
import unittest
from ir_datasets.formats import TrecQrel, GenericDoc, GenericScoredDoc
from ir_datasets.datasets.trec_cast import Cast2019Query, Cast2020Query, Cast2021Query, Cast2022Query, CastDoc, CastPassage, CastPassageDoc
from .base import DatasetIntegrationTest


class TestTrecCast(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('trec-cast/v1', count=None, items={
            0: GenericDoc(doc_id='MARCO_0', text='The presence of communication amid scientific minds was equally important to the success of the Manhattan Project as scientific intellect was. The only cloud hanging over the impressive achievement of the atomic researchers and engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated.'),
            49: GenericDoc(doc_id='MARCO_49', text='Color—urine can be a variety of colors, most often shades of yellow, from very pale or colorless to very dark or amber. Unusual or abnormal urine colors can be the result of a disease process, several medications (e.g., multivitamins can turn urine bright yellow), or the result of eating certain foods.')
        })
        self._test_docs('trec-cast/v2', count=None, items={
            0: CastDoc(doc_id='MARCO_D1555982', title='The hot glowing surfaces of stars emit energy in the form of electromagnetic radiation.?', url='https://answers.yahoo.com/question/index?qid=20071007114826AAwCFvR', passages=['Science & Mathematics Physics The hot glowing surfaces of stars emit energy in the form of electromagnetic radiation.? It is a good approximation to assume that the emissivity e is equal to 1 for these surfaces. Find the radius of the star Rigel, the bright blue star in the constellation Orion that radiates energy at a rate of 2.7 x 10^32 W and has a surface temperature of 11,000 K. Assume that the star is spherical. Use σ =... show more Follow 3 answers Answers Relevance Rating Newest Oldest Best Answer: Stefan-Boltzmann law states that the energy flux by radiation is proportional to the forth power of the temperature: q = ε · σ · T^4 The total energy flux at a spherical surface of Radius R is Q = q·π·R² = ε·σ·T^4·π·R² Hence the radius is R = √ ( Q / (ε·σ·T^4·π) ) = √ ( 2.7x10+32 W / (1 · 5.67x10-8W/m²K^4 · (1100K)^4 · π) ) = 3.22x10+13 m Source (s):http://en.wikipedia.org/wiki/Stefan_bolt...schmiso · 1 decade ago0 18 Comment Schmiso, you forgot a 4 in your answer. Your link even says it: L = 4pi (R^2)sigma (T^4). Using L, luminosity, as the energy in this problem, you can find the radius R by doing sqrt (L/ (4pisigma (T^4)). Hope this helps everyone. Caroline · 4 years ago4 1 Comment (Stefan-Boltzmann law) L = 4pi*R^2*sigma*T^4 Solving for R we get: => R = (1/ (2T^2)) * sqrt (L/ (pi*sigma)) Plugging in your values you should get: => R = (1/ (2 (11,000K)^2)) *sqrt ( (2.7*10^32W)/ (pi * (5.67*10^-8 W/m^2K^4))) R = 1.609 * 10^11 m? · 3 years ago0 1 Comment Maybe you would like to learn more about one of these? Want to build a free website? Interested in dating sites? Need a Home Security Safe? How to order contacts online?']),
            49: CastDoc(doc_id='MARCO_D1256481', title='NIST definition for SaaS, PaaS, IaaS', url='https://cloudinfosec.wordpress.com/2013/05/04/nist-definition-for-saas-paas-iaas/', passages=['Software as a Service (Saa S) — The capability provided to the consumer is to use the provider’s applications running on a cloud infrastructure. The applications are accessible from various client devices through a thin client interface such as a web browser (e.g., web-based email). The consumer does not manage or control the underlying cloud infrastructure including network, servers, operating systems, storage, or even individual application capabilities, with the possible exception of limited user-specific application configuration settings. Examples: Gov-Apps, Internet Services Blogging/Surveys/Twitter, Social Networking Information/Knowledge Sharing (Wiki)Communication (e-mail), Collaboration (e-meeting)Productivity Tools (office)Enterprise Resource Planning (ERP)Platform as a Service (Paa S) — The capability provided to the consumer is to deploy onto the cloud infrastructure consumer-created or acquired applications created using programming languages and tools supported by the provider. The consumer does not manage or control the underlying cloud infrastructure including network, servers, operating systems, or storage, but has control over the deployed applications and possibly application hosting environment configurations. Examples: Application Development, Data, Workflow, etc. Security Services (Single Sign-On, Authentication, etc. )Database Management Directory Services Infrastructure as a Service (Iaa S) — The capability provided to the consumer is to provision processing, storage, networks, and other fundamental computing resources where the consumer is able to deploy and run arbitrary software, which can include operating systems and applications. The consumer does not manage or control the underlying cloud infrastructure but has control over operating systems, storage, deployed applications, and possibly limited control of select networking components (e.g., host firewalls). Examples: Mainframes, Servers, Storage IT Facilities/Hosting Services Advertisements Share this: Twitter Facebook Loading...'])
        })
        
        self._test_docs('trec-cast/v2/passages', count=None, items={
            0: CastPassageDoc(doc_id='MARCO_D1555982-1', title='The hot glowing surfaces of stars emit energy in the form of electromagnetic radiation.?', url='https://answers.yahoo.com/question/index?qid=20071007114826AAwCFvR', text='Science & Mathematics Physics The hot glowing surfaces of stars emit energy in the form of electromagnetic radiation.? It is a good approximation to assume that the emissivity e is equal to 1 for these surfaces. Find the radius of the star Rigel, the bright blue star in the constellation Orion that radiates energy at a rate of 2.7 x 10^32 W and has a surface temperature of 11,000 K. Assume that the star is spherical. Use σ =... show more Follow 3 answers Answers Relevance Rating Newest Oldest Best Answer: Stefan-Boltzmann law states that the energy flux by radiation is proportional to the forth power of the temperature: q = ε · σ · T^4 The total energy flux at a spherical surface of Radius R is Q = q·π·R² = ε·σ·T^4·π·R² Hence the radius is R = √ ( Q / (ε·σ·T^4·π) ) = √ ( 2.7x10+32 W / (1 · 5.67x10-8W/m²K^4 · (1100K)^4 · π) ) = 3.22x10+13 m Source (s):http://en.wikipedia.org/wiki/Stefan_bolt...schmiso · 1 decade ago0 18 Comment Schmiso, you forgot a 4 in your answer. Your link even says it: L = 4pi (R^2)sigma (T^4).'),
            49: CastPassageDoc(doc_id='MARCO_D1311240-14', title='President Roosevelt Led US To Victory In World War 2', url='http://vanrcook.tripod.com/presidentroosevelt.htm', text='President Roosevelt died on April 12, 1945, a little over two weeks before the death of Hitler and a month before Germany surrendered and the European part of World War 2 ended. After his death, Churchill wrote Eleanor: "I have lost a dear and cherished friendship which was forged in the fire of war. "Overall, Roosevelt was a wonderful leader for America. He pulled us through both the great depression of the 30s and the World War 2. He made his share of mistakes. For example, there is no doubt that he waffled on the Jewish problem even as news of German atrocities toward the Jews began to be publicized. However, when you look at how the other American leaders have handled different crisis in the twentieth century, he was, in my opinion, clearly superior to all the others. Web Sites: President Roosevelt In World War 2.1. Germany in World War 2 . Germany fought long and hard in World War 2 but the U. S., Great Britain, and Russia were too smart and tough.2. American Generals - World War 2. Roosevelt had some great generals working with him in World War 2. They had to be great! They were facing fine German generals who knew new warfare tactics, e.g., blitzkrieg.3. Pacific War.')
        })

        self._test_docs('trec-cast/v3', count=None, items={
            0: CastPassageDoc(doc_id='MARCO_00_0-1', title='0-60 Times - 0-60 | 0 to 60 Times & 1/4 Mile Times | Zero to 60 Car Reviews', url='http://0-60.reviews/0-60-times/', text='0-60 Times - 0-60 | 0 to 60 Times & 1/4 Mile Times | Zero to 60 Car Reviews 0-60 Times There are many ways to measure the power a vehicle has – top speed, horsepower, foot-pounds of torque. Those are all important, but the most asked question is, “What’s the 0-60 time?” This is nothing more than a measure of how quickly a vehicle can reach the 60 mile per hour mark. It is a measure of acceleration of a vehicle. 0-60 times differ a great deal depending on the amount of power a motor puts out, of course. But anyone who spends any amount of time with car enthusiasts are sure to hear the ubiquitous term bantered around more often than most other metrics by which cars are measured in terms of power. The only other measure that comes close as far as how acceleration is commonly measures in cars in the United States is the quarter mile time. Enthusiasts will often ask about how quickly a car can get through a quarter mile, but that can be seen as less accurate a estimate of acceleration than the amount of time it takes a vehicle to reach the sixty miler per hour mark. The quarter mile time can often have more variable such as driver experience.'),
            49: CastPassageDoc(doc_id='MARCO_00_44206-10', title='', url='http://001yourtranslationservice.com/kenax/Translators/Resources/TimeZones.htm', text='A year, which works out to 365 days (with some leap years, because our means of creating time is not exact), is the number of times the earth spins around its own axis (creating what we call a day) while the earth revolves in its orbit around the sun to come back to the same place it started at. This is how the Sumerians defined time. But because the world is a round globe, the beginning of night and day is different depending on where you are located in the world. What is high noon for someone in the US would be pitch black midnight for someone in China, on the other side of the world. Therefore, over time, we humans started to draw imaginary lines on the planet, cutting up the planet into 24 parts, one for each hour of the day. In the days of old, people used a sundial to tell the time, which is basically a solar clock. A little stick at a certain angle which would cast a shadow as the sun, from our perspective, would revolve around our planet.')
        })

    def test_queries(self):
        self._test_queries('trec-cast/v0/train', count=269, items={
            0: Cast2019Query('1_1', "What is a physician's assistant?", 1, 1, "Career choice for Nursing and Physician's Assistant", "Considering career options for becoming a physician's assistant vs a nurse.  Discussion topics include required education (including time, cost), salaries, and which is better overall."),
            9: Cast2019Query('1_10', 'Is a PA above a NP?', 1, 10, "Career choice for Nursing and Physician's Assistant", "Considering career options for becoming a physician's assistant vs a nurse.  Discussion topics include required education (including time, cost), salaries, and which is better overall."),
            268: Cast2019Query('30_7', 'Tell me about how I can share files.', 30, 7, 'Linux and Windows', 'A comparison of Windows and Linux, followed by some tips regarding software installation etc.'),
        })
        self._test_queries('trec-cast/v0/train/judged', count=120, items={
            0: Cast2019Query('1_1', "What is a physician's assistant?", 1, 1, "Career choice for Nursing and Physician's Assistant", "Considering career options for becoming a physician's assistant vs a nurse.  Discussion topics include required education (including time, cost), salaries, and which is better overall."),
            9: Cast2019Query('1_10', 'Is a PA above a NP?', 1, 10, "Career choice for Nursing and Physician's Assistant", "Considering career options for becoming a physician's assistant vs a nurse.  Discussion topics include required education (including time, cost), salaries, and which is better overall."),
            119: Cast2019Query('30_7', 'Tell me about how I can share files.', 30, 7, 'Linux and Windows', 'A comparison of Windows and Linux, followed by some tips regarding software installation etc.'),
        })
        self._test_queries('trec-cast/v1/2019', count=479, items={
            0: Cast2019Query('31_1', 'What is throat cancer?', 31, 1, 'head and neck cancer', 'A person is trying to compare and contrast types of cancer in the throat, esophagus, and lungs.'),
            9: Cast2019Query('32_1', 'What are the different types of sharks?', 32, 1, 'sharks', 'Information about sharks including several of the main types of sharks, their biological properties including size (whether they have teeth), as well as adaptations.  This includes difference between sharks and whales.'),
            478: Cast2019Query('80_10', 'What was the impact of the expedition?', 80, 10, 'Lewis and Clark expedition', 'Information about the Lewis and Clark expedition, findings, and its significance in US history.'),
        })
        self._test_queries('trec-cast/v1/2019/judged', count=173, items={
            0: Cast2019Query('31_1', 'What is throat cancer?', 31, 1, 'head and neck cancer', 'A person is trying to compare and contrast types of cancer in the throat, esophagus, and lungs.'),
            9: Cast2019Query('32_1', 'What are the different types of sharks?', 32, 1, 'sharks', 'Information about sharks including several of the main types of sharks, their biological properties including size (whether they have teeth), as well as adaptations.  This includes difference between sharks and whales.'),
            172: Cast2019Query('79_9', 'What are modern examples of conflict theory?', 79, 9, 'sociology', 'Information about the field of sociology including important people, theories, and how they relate to one another.'),
        })
        self._test_queries('trec-cast/v1/2020', count=216, items={
            0: Cast2020Query('81_1', 'How do you know when your garage door opener is going bad?', 'How do you know when your garage door opener is going bad?', 'How do you know when your garage door opener is going bad?', 'MARCO_5498474', 81, 1),
            9: Cast2020Query('82_2', 'What are the pros and cons?', 'What are the pros and cons of GMO Food labeling?', 'What are the pros and cons of GMO food labeling?', 'CAR_bafb3c1c72e23c444e182cac4e0ea9e4330d21c9', 82, 2),
            215: Cast2020Query('105_9', 'What else motivates the Black Lives Matter movement?', 'What else motivates the Black Lives Matter movement?', 'What else motivates the Black Lives Matter movement?', 'MARCO_801480', 105, 9),
        })
        self._test_queries('trec-cast/v1/2020/judged', count=208, items={
            0: Cast2020Query('81_1', 'How do you know when your garage door opener is going bad?', 'How do you know when your garage door opener is going bad?', 'How do you know when your garage door opener is going bad?', 'MARCO_5498474', 81, 1),
            9: Cast2020Query('82_2', 'What are the pros and cons?', 'What are the pros and cons of GMO Food labeling?', 'What are the pros and cons of GMO food labeling?', 'CAR_bafb3c1c72e23c444e182cac4e0ea9e4330d21c9', 82, 2),
            207: Cast2020Query('105_9', 'What else motivates the Black Lives Matter movement?', 'What else motivates the Black Lives Matter movement?', 'What else motivates the Black Lives Matter movement?', 'MARCO_801480', 105, 9),
        })
        self._test_queries('trec-cast/v2/2021', count=239, items={
            0: Cast2021Query('106_1', 'I just had a breast biopsy for cancer. What are the most common types?', 'What are the most common types of cancer in regards to breast biopsy?', 'I just had a breast biopsy for cancer. What are the most common types of breast cancer?', 'MARCO_D59865', 106, 1),
            9: Cast2021Query('106_10', 'Does freezing work?', 'Does freezing work for breast cancer?', 'Does freezing tumors work as an alternative to surgery for stage 1 invasive lobular cancer?', 'MARCO_D909677', 106, 10),
            238: Cast2021Query('131_10', 'How is it different from a heat pump?', 'How is an AC compressor different from a heat pump?', 'How is an AC system different from a heat pump?', 'MARCO_D981398', 131, 10),
        })
        self._test_queries('trec-cast/v3/2022', count=408, items={
            0: Cast2022Query('132_1-1', None, 'User', 'I remember Glasgow hosting COP26 last year, but unfortunately I was out of the loop. What was it about?', 'I remember Glasgow hosting COP26 last year, but unfortunately I was out of the loop. What was the conference about?', None, [], 132, '1-1'),
            9: Cast2022Query(query_id='132_2-2', parent_id='132_2-1', participant='System', raw_utterance=None, manual_rewritten_utterance=None, response='For several years, there have been concerns that climate change negotiations will essentially ignore a key principle of climate change negotiation frameworks: the common but differentiated responsibilities. Realizing that greenhouse emissions remain in the atmosphere for a very long time, this principle recognizes that historically: Industrialized nations have emitted far more greenhouse gas emissions (even if some developing nations are only now increasing theirs); Rich countries, therefore, face the biggest responsibility and burden for action to address climate change; and Rich countries, therefore, must support developing nations adapt—through financing and technology transfer, for example. This notion of climate justice is typically ignored by many rich nations and their mainstream media, making it easy to blame China, India and other developing countries for failures in climate change mitigation negotiations.', provenance=['MARCO_06_772219573-9'], topic_number=132, turn_number='2-2'),
            407: Cast2022Query(query_id='149_4-2', parent_id='149_4-1', participant='System', raw_utterance=None, manual_rewritten_utterance=None, response='Consider using a number of different search engines. While Google is a globally recognized search engine and an industry giant, in fact, even the second biggest “search engine” is Google Images, according to this study: Search engine market share Even if it’s the biggest and most well known, it doesn’t mean it’s your only choice. One of the main reasons that people choose to use an alternative search engine instead is for increased privacy, as Google is known to track user data both for its own and third-party use. If you’ve only ever used Google, check out some of the other search engines and you might find something that you prefer. Bing Bing search engine Microsoft’s Bing is the second largest search engine after Google. It’s easy to use and provides a more visual experience with beautiful daily background photos. Bing is great for video searches, as it displays results as large thumbnails that can be previewed with sound by hovering over them. DuckDuckGo is a popular search engine for those who value their privacy and are put off by the thought of their every query being tracked and logged.It has a very clean interface with minimal ads and infinite scrolling, so the user experience is nice and streamlined. There’s absolutely zero user tracking, and you can even add DuckDuckGo’s extension to your browser to keep your activity private.', provenance=['MARCO_23_183358734-1', 'MARCO_23_183358734-2', 'MARCO_23_183358734-31'], topic_number=149, turn_number='4-2'),
        })


    def test_qrels(self):
        self._test_qrels('trec-cast/v0/train', count=2399, items={
            0: TrecQrel('1_1', 'MARCO_955948', 2, '0'),
            9: TrecQrel('1_1', 'MARCO_4903530', 0, '0'),
            2398: TrecQrel('30_7', 'MARCO_4250016', 0, '0'),
        })
        self._test_qrels('trec-cast/v0/train/judged', count=2399, items={
            0: TrecQrel('1_1', 'MARCO_955948', 2, '0'),
            9: TrecQrel('1_1', 'MARCO_4903530', 0, '0'),
            2398: TrecQrel('30_7', 'MARCO_4250016', 0, '0'),
        })
        self._test_qrels('trec-cast/v1/2019', count=29350, items={
            0: TrecQrel('31_1', 'CAR_116d829c4c800c2fc70f11692fec5e8c7e975250', 0, 'Q0'),
            9: TrecQrel('31_1', 'CAR_40c64256e988c8103550008f4e9b7ce436d9536d', 2, 'Q0'),
            29349: TrecQrel('79_9', 'MARCO_8795237', 3, 'Q0'),
        })
        self._test_qrels('trec-cast/v1/2019/judged', count=29350, items={
            0: TrecQrel('31_1', 'CAR_116d829c4c800c2fc70f11692fec5e8c7e975250', 0, 'Q0'),
            9: TrecQrel('31_1', 'CAR_40c64256e988c8103550008f4e9b7ce436d9536d', 2, 'Q0'),
            29349: TrecQrel('79_9', 'MARCO_8795237', 3, 'Q0'),
        })
        self._test_qrels('trec-cast/v1/2020', count=40451, items={
            0: TrecQrel('81_1', 'CAR_3add84966af079ed84e8b2fc412ad1dc27800127', 1, '0'),
            9: TrecQrel('81_1', 'MARCO_1381086', 1, '0'),
            40450: TrecQrel('105_9', 'MARCO_8757526', 0, '0'),
        })
        self._test_qrels('trec-cast/v1/2020/judged', count=40451, items={
            0: TrecQrel('81_1', 'CAR_3add84966af079ed84e8b2fc412ad1dc27800127', 1, '0'),
            9: TrecQrel('81_1', 'MARCO_1381086', 1, '0'),
            40450: TrecQrel('105_9', 'MARCO_8757526', 0, '0'),
        })
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

    def test_scoreddocs(self):
        self._test_scoreddocs('trec-cast/v0/train', count=269000, items={
            0: GenericScoredDoc('1_1', 'MARCO_955948', -5.32579),
            9: GenericScoredDoc('1_1', 'CAR_87772d4208721133d00d7d62f4eaaf164da5b4e3', -5.44505),
            268999: GenericScoredDoc('30_7', 'WAPO_595c1be2ba9e3b1e66d552a174219c12-3', -7.07828),
        })
        self._test_scoreddocs('trec-cast/v0/train/judged', count=120000, items={
            0: GenericScoredDoc('1_1', 'MARCO_955948', -5.32579),
            9: GenericScoredDoc('1_1', 'CAR_87772d4208721133d00d7d62f4eaaf164da5b4e3', -5.44505),
            119999: GenericScoredDoc('30_7', 'WAPO_595c1be2ba9e3b1e66d552a174219c12-3', -7.07828),
        })
        self._test_scoreddocs('trec-cast/v1/2019', count=479000, items={
            0: GenericScoredDoc('31_1', 'MARCO_789620', -5.71312),
            9: GenericScoredDoc('31_1', 'MARCO_291004', -5.88053),
            478999: GenericScoredDoc('80_10', 'CAR_268dcb1c6bc4326f81500513e0ad9d11acb2a693', -5.23496),
        })
        self._test_scoreddocs('trec-cast/v1/2019/judged', count=173000, items={
            0: GenericScoredDoc('31_1', 'MARCO_789620', -5.71312),
            9: GenericScoredDoc('31_1', 'MARCO_291004', -5.88053),
            172999: GenericScoredDoc('79_9', 'MARCO_1431776', -6.75024),
        })


if __name__ == '__main__':
    unittest.main()
