import re
import unittest
from ir_datasets.datasets.wapo import WapoDoc, WapoDocMedia, TrecBackgroundLinkingQuery
from ir_datasets.formats import GenericQrel, GenericQuery, TrecQuery, TrecQrel
from .base import DatasetIntegrationTest


class TestWapo(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('wapo/v2', count=595037, items={
            0: WapoDoc('b2e89334-33f9-11e1-825f-dabc29fd7071', 'https://www.washingtonpost.com/sports/colleges/danny-coale-jarrett-boykin-are-a-perfect-1-2-punch-for-virginia-tech/2011/12/31/gIQAAaW4SP_story.html', re.compile('^Danny Coale, Jarrett .* Virginia Tech$', flags=48), 'Mark Giannotto', 1325376562000, 'Colleges', re.compile('^Virginia Tech wide receiver Danny Coale \\(19\\) was lightly recruited out of Episcopal in Alexandria bu.{2838} on him and he’ll be there\\. I know when I look back, part of my Tech experience is going to be him\\.”$', flags=48), (re.compile('<span class="dateline">NEW.*North Carolina comes up.', flags=48),re.compile('Midway through the first.*position coach, said recently.', flags=48),re.compile('Now that Boykin and.*taken a different stance.', flags=48),re.compile('“I still don’t think.*him nodding in agreement.', flags=48),re.compile('Just add that to.*have had to overcome.', flags=48),re.compile('Boykin has been <a.*any other Hokies receiver.', flags=48),re.compile('Coale, an Episcopal.*to be open.”', flags=48),re.compile('And yet neither warranted.*participated in the voting.', flags=48),re.compile('In retrospect, Boykin.*a first-year quarterback.', flags=48),re.compile('“There’s just some things.*up there with them.', flags=48),re.compile('“It’s great playing wide.*and 22 touchdowns.”', flags=48),re.compile('The other issue is.*wide receivers these days.', flags=48),re.compile('Coale has graduated with.*field or in games.', flags=48),re.compile('Coming out of high.*under the radar.”', flags=48),re.compile('But their accomplishments haven’t.*ball,” he said.', flags=48),re.compile('Years of lining up.*championship game this year.', flags=48),re.compile('Boykin was supposed to.*to take his place.', flags=48),re.compile('“I’ve been through his.*to be him.”', flags=48)),(WapoDocMedia(type='image', url=re.compile('https://img.washingtonpost.com.*Images/Virginia_Tech_Georgia_Tech_Football_0cf3f.jpg', flags=48), text=re.compile('Virginia Tech wide receiver.*John Bazemore/AP', flags=48)),)),
            9: WapoDoc('153127ee-341e-11e1-825f-dabc29fd7071', 'https://www.washingtonpost.com/sports/capitals/capitals-vs-blue-jackets-alex-ovechkin-scores-twice-as-washington-wins/2011/12/31/gIQA1deHTP_story.html', re.compile('^Capitals vs. Blue .* Washington wins$', flags=48), 'Katie Carrera', 1325387427000, 'Capitals/NHL', re.compile("^Columbus goalie Steve Mason makes a save as Grant Clitsome \\(14\\) and Washington's Brooks Laich \\(21\\) l.{3476} the group into making sure it didn’t let the week of progress unravel in one night\\.\n\nCapitals note:$", flags=48), (re.compile('<span class="dateline">COLUMBUS.*team in the NHL.', flags=48),re.compile('A furious third-period.*winning streak since October.', flags=48),re.compile('Three goals in the.*entering the third period.', flags=48),re.compile('The victory improved the.*a> in ninth place.', flags=48),re.compile('“For a while here.*need to be.”', flags=48),re.compile('Columbus, which has.*at the Blue Jackets.', flags=48),re.compile('“We had nothing to.*credit to them.”', flags=48),re.compile('Hunter wanted pressure from.*slap pass from Wideman.', flags=48),re.compile('The defenseman set up.*for matching roughing minors.', flags=48),re.compile('Twenty-eight seconds later.*as the game-winner.', flags=48),re.compile('Ovechkin added his second.*the past eight games.', flags=48),re.compile('“Everybody was upset how.*on our side.”', flags=48),re.compile('Until the final period.*minutes of scoreless action.', flags=48),re.compile('Early in the game.*in as many nights.', flags=48),re.compile('As the first period.*in a scoreless tie.', flags=48),re.compile('In the second,.*47 of the second.', flags=48),re.compile('Samuel Pahlsson made it.*unravel in one night.', flags=48),re.compile('“It’s a big win.*we can get.”', flags=48),re.compile('<b>Capitals note:</b.*on-ice workout Saturday.', flags=48)),(WapoDocMedia(type='image', url=re.compile('https://img.washingtonpost.com.*Images/Capitals_Blue_Jackets_Hockey_0ed19.jpg', flags=48), text=re.compile("Columbus goalie Steve Mason.*Jay LaPrete/Associated Press", flags=48)),WapoDocMedia(type='image', url=re.compile('https://img.washingtonpost.com.*Images/Capitals_Blue_Jackets_Hockey_09796.jpg', flags=48), text=re.compile("Washington's Dmitry Orlov.*Jay LaPrete/Associated Press", flags=48)))),
            595036: WapoDoc('fffbc452-b422-11e2-9a98-4be1688d7d84', 'https://www.washingtonpost.com/business/capitalbusiness/gwu-studentshelp-small-groups-raise-money/2013/05/05/fffbc452-b422-11e2-9a98-4be1688d7d84_story.html', re.compile('^GWU students .* raise money$', flags=48), 'Vanessa Small', 1367797043000, 'Capital Business', re.compile('^Dylan Fox, right, is founder and chief executive of Crowdvance, a new online fundraising platform\\. H.{3696}d,” Fox said\\. “I think you can only be successful if you’re not only creating a profit but a value\\.”$', flags=48), (re.compile('While most students at.*approval from business executives.', flags=48),re.compile('The student entrepreneur started.*its creation last September.', flags=48),re.compile('Crowdvance recently won first.*values and social impact.', flags=48),re.compile('Now Crowdvance is competing.*incentives to attract givers.', flags=48),re.compile('Crowdvance is something like.*enter a national competition.', flags=48),re.compile('When making a donation.*com and Ice.com.', flags=48),re.compile('Fox .* revenue.', flags=48),re.compile('“Small organizations can’t generate.*burns out donors.”', flags=48),re.compile('Crowdvance’s biggest success stories.*to be registered nonprofits.', flags=48),re.compile('Fox has been an.*to students each month.', flags=48),re.compile('Then one afternoon last.*became the marketing officer.', flags=48),re.compile('Fox, who spent.*of raising seed money.', flags=48),re.compile('In between their busy.*to start social ventures.', flags=48),re.compile('“I don’t want to.*but a value.”', flags=48)),(WapoDocMedia(type='image', url=re.compile('https://img.washingtonpost.com.*Images/IMG_93451367464507.JPG', flags=48), text=re.compile('Dylan Fox, right.*THE WASHINGTON POST', flags=48)),)),
        })

    def test_queries(self):
        self._test_queries('wapo/v2/trec-core-2018', count=50, items={
            0: TrecQuery('321', 'Women in Parliaments', 'Pertinent documents will reflect the fact that women continue to be poorly represented in parliaments across the world, and the gap in political power between the sexes is very wide, particularly in the Third World.', 'Pertinent documents relating to this issue will discuss the lack of representation by women, the countries that mandate the inclusion of a certain percentage of women in their legislatures, decreases if any in female representation in legislatures, and those countries in which there is no representation of women.'),
            9: TrecQuery('378', 'euro opposition', 'Identify documents that discuss opposition to the use of the euro, the European currency.', 'A relevant document should include the countries or individuals who oppose the use of the euro and the reason(s) for their opposition to its use.'),
            49: TrecQuery('825', 'ethanol and food prices', 'Does diversion of U.S. corn crops into ethanol for fuel increase food prices?', 'Identify documents that discuss the impact of growing corn with the intention of using it for ethanol fuel on food prices in the U.S.'),
        })
        self._test_queries('wapo/v2/trec-news-2018', count=50, items={
            0: TrecBackgroundLinkingQuery('321', '9171debc316e5e2782e0d2404ca7d09d', 'https://www.washingtonpost.com/news/worldviews/wp/2016/09/01/women-are-half-of-the-world-but-only-22-percent-of-its-parliaments/<url>'),
            9: TrecBackgroundLinkingQuery('378', '3c5be31e-24ab-11e5-b621-b55e495e9b78', 'https://www.washingtonpost.com/world/europe/to-greeks-german-offers-of-help-sound-more-like-a-threat/2015/07/07/3c5be31e-24ab-11e5-b621-b55e495e9b78_story.html<url>'),
            49: TrecBackgroundLinkingQuery('825', 'a1c41a70-35c7-11e3-8a0e-4e2cf80831fc', 'https://www.washingtonpost.com/business/economy/cellulosic-ethanol-once-the-way-of-the-future-is-off-to-a-delayed-boisterous-start/2013/11/08/a1c41a70-35c7-11e3-8a0e-4e2cf80831fc_story.html'),
        })
        self._test_queries('wapo/v2/trec-news-2019', count=60, items={
            0: TrecBackgroundLinkingQuery('826', '96ab542e-6a07-11e6-ba32-5a4bf5aad4fa', 'https://www.washingtonpost.com/sports/nationals/the-minor-leagues-life-in-pro-baseballs-shadowy-corner/2016/08/26/96ab542e-6a07-11e6-ba32-5a4bf5aad4fa_story.html'),
            9: TrecBackgroundLinkingQuery('835', 'c0c4e2d0-628f-11e7-a4f7-af34fc1d9d39', 'https://www.washingtonpost.com/local/social-issues/a-healthy-mystery-over-attending-houses-of-worship/2017/07/07/c0c4e2d0-628f-11e7-a4f7-af34fc1d9d39_story.html'),
            59: TrecBackgroundLinkingQuery('885', '5ae44bfd66a49bcad7b55b29b55d63b6', 'https://www.washingtonpost.com/news/capital-weather-gang/wp/2017/07/14/sun-erupts-to-mark-another-bastille-day-aurora-possible-in-new-england-sunday-night/'),
        })
        self._test_queries('wapo/v3/trec-news-2020', count=50, items={
            0: TrecBackgroundLinkingQuery('886', 'AEQZNZSVT5BGPPUTTJO7SNMOLE', 'https://www.washingtonpost.com/politics/2019/06/05/trump-says-transgender-troops-cant-serve-because-troops-cant-take-any-drugs-hes-wrong-many-ways/'),
            9: TrecBackgroundLinkingQuery('895', '615f0d53ac8f1e05c51bfebf4fdaf0e5', 'https://www.washingtonpost.comhttps://www.washingtonpost.com/news/to-your-health/wp/2015/04/09/sabra-pulls-30000-cases-of-hummus-off-store-shelves-due-to-listeria-fears/'),
            49: TrecBackgroundLinkingQuery('935', 'CCUJNXOJNFEJFBL57GD27EHMWI', 'https://www.washingtonpost.com/news/to-your-health/wp/2018/05/30/this-mock-pandemic-killed-150-million-people-next-time-it-might-not-be-a-drill/'),
        })

    def test_qrels(self):
        self._test_qrels('wapo/v2/trec-core-2018', count=26233, items={
            0: TrecQrel('321', '004c6120d0aa69da29cc045da0562168', 0, '0'),
            9: TrecQrel('321', '01664d72845d37c958a504b9b4085883', 0, '0'),
            26232: TrecQrel('825', 'ff3a25b0-0ba4-11e4-8341-b8072b1e7348', 0, '0'),
        })
        self._test_qrels('wapo/v2/trec-news-2018', count=8508, items={
            0: TrecQrel('321', '00f57310e5c8ec7833d6756ba637332e', 16, '0'),
            9: TrecQrel('321', '09b3167f0d1aa5cfa8be932bb704d75a', 8, '0'),
            8507: TrecQrel('825', 'f66b624ba8689d704872fa776fb52860', 0, '0'),
        })
        self._test_qrels('wapo/v2/trec-news-2019', count=15655, items={
            0: TrecQrel('826', '0154349511cd8c49ab862d6cb0d8f6a8', 2, '0'),
            9: TrecQrel('826', '054be3904bde907f71d684b268e2273d', 0, '0'),
            15654: TrecQrel('885', 'fde80cb0-b4f0-11e2-bbf2-a6f9e9d79e19', 0, '0'),
        })
        self._test_qrels('wapo/v3/trec-news-2020', count=17764, items={
            0: TrecQrel('886', '00183d98-741b-11e5-8248-98e0f5a2e830', 0, '0'),
            9: TrecQrel('886', '03c3c222-0e01-11e4-8c9a-923ecc0c7d23', 0, '0'),
            17763: TrecQrel('935', 'ff0a760128ecdbcc096cafc8cd553255', 0, '0'),
        })


if __name__ == '__main__':
    unittest.main()
