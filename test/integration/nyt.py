import re
import unittest
from ir_datasets.datasets.nyt import NytDoc
from ir_datasets.formats import GenericQrel, GenericQuery, TrecQuery, TrecQrel
from .base import DatasetIntegrationTest


class TestNyt(DatasetIntegrationTest):
    def test_nyt_docs(self):
        self._test_docs('nyt', count=1864661, items={
            0: NytDoc('8454', 'MARSH & MCLENNAN INC reports earnings for Qtr to Dec 31', re.compile('^LEAD:\n\\*3\\*\\*\\* COMPANY REPORTS \\*\\*\n\\*3\\*MARSH \\& MCLENNAN INC \\(NYSE\\)\nQtr to Dec 31\n1986\n1985\nRevenue\n444,70.{307}rns\n\\.67\n\\.50\nYr rev\n1,804,100,000\n1,367,600 000\nNet inc\n243,200,000\n162,900 000\nShare earns\n3\\.30\n2\\.23$', flags=48), re.compile(b'^<\\?xml version="1\\.0" encoding="UTF\\-8"\\?>\n<!DOCTYPE nitf SYSTEM "http://www\\.nitf\\.org/IPTC/NITF/3\\.3/spec.{3597}ns</p>\n        <p>3\\.30</p>\n        <p>2\\.23</p>\n      </block>\n    </body\\.content>\n  </body>\n</nitf>\n$', flags=16)),
            9: NytDoc('8579', 'SALINGER BIOGRAPHY IS BLOCKED', re.compile('^LEAD: A biography of J\\. D\\. Salinger was blocked yesterday by a Federal appeals court in Manhattan th.{6380}ers in his lifetime, the appeals court said he was entitled to protect his opportunity to sell them\\.$', flags=48), re.compile(b'^<\\?xml version="1\\.0" encoding="UTF\\-8"\\?>\n<!DOCTYPE nitf SYSTEM "http://www\\.nitf\\.org/IPTC/NITF/3\\.3/spec.{9976}d to protect his opportunity to sell them\\.</p>\n      </block>\n    </body\\.content>\n  </body>\n</nitf>\n$', flags=16)),
            1864660: NytDoc('1854817', 'STRATEGY ON IRAN STIRS NEW DEBATE AT WHITE HOUSE', re.compile("^A year after President Bush and Secretary of State Condoleezza Rice announced a new strategy toward .{8110}, who left his post partly over his opposition to the administration's recent deal with North Korea\\.$", flags=48), re.compile(b'^<\\?xml version="1\\.0" encoding="UTF\\-8"\\?>\n<!DOCTYPE nitf SYSTEM "http://www\\.nitf\\.org/IPTC/NITF/3\\.3/spec.{17438}nistration\'s recent deal with North Korea\\.</p>\n      </block>\n    </body\\.content>\n  </body>\n</nitf>\n$', flags=16)),
        })

    def test_nyt_queries(self):
        self._test_queries('nyt/wksup', count=1864661, items={
            0: GenericQuery('8454', 'MARSH & MCLENNAN INC reports earnings for Qtr to Dec 31'),
            9: GenericQuery('8579', 'SALINGER BIOGRAPHY IS BLOCKED'),
            1864660: GenericQuery('1854817', 'STRATEGY ON IRAN STIRS NEW DEBATE AT WHITE HOUSE'),
        })
        self._test_queries('nyt/wksup/train', count=1863657, items={
            0: GenericQuery('8454', 'MARSH & MCLENNAN INC reports earnings for Qtr to Dec 31'),
            9: GenericQuery('8579', 'SALINGER BIOGRAPHY IS BLOCKED'),
            1863656: GenericQuery('1854817', 'STRATEGY ON IRAN STIRS NEW DEBATE AT WHITE HOUSE'),
        })
        self._test_queries('nyt/wksup/valid', count=1004, items={
            0: GenericQuery('6461', "Why We're Forced To Be Slumlords"),
            9: GenericQuery('13148', 'NOVAMETRIX MEDICAL SYSTEMS INC reports earnings for Qtr to Dec 31'),
            1003: GenericQuery('1854529', 'The Newest Antique: Atari'),
        })
        self._test_queries('nyt/trec-core-2017', count=50, items={
            0: TrecQuery('307', 'New Hydroelectric Projects', 'Identify hydroelectric projects proposed or under construction by country and location. Detailed description of nature, extent, purpose, problems, and consequences is desirable.', 'Relevant documents would contain as a minimum a clear statement that a hydroelectric project is planned or construction is under way and the location of the project. Renovation of existing facilities would be judged not relevant unless plans call for a significant increase in acre-feet or reservoir or a marked change in the environmental impact of the project. Arguments for and against proposed projects are relevant as long as they are supported by specifics, including as a minimum the name or location of the project. A statement that an individual or organization is for or against such projects in general would not be relevant. Proposals or projects underway to dismantle existing facilities or drain existing reservoirs are not relevant, nor are articles reporting a decision to drop a proposed plan.'),
            9: TrecQuery('347', 'Wildlife Extinction', 'The spotted owl episode in America highlighted U.S. efforts to prevent the extinction of wildlife species. What is not well known is the effort of other countries to prevent the demise of species native to their countries. What other countries have begun efforts to prevent such declines?', 'A relevant item will specify the country, the involved species, and steps taken to save the species.'),
            49: TrecQuery('690', 'college education advantage', 'Find documents which describe an advantage in hiring potential or increased income for graduates of U.S. colleges.', 'Relevant documents cite some advantage of a college education for job opportunities. Documents citing better opportunities for non-college vocational-training is not relevant.'),
        })

    def test_nyt_qrels(self):
        self._test_qrels('nyt/wksup', count=1864661, items={
            0: GenericQrel('8454', '8454', 1),
            9: GenericQrel('8579', '8579', 1),
            1864660: GenericQrel('1854817', '1854817', 1),
        })
        self._test_qrels('nyt/wksup/train', count=1863657, items={
            0: GenericQrel('8454', '8454', 1),
            9: GenericQrel('8579', '8579', 1),
            1863656: GenericQrel('1854817', '1854817', 1),
        })
        self._test_qrels('nyt/wksup/valid', count=1004, items={
            0: GenericQrel('6461', '6461', 1),
            9: GenericQrel('13148', '13148', 1),
            1003: GenericQrel('1854529', '1854529', 1),
        })
        self._test_qrels('nyt/trec-core-2017', count=30030, items={
            0: TrecQrel('307', '1001536', 1, '0'),
            9: TrecQrel('307', '1029429', 1, '0'),
            30029: TrecQrel('690', '996059', 0, '0'),
        })


if __name__ == '__main__':
    unittest.main()
