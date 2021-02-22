import re
import unittest
import ir_datasets
from ir_datasets.datasets.gov2 import Gov2Doc
from ir_datasets.formats import TrecQrel, TrecQuery, GenericQuery, TrecPrel
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestGov2(DatasetIntegrationTest):
    def test_gov2_docs(self):
        self._test_docs('gov2', items={
            0: Gov2Doc('GX000-00-0000000', 'http://sgra.jpl.nasa.gov', re.compile('^HTTP/1\\.1 200 OK\\\r\nDate: Tue, 09 Dec 2003 21:21:33 GMT\\\r\nServer: Apache/1\\.3\\.27 \\(Unix\\)\\\r\nLast\\-Modified: T.{45}6\\-3ca0cae9"\\\r\nAccept\\-Ranges: bytes\\\r\nContent\\-Length: 614\\\r\nConnection: close\\\r\nContent\\-Type: text/html\\\r\n$', flags=48), re.compile(b'^<html>\n\n<head>\n<title>\nJPL Sgra web Site\n</title>\n</head>\n\n<body bgcolor="\\#fffff">\n\n<hr>\n<h1>\n<cente.{415}roject science web page </a>\n</ul>\n\n<hr>\nLast updated: Thu Sep 16 17:24:48 PDT 1999 \n<hr>\n\n</body>\n\n$', flags=16), 'text/html'),
            9: Gov2Doc('GX000-00-0109156', 'http://gsbca2.gsa.gov', re.compile('^HTTP/1\\.1 200 OK\\\r\nServer: Netscape\\-Enterprise/3\\.6 SP3\\\r\nDate: Sun, 15 Jun 2003 01:36:47 GMT\\\r\nContent\\-t.{6}ext/html\\\r\nLast\\-modified: Wed, 16 Apr 2003 19:30:56 GMT\\\r\nContent\\-length: 5063\\\r\nAccept\\-ranges: bytes\\\r\n$', flags=48), re.compile(b'^<!doctype html public "\\-//w3c//dtd html 4\\.0 transitional//en">\n<html>\n<head>\n   <meta http\\-equiv="Co.{4864}eight=32 width=56></a>\n<br><font size=\\+1>Go to Archived Protests Index Page</font>\n</body>\n</html>\n\n$', flags=16), 'text/html'),
            2000: Gov2Doc('GX000-01-14704660', 'http://msl.jpl.nasa.gov/add/intro.html', 'HTTP/1.1 200 OK\r\nDate: Tue, 09 Dec 2003 21:31:58 GMT\r\nServer: Apache/1.3.28 (Darwin)\r\nConnection: close\r\nContent-Type: text/html\r\n', re.compile(b'^<HTML><HEAD><TITLE>Content Submission Guidelines</TITLE></HEAD>\n\n<BODY BACKGROUND="\\.\\./images/silverB.{4221}images/blank\\.gif" WIDTH="1" HEIGHT="1" HSPACE="27" VSPACE="200"></TD>\n</TR></TABLE>\n</BODY></HTML>\n\n$', flags=16), 'text/html'),
        })

    def test_gov2_docstore(self):
        docstore = ir_datasets.load('gov2').docs_store()
        docstore.clear_cache()
        with _logger.duration('cold fetch'):
            docstore.get_many(['GX269-06-1933735', 'GX269-06-16539507', 'GX002-04-0481202'])
        with _logger.duration('warm fetch'):
            docstore.get_many(['GX269-06-1933735', 'GX269-06-16539507', 'GX002-04-0481202'])
        docstore = ir_datasets.load('gov2').docs_store()
        with _logger.duration('warm fetch (new docstore)'):
            docstore.get_many(['GX269-06-1933735', 'GX269-06-16539507', 'GX002-04-0481202'])
        with _logger.duration('cold fetch (nearby)'):
            docstore.get_many(['GX269-06-16476479', 'GX269-06-1939325', 'GX002-04-0587205'])
        with _logger.duration('cold fetch (earlier)'):
            docstore.get_many(['GX269-06-0125294', 'GX002-04-0050816'])

    def test_gov2_queries(self):
        self._test_queries('gov2/trec-tb-2004', count=50, items={
            0: TrecQuery('701', 'U.S. oil industry history', 'Describe the history of the U.S. oil industry', 'Relevant documents will include those on historical exploration and\ndrilling as well as history of regulatory bodies. Relevant are history\nof the oil industry in various states, even if drilling began in 1950\nor later.'),
            9: TrecQuery('710', 'Prostate cancer treatments', 'What are the various treatments for prostate cancer?', 'Relevant cancer treatments include radiation therapy, radioactive\npellets, hormonal therapy and surgery. "Watchful waiting" is also\nconsidered relevant.'),
            49: TrecQuery('750', 'John Edwards womens issues', "What are Senator John Edwards' positions on women's issues such as pay\nequity, abortion, Title IX and violence against women.", "Relevant documents will indicate Senator John Edwards' stand on issues\nconcerning women, such as pay parity, abortion rights, Title IX, and\nviolence against women.  Lists of press releases are relevant when the\nheadlines show he is voting for or against bills on women's\nissues. Not relevant are Edwards' positions on issues not exclusively\nconcerning women."),
        })
        self._test_queries('gov2/trec-tb-2005', count=50, items={
            0: TrecQuery('751', 'Scrabble Players', 'Give information on Scrabble players, when and where Scrabble is\nplayed, and how popular it has been.', "Give information on the social aspects of the game Scrabble. Scrabble\nplayers may be named or described as a group.  Both real and fictional\nplayers are relevant. Mention of a scheduled Scrabble game is\nrelevant. Scrabble's popularity is relevant.  An account of a\nparticular game is relevant.  Descriptions of variants on the Scrabble\ngame are not relevant. Use of Scrabble tiles for other purposes are\nnot relevant. Scrabble software is not relevant unless there is\nmention of its users.  Titles of Scrabble-related books (dictionaries,\nglossaries, rulebooks) are not relevant."),
            9: TrecQuery('760', 'american muslim mosques schools', 'Statistics regarding American Muslims, mosques, and schools.', 'Relevant documents should provide some count or proportion of mosques,\nMuslim-affiliated schools, or population. With regard to population,\nspecific age groupings, sexes, or other categorizations are\nacceptable. The statistics can be pertinent to a specific geographic\narea, such as Fulton County, the state of California, or the\nNortheast.  There is no restriction as to time period (for example\n2005 versus 1987).'),
            49: TrecQuery('800', 'Ovarian Cancer Treatment', 'The remedies and treatments given to lesson or stop effects of ovarian\ncancer.', 'Relevant documents must include names of chemicals or medicines used\nto fight ovarian cancer. Studies of new treatments that are being\ntried are valid, even if they have not reached a conclusion as to\neffectiveness.'),
        })
        self._test_queries('gov2/trec-tb-2006', count=50, items={
            0: TrecQuery('801', 'Kudzu Pueraria lobata', 'Describe the origin, nature, extent of spread and means of controlling\nkudzu.', 'Identification of kudzu as an invasive species with description of how\nit spreads and grows is relevant.  A document which is simply a list\nheaded "invasive species" or "noxious weeds" including kudzu is not\nrelevant.  A statement that kudzu is present in a specific location is\nnot relevant unless it relates to its spread.  Features of kudzu such\nas its use as a treatment for alcoholism or its function as a haven\nfor plant pathogens describe its nature and are relevant.'),
            9: TrecQuery('810', 'timeshare resales', 'Provide information regarding timeshare resales.', 'Relevant documents will include those describing the prospects of\nreselling a timeshare and the pitfalls one should be aware of when\nselling a timeshare.  Real estate legislature regarding the resale of\ntimeshares is not relevant.'),
            49: TrecQuery('850', 'Mississippi River flood', 'How frequently does the Mississippi River flood its banks?', 'Flooding is a relative term which implies water overflowing its\ncontainer and causing damage to the surrounding ares.  Documents are\nrelevant if they describe Mississippi River events which are commonly\nconsidered to be floods.  Relevant documents may also show how such\nevents have led to the introduction of controls to lessen the\nfrequency of damaging floods of this river.  Relevant documents\ninclude different levels of flooding, not only the major ones.\nDocuments are not relevant if they are essentially forecasts or\nroutine reports of water levels.  They are also not relevant if they\nare purely bibliographies or lists of sources for relevant documents.\nPhotos and videos of floods alone are not relevant.'),
        })
        self._test_queries('gov2/trec-tb-2005/named-page', count=252, items={
            0: GenericQuery('601', 'metallurgy division world war history'),
            9: GenericQuery('610', 'united states vs david j. kaiser transcript court appeal'),
            251: GenericQuery('872', 'medical advisory committee memorandum a rule to exclude idet'),
        })
        self._test_queries('gov2/trec-tb-2005/efficiency', count=50000, items={
            0: GenericQuery('1', 'pierson s twin lakes marina'),
            9: GenericQuery('10', 'hotel meistertrunk'),
            49999: GenericQuery('50000', 'senator durbin'),
        })
        self._test_queries('gov2/trec-tb-2006/named-page', count=181, items={
            0: GenericQuery('901', 'CCAP advance case search'),
            9: GenericQuery('910', 'HS project "It\'s not easy being green"'),
            180: GenericQuery('1081', 'Colleges in PA'),
        })
        self._test_queries('gov2/trec-tb-2006/efficiency', count=100000, items={
            0: GenericQuery('1', 'commissioner of revenue orange county virginia'),
            9: GenericQuery('10', 'terrorism policies in history'),
            99999: GenericQuery('100000', 'cervical flexion extension injury'),
        })
        self._test_queries('gov2/trec-tb-2006/efficiency/10k', count=10000, items={
            0: GenericQuery('1', 'commissioner of revenue orange county virginia'),
            9: GenericQuery('10', 'terrorism policies in history'),
            9999: GenericQuery('10000', 'gdp of international business in bermuda'),
        })
        self._test_queries('gov2/trec-tb-2006/efficiency/stream1', count=25000, items={
            0: GenericQuery('1', 'commissioner of revenue orange county virginia'),
            9: GenericQuery('10', 'terrorism policies in history'),
            24999: GenericQuery('25000', 'organized crime in columbus ohio'),
        })
        self._test_queries('gov2/trec-tb-2006/efficiency/stream2', count=25000, items={
            0: GenericQuery('25001', 'pea ridge national park'),
            9: GenericQuery('25010', 'nylon concrete'),
            24999: GenericQuery('50000', 'mark wallace bush and u.n.'),
        })
        self._test_queries('gov2/trec-tb-2006/efficiency/stream3', count=25000, items={
            0: GenericQuery('50001', "dept veteran's affairs connecticut"),
            9: GenericQuery('50010', 'nuclear & missile cold war'),
            24999: GenericQuery('75000', 'the role and responsibilities of the u.s. senate'),
        })
        self._test_queries('gov2/trec-tb-2006/efficiency/stream4', count=25000, items={
            0: GenericQuery('75001', 'united states office of personel management'),
            9: GenericQuery('75010', 'percentage of youth tobacco smokers'),
            24999: GenericQuery('100000', 'cervical flexion extension injury'),
        })
        self._test_queries('gov2/trec-mq-2007', count=10000, items={
            0: GenericQuery('1', 'after school program evaluation'),
            9: GenericQuery('10', 'qualifications for a senator'),
            9999: GenericQuery('10000', 'californa mission'),
        })
        self._test_queries('gov2/trec-mq-2008', count=10000, items={
            0: GenericQuery('10001', 'comparability of pay analyses'),
            9: GenericQuery('10010', 'in in 2015 will the u.s military be fighting iran and north korea'),
            9999: GenericQuery('20000', 'manchester city hall'),
        })

    def test_gov2_qrels(self):
        self._test_qrels('gov2/trec-tb-2004', count=58077, items={
            0: TrecQrel('701', 'GX000-00-13923627', 0, '0'),
            9: TrecQrel('701', 'GX000-25-2008761', 1, '0'),
            58076: TrecQrel('750', 'GX272-82-4931834', 0, '0'),
        })
        self._test_qrels('gov2/trec-tb-2005', count=45291, items={
            0: TrecQrel('751', 'GX000-00-13125308', 0, '0'),
            9: TrecQrel('751', 'GX000-47-11993633', 0, '0'),
            45290: TrecQrel('800', 'GX272-48-8680401', 1, '0'),
        })
        self._test_qrels('gov2/trec-tb-2006', count=31984, items={
            0: TrecQrel('801', 'GX000-01-2722311', 0, '0'),
            9: TrecQrel('801', 'GX001-46-11521081', 1, '0'),
            31983: TrecQrel('850', 'GX272-67-14117174', 0, '0'),
        })
        self._test_qrels('gov2/trec-tb-2005/named-page', count=11729, items={
            0: TrecQrel('601', 'GX000-06-6013381', 1, '0'),
            9: TrecQrel('606', 'GX001-80-15356704', 1, '0'),
            11728: TrecQrel('872', 'GX270-03-12329248', 1, '0'),
        })
        self._test_qrels('gov2/trec-tb-2005/efficiency', count=45291, items={
            0: TrecQrel('1192', 'GX000-00-13125308', 0, '0'),
            9: TrecQrel('1192', 'GX000-47-11993633', 0, '0'),
            45290: TrecQrel('49462', 'GX272-48-8680401', 1, '0'),
        })
        self._test_qrels('gov2/trec-tb-2006/named-page', count=2361, items={
            0: TrecQrel('901', 'GX123-98-3885901', 1, '0'),
            9: TrecQrel('902', 'GX078-80-12349004', 1, '1'),
            2360: TrecQrel('1081', 'GX136-71-9506712', 1, '0'),
        })
        self._test_qrels('gov2/trec-tb-2006/efficiency', count=31984, items={
            0: TrecQrel('62937', 'GX000-01-2722311', 0, '0'),
            9: TrecQrel('62937', 'GX001-46-11521081', 1, '0'),
            31983: TrecQrel('71136', 'GX272-67-14117174', 0, '0'),
        })
        self._test_qrels('gov2/trec-tb-2006/efficiency/stream3', count=31984, items={
            0: TrecQrel('62937', 'GX000-01-2722311', 0, '0'),
            9: TrecQrel('62937', 'GX001-46-11521081', 1, '0'),
            31983: TrecQrel('71136', 'GX272-67-14117174', 0, '0'),
        })
        self._test_qrels('gov2/trec-mq-2007', count=73015, items={
            0: TrecPrel('10', 'GX253-98-16418961', 0, 1, 0.165904710473544),
            9: TrecPrel('10', 'GX225-79-9870332', 1, 1, 0.568969759822883),
            73014: TrecPrel('9999', 'GX237-19-4725226', 1, 1, 0.659520100607628),
        })
        self._test_qrels('gov2/trec-mq-2008', count=15211, items={
            0: TrecPrel('10002', 'GX037-06-11625428', 0, 1, 0.0031586555555558),
            9: TrecPrel('10032', 'GX010-65-7921994', 0, 1, 0.00137811889937823),
            15210: TrecPrel('19997', 'GX257-71-11550035', 0, 1, 0.00107614156144011),
        })

if __name__ == '__main__':
    unittest.main()
