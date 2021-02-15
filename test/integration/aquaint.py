import re
import unittest
import ir_datasets
from ir_datasets.formats import TrecQrel, TrecDoc, TrecQuery
from .base import DatasetIntegrationTest

class TestAquaint(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('aquaint', count=1033461, items={
            0: TrecDoc('APW19980601.0003', re.compile("^\n\nDoohan calls for upgrade to 1000cc bikes \n\n\n\\\t   SYDNEY, Australia \\(AP\\) _ Four\\-time world 500cc mot.{1909}r\\-stroke grand prix\nbikes there's plenty of other championships for them to race in\\.''\n \\&UR; \\(tjh\\)\n\n$", flags=48), re.compile("^<HEADLINE>\nDoohan calls for upgrade to 1000cc bikes \n</HEADLINE>\n<TEXT>\n\\\t   SYDNEY, Australia \\(AP\\) _.{1942}e grand prix\nbikes there's plenty of other championships for them to race in\\.''\n \\&UR; \\(tjh\\)\n</TEXT>\n$", flags=48)),
            9: TrecDoc('APW19980601.0028', re.compile("^\n\nForeign minister again denounces nuclear tests \n\n\n\\\t   CANBERRA, Australia \\(AP\\) _ Australia does no.{988} Pakistan to\nimmediately sign and ratify the comprehensive nuclear ban treaty,''\nDowner said\\. \\\t   \n\n$", flags=48), re.compile("^<HEADLINE>\nForeign minister again denounces nuclear tests \n</HEADLINE>\n<TEXT>\n\\\t   CANBERRA, Australi.{1021}an to\nimmediately sign and ratify the comprehensive nuclear ban treaty,''\nDowner said\\. \\\t   \n</TEXT>\n$", flags=48)),
            1033460: TrecDoc('XIE20000930.0369', re.compile('^\n\n 2000\\-09\\-30 \n Argentine President Meets With Indonesian Counterpart \n\n\nArgentine Agriculture Minis.{488} on\nThursday\\.\n\n\nThe Indonesian president is scheduled to leave here for Chile on\nSaturday night\\. \n\n\n$', flags=48), re.compile('^<DOC>\n<DATE_TIME> 2000\\-09\\-30 </DATE_TIME>\n<BODY>\n<HEADLINE> Argentine President Meets With Indonesia.{599} Indonesian president is scheduled to leave here for Chile on\nSaturday night\\. \n</P>\n</TEXT>\n</BODY>\n$', flags=48)),
        })

    def test_queries(self):
        self._test_queries('aquaint/trec-robust-2005', count=50, items={
            0: TrecQuery('303', 'Hubble Telescope Achievements', 'Identify positive accomplishments of the Hubble telescope since it\nwas launched in 1991.', 'Documents are relevant that show the Hubble telescope has produced\nnew data, better quality data than previously available, data that\nhas increased human knowledge of the universe, or data that has led\nto disproving previously existing theories or hypotheses.  Documents\nlimited to the shortcomings of the telescope would be irrelevant.\nDetails of repairs or modifications to the telescope without\nreference to positive achievements would not be relevant.'),
            9: TrecQuery('344', 'Abuses of E-Mail', 'The availability of E-mail to many people through their\njob or school affiliation has allowed for many efficiencies\nin communications but also has provided the opportunity for\nabuses.  What steps have been taken world-wide by those\nbearing the cost of E-mail to prevent excesses?', "To be relevant, a document will concern dissatisfaction by\nan entity paying for the cost of electronic mail.  Particularly\nsought are items which relate to system users (such as employees)\nwho abuse the system by engaging in communications of the type\nnot related to the payer's desired use of the system."),
            49: TrecQuery('689', 'family-planning aid', 'To which countries does the U.S. provide aid to support family planning,\nand for which countries has the U.S. refused or limited support?', 'Relevant documents indicate where U.S. aid supports\nfamily planning or where such aid has been denied.\nDiscussions of why aid for family planning has been refused are\nalso relevant.  Documents that mention U.S. aid to countries,\nbut not specifically for family planning are not relevant.\nDescriptions of funds for family planning in the U.S. itself are not relevant.'),
        })

    def test_qrels(self):
        self._test_qrels('aquaint/trec-robust-2005', count=37798, items={
            0: TrecQrel('303', 'APW19980609.1531', 2, '0'),
            9: TrecQrel('303', 'APW19981117.0914', 0, '0'),
            37797: TrecQrel('689', 'XIE20000925.0055', 0, '0'),
        })

if __name__ == '__main__':
    unittest.main()
