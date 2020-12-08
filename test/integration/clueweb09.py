import re
import unittest
import ir_datasets
from ir_datasets.datasets.clueweb09 import TrecWebTrackQuery, TrecPrel, WarcHtmlDoc
from ir_datasets.formats import TrecQrel, TrecSubtopic, GenericDoc, GenericQuery
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestClueWeb09(DatasetIntegrationTest):
    def test_clueweb09_docs(self):
        self._test_docs('clueweb09', items={
            0: WarcHtmlDoc('clueweb09-en0000-00-00000', 'http://00000-nrt-realestate.homepagestartup.com/', '2009-03-65T08:43:19-0800', re.compile('^<head> <meta http\\-equiv="Content\\-Language" content="en\\-gb"> <meta http\\-equiv="Content\\-Type" content=.{16056} 8pt">YouTube Videos</span></td> </tr> </table> </td> </tr> </table></div> </div> </body> </html> \n\n$', flags=48)),
            9: WarcHtmlDoc('clueweb09-en0000-00-00009', 'http://00perdomain.com/computers/', '2009-03-65T08:43:20-0800', re.compile('^\n\n\n<!\\-\\- Site by Zaz Corporation, 1 \\- 8 8 8 \\- 2 \\- Z A Z C O R  http://www\\.zazcorp\\.us \\-\\->\n\n\n\n<html><he.{23302}<script type="text/javascript">\n_uacct = "UA\\-488717\\-2";\nurchinTracker\\(\\);\n</script>\n\n</body></html>\n\n$', flags=48)),
            1000: WarcHtmlDoc('clueweb09-en0000-00-01000', 'http://2modern.com/designer/FLOS/Flos-Archimoon-Soft-Table-Lamp', '2009-03-65T08:44:07-0800', re.compile('^\n<html>\n<head>\n<meta http\\-equiv="Content\\-Type" content="text/html; charset=UTF\\-8">\n<title>FLOS \\- Arc.{52543}\\- \\[ 418126 \\] \\[  \\] \\[ /s\\.nl \\] \\[ Tue Jan 13 13:10:47 PST 2009 \\] \\-\\->\n<!\\-\\- Not logging slowest SQL \\-\\->\n\n\n$', flags=48)),
        })

    def test_clueweb09_docstore(self):
        docstore = ir_datasets.load('clueweb09').docs_store()
        docstore.clear_cache()
        with _logger.duration('cold fetch'):
            docstore.get_many(['clueweb09-en0000-00-00003', 'clueweb09-en0000-00-35581', 'clueweb09-ar0000-48-02342'])
        with _logger.duration('warm fetch'):
            docstore.get_many(['clueweb09-en0000-00-00003', 'clueweb09-en0000-00-35581', 'clueweb09-ar0000-48-02342'])
        docstore = ir_datasets.load('clueweb09').docs_store()
        with _logger.duration('warm fetch (new docstore)'):
            docstore.get_many(['clueweb09-en0000-00-00003', 'clueweb09-en0000-00-35581', 'clueweb09-ar0000-48-02342'])
        with _logger.duration('cold fetch (nearby)'):
            docstore.get_many(['clueweb09-en0000-00-00023', 'clueweb09-en0000-00-35570', 'clueweb09-ar0000-48-02348'])
        with _logger.duration('cold fetch (earlier)'):
            docstore.get_many(['clueweb09-en0000-00-00001', 'clueweb09-ar0000-48-00009'])


    def test_clueweb09_queries(self):
        self._test_queries('clueweb09/trec-web-2009', count=50, items={
            0: TrecWebTrackQuery('1', 'obama family tree', "Find information on President Barack Obama's family\n  history, including genealogy, national origins, places and dates of\n  birth, etc.\n  ", 'faceted', (TrecSubtopic(number='1', text='\n    Find the TIME magazine photo essay "Barack Obama\'s Family Tree".\n  ', type='nav'), TrecSubtopic(number='2', text="\n    Where did Barack Obama's parents and grandparents come from?\n  ", type='inf'), TrecSubtopic(number='3', text="\n    Find biographical information on Barack Obama's mother.\n  ", type='inf'))),
            9: TrecWebTrackQuery('10', 'cheap internet', "I'm looking for cheap (i.e. low-cost) internet service.\n  ", 'faceted', (TrecSubtopic(number='1', text='\n    What are some low-cost broadband internet providers?\n  ', type='inf'), TrecSubtopic(number='2', text='\n    Do any internet providers still sell dial-up?\n  ', type='inf'), TrecSubtopic(number='3', text='\n    Who can provide inexpensive digital cable television bundled with\n    internet service?\n  ', type='inf'), TrecSubtopic(number='4', text="\n    I'm looking for the Vonage homepage.\n  ", type='nav'), TrecSubtopic(number='5', text='\n    Find me some providers of free wireless internet access.\n  ', type='inf'), TrecSubtopic(number='6', text='\n    I want to find cheap DSL providers.\n  ', type='inf'), TrecSubtopic(number='7', text='\n    Is there a way to get internet access without phone service?\n  ', type='inf'), TrecSubtopic(number='8', text="\n    Take me to Comcast's homepage.\n  ", type='nav'))),
            49: TrecWebTrackQuery('50', 'dog heat', 'What is the effect of excessive heat on dogs?\n  ', 'ambiguous', (TrecSubtopic(number='1', text='\n    What is the effect of excessive heat on dogs?\n  ', type='inf'), TrecSubtopic(number='2', text='\n    What are symptoms of heat stroke and other heat-related illnesses\n    in dogs?\n  ', type='inf'), TrecSubtopic(number='3', text='\n    Find information on dogs\' reproductive cycle.  What does it mean\n    when a dog is "in heat"?\n  ', type='inf'))),
        })
        self._test_queries('clueweb09/trec-web-2010', count=50, items={
            0: TrecWebTrackQuery('51', 'horse hooves', '\n    Find information about horse hooves, their care, and diseases of hooves.\n  ', 'faceted', (TrecSubtopic(number='1', text="\n    Find information about horses' hooves and how to care for them.\n  ", type='inf'), TrecSubtopic(number='2', text='\n    Find pictures of horse hooves.\n  ', type='nav'), TrecSubtopic(number='3', text='\n    What are some injuries or diseases of hooves in horses, and how\n    are they treated?\n  ', type='inf'), TrecSubtopic(number='4', text="\n    Describe the anatomy of horses' feet and hooves.\n  ", type='inf'), TrecSubtopic(number='5', text='\n    Find information on shoeing horses and horseshoe problems.\n  ', type='inf'))),
            9: TrecWebTrackQuery('60', 'bellevue', '\n    Find information about Bellevue, Washington.\n  ', 'ambiguous', (TrecSubtopic(number='1', text='\n    Find information about Bellevue, Washington.\n  ', type='inf'), TrecSubtopic(number='2', text='\n    Find information about Bellevue, Nebraska.\n  ', type='inf'), TrecSubtopic(number='3', text='\n    Find information about Bellevue Hospital Center in New York, NY.\n  ', type='inf'), TrecSubtopic(number='4', text='\n    Find the homepage of Bellevue University.\n  ', type='nav'), TrecSubtopic(number='5', text='\n    Find the homepage of Bellevue College, Washington.\n  ', type='nav'), TrecSubtopic(number='6', text='\n    Find the homepage of Bellevue Hospital Center in New York, NY.\n  ', type='nav'))),
            49: TrecWebTrackQuery('100', 'rincon puerto rico', '\n    Find information about Rincon, Puerto Rico.\n  ', 'faceted', (TrecSubtopic(number='1', text='\n    Find hotels and beach resorts in Rincon, Puerto Rico.\n  ', type='inf'), TrecSubtopic(number='2', text='\n    Find information on the history of Rincon, Puerto Rico.\n  ', type='inf'), TrecSubtopic(number='3', text='\n    Find surf forecasts for Rincon, Puerto Rico.\n  ', type='inf'), TrecSubtopic(number='4', text='\n    Find pictures of Rincon, Puerto Rico.\n  ', type='nav'), TrecSubtopic(number='5', text='\n    Find information about real estate and rental properties in\n    Rincon, Puerto Rico.\n  ', type='inf'))),
        })
        self._test_queries('clueweb09/trec-web-2011', count=50, items={
            0: TrecWebTrackQuery('101', 'ritz carlton lake las vegas', '\n    Find information about the Ritz Carlton resort at Lake Las Vegas.\n  ', 'faceted', (TrecSubtopic(number='1', text='\n    Find information about the Ritz Carlton resort at Lake Las Vegas.\n  ', type='inf'), TrecSubtopic(number='2', text='\n    Find a site where I can determine room price and availability.\n  ', type='nav'), TrecSubtopic(number='3', text='\n    Find directions to the Ritz Carlton Lake Las Vegas.\n  ', type='nav'), TrecSubtopic(number='4', text='\n    Find reviews of the Ritz Carlton Lake Las Vegas.\n  ', type='inf'))),
            9: TrecWebTrackQuery('110', 'map of brazil', '\n    What are the boundaries of the political jurisdictions in Brazil?\n  ', 'ambiguous', (TrecSubtopic(number='1', text='\n    What are the boundaries of the political jurisdictions in Brazil?\n  ', type='inf'), TrecSubtopic(number='2', text='\n    I am looking for information about taking a vacation trip to Brazil.\n  ', type='inf'), TrecSubtopic(number='3', text='\n    I want to buy a road map of Brazil.\n  ', type='nav'))),
            49: TrecWebTrackQuery('150', 'tn highway patrol', '\n    What are the requirements to become a Tennessee Highway Patrol State Trooper?\n  ', 'faceted', (TrecSubtopic(number='1', text='\n    What are the requirements to become a Tennessee Highway Patrol State Trooper?\n  ', type='inf'), TrecSubtopic(number='2', text='\n    information about the responsibilities of the Tennessee Highway Patrol\n  ', type='inf'), TrecSubtopic(number='3', text='\n    home page of the Tennessee Highway Patrol\n  ', type='nav'), TrecSubtopic(number='4', text='\n    I want to fill in the customer satisfaction survey about my interaction with a Tennessee Highway Patrol State Trooper.\n  ', type='nav'))),
        })
        self._test_queries('clueweb09/trec-web-2012', count=50, items={
            0: TrecWebTrackQuery('151', '403b', '\n    What is a 403b plan?\n  ', 'faceted', (TrecSubtopic(number='1', text='\n     What is a 403b plan?\n  ', type='inf'), TrecSubtopic(number='2', text='\n    Who is eligible for a 403b plan?\n  ', type='inf'), TrecSubtopic(number='3', text='\n    What are the rules for a 403b retirement plan?\n  ', type='nav'), TrecSubtopic(number='4', text='\n    What is the difference between 401k and 403b retirement plans?\n  ', type='inf'), TrecSubtopic(number='5', text='\n    What are the withdrawal limitations for a 403b retirement plan?\n  ', type='nav'))),
            9: TrecWebTrackQuery('160', 'grilling', '\n    Find kabob recipes.\n  ', 'ambiguous', (TrecSubtopic(number='1', text='\n    Find kabob recipes.\n  ', type='nav'), TrecSubtopic(number='2', text='\n    Find tips on grilling vegetables.\n  ', type='inf'), TrecSubtopic(number='3', text='\n    Find tips on grilling fish.\n  ', type='inf'), TrecSubtopic(number='4', text='\n    Find instructions for grilling chicken.\n  ', type='inf'), TrecSubtopic(number='5', text='\n    Find the Grilling Magazine website.\n  ', type='nav'), TrecSubtopic(number='6', text='\n    Find information on gas barbecue grills and cooking on a gas grill.\n  ', type='inf'))),
            49: TrecWebTrackQuery('200', 'ontario california airport', '\n    Find flight information for the Ontario, CA airport.\n  ', 'faceted', (TrecSubtopic(number='1', text='\n    Find flight information for the Ontario, CA airport.\n  ', type='inf'), TrecSubtopic(number='2', text='\n    What hotels are near the Ontario, CA airport?\n  ', type='inf'), TrecSubtopic(number='3', text='\n    What services/facilities does the Ontario, CA airport offer?\n  ', type='inf'), TrecSubtopic(number='4', text='\n    What is the address of the Ontario, CA airport?\n  ', type='nav'))),
        })

    def test_clueweb09_qrels(self):
        self._test_qrels('clueweb09/trec-web-2009', count=23601, items={
            0: TrecPrel('1', 'clueweb09-en0003-55-31884', 0, 0, 1.0),
            9: TrecPrel('1', 'clueweb09-en0009-84-37392', 0, 1, 0.0136322534877696),
            23600: TrecPrel('50', 'clueweb09-en0007-05-20194', 0, 1, 1.0),
        })
        self._test_qrels('clueweb09/trec-web-2010', count=25329, items={
            0: TrecQrel('51', 'clueweb09-en0000-16-19379', 0, '0'),
            9: TrecQrel('51', 'clueweb09-en0001-55-24197', 0, '0'),
            25328: TrecQrel('99', 'clueweb09-enwp03-23-18429', 0, '0'),
        })
        self._test_qrels('clueweb09/trec-web-2011', count=19381, items={
            0: TrecQrel('101', 'clueweb09-en0007-71-07471', 0, '0'),
            9: TrecQrel('101', 'clueweb09-en0044-05-29808', 2, '0'),
            19380: TrecQrel('150', 'clueweb09-en0003-86-25593', -2, '0'),
        })
        self._test_qrels('clueweb09/trec-web-2012', count=16055, items={
            0: TrecQrel('151', 'clueweb09-en0000-00-03430', -2, '0'),
            9: TrecQrel('151', 'clueweb09-en0000-00-04023', -2, '0'),
            16054: TrecQrel('200', 'clueweb09-enwp03-49-00268', 0, '0'),
        })


if __name__ == '__main__':
    unittest.main()
