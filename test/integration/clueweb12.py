import re
import unittest
from ir_datasets.datasets.clueweb12 import TrecWebTrackQuery, NtcirQuery, WarcHtmlDoc, MisinfoQrel, MisinfoQuery
from ir_datasets.formats import TrecQrel, TrecSubtopic, GenericDoc, GenericQuery
from .base import DatasetIntegrationTest


class TestClueWeb12(DatasetIntegrationTest):
    def test_clueweb12_docs(self):
        self._test_docs('clueweb12', items={
            0: WarcHtmlDoc('clueweb12-0000tw-00-00000', 'http://tsawer.net/2012/02/10/france-image-pool-2012-02-10-162252/', '2012-02-10T22:50:41Z', re.compile('^<!DOCTYPE html PUBLIC "\\-//W3C//DTD XHTML 1\\.0 Strict//EN"\\\r\n        "http://www\\.w3\\.org/TR/xhtml1/DTD/x.{22239}p://tsawer\\.net/wpaggregator/wp\\-content/plugins/contact\\-form\\-7/scripts\\.js\\?ver=3\\.1\'></script>\n</html>\n$', flags=48)),
            9: WarcHtmlDoc('clueweb12-0000tw-00-00009', 'http://claywginn.com/2012/02/10/lessons-learned-from-a-week-on-vacation/', '2012-02-10T21:47:35Z', re.compile('^<!DOCTYPE html PUBLIC "\\-//W3C//DTD XHTML 1\\.0 Transitional//EN" "http://www\\.w3\\.org/TR/xhtml1/DTD/xhtm.{25532}f addLoadEvent != \'undefined\' \\) addLoadEvent\\(load_cmc\\);\n\\\telse load_cmc\\(\\);\n\\\t</script></body>\\\r\n</html>$', flags=48)),
            1000: WarcHtmlDoc('clueweb12-0000tw-00-01002', 'http://beanpotscastiron.waffleironshapes.com/le-creuset-enameled-cast-iron-7-14-quart-round-french-oven-cherry-red-save-price-shopping-online/', '2012-02-10T21:55:43Z', re.compile('^<!DOCTYPE html PUBLIC "\\-//W3C//DTD XHTML 1\\.0 Transitional//EN" "http://www\\.w3\\.org/TR/xhtml1/DTD/xhtm.{27882}insertBefore\\(js, fjs\\);\\\r\n\\}\\(document, \'script\', \'facebook\\-jssdk\'\\)\\);</script>\\\r\n</div></body>\\\r\n</html>\\\r\n$', flags=48)),
        })
        self._test_docs('clueweb12/b13', items={
            0: WarcHtmlDoc('clueweb12-0000tw-00-00013', 'http://cheapcosthealthinsurance.com/2012/01/25/what-is-hiv-aids/', '2012-02-10T21:51:20Z', re.compile('^\n<!DOCTYPE html PUBLIC "\\-//W3C//DTD XHTML 1\\.0 Transitional//EN" "http://www\\.w3\\.org/TR/xhtml1/DTD/xht.{71109}\\.js"></script>\n</body>\n\n</html>\n\n<!\\-\\-\nEnd of footer\\.php\n\\~\\~\\~ \\-\\->\n\\\t\n\n\n<!\\-\\-\nEnd of single\\.php \n\\~\\~\\~ \\-\\->\n$', flags=48)),
            9: WarcHtmlDoc('clueweb12-0000tw-00-00139', 'http://data-protection.safenet-inc.com/social-media/', '2012-02-10T21:56:06Z', re.compile('^<!DOCTYPE html>\n<!\\-\\-\\[if IE 6\\]>\n<html id="ie6" dir="ltr" lang="en\\-US">\n<!\\[endif\\]\\-\\->\n<!\\-\\-\\[if IE 7\\]>\n<h.{13818}cs\\.com/ga\\.js";\n    s\\.parentNode\\.insertBefore\\(g,s\\)\\}\\(document,"script"\\)\\);\n  </script>\n\\\t</body>\n</html>$', flags=48)),
            1000: WarcHtmlDoc('clueweb12-0000tw-00-14061', 'http://opinionator.blogs.nytimes.com/2006/01/12/this-wont-hurt-a-bit/', '2012-02-10T22:24:09Z', re.compile('^<!DOCTYPE html PUBLIC "\\-//W3C//DTD XHTML 1\\.0 Transitional//EN" "http://www\\.w3\\.org/TR/xhtml1/DTD/xhtm.{105686},Spon3,ADX_CLIENTSIDE,SponLink2\\&pos=Bottom8\\&query=qstring\\&keywords=\\?"></a></noscript></body>\n</html>$', flags=48)),
        })

    def test_clueweb12_queries(self):
        self._test_queries('clueweb12/trec-web-2013', count=50, items={
            0: TrecWebTrackQuery('201', 'raspberry pi', '\n    What is a raspberry pi?\n  ', 'faceted', (TrecSubtopic(number='1', text='\n    What is a raspberry pi?\n  ', type='inf'), TrecSubtopic(number='2', text='\n    What software does a raspberry pi use?\n  ', type='inf'), TrecSubtopic(number='3', text='\n    What are hardware options for a raspberry pi?\n  ', type='inf'), TrecSubtopic(number='4', text='\n    How much does a basic raspberry pi cost?\n  ', type='nav'), TrecSubtopic(number='5', text='\n    Find info about the raspberry pi foundation.\n  ', type='inf'), TrecSubtopic(number='6', text='\n    Find a picture of a raspberry pi.\n  ', type='nav'))),
            9: TrecWebTrackQuery('210', 'golf gps', '\n    What is the best golf gps device?\n  ', 'faceted', (TrecSubtopic(number='1', text='\n    What is the best golf gps device?\n  ', type='inf'), TrecSubtopic(number='2', text='\n    Compare Bushnell, Callaway and Garmin golf gps systems.\n  ', type='inf'), TrecSubtopic(number='3', text='\n    Is there a golf gps app for the Iphone?\n  ', type='nav'), TrecSubtopic(number='4', text='\n    Find information on handheld golf gps devices.\n  ', type='inf'), TrecSubtopic(number='5', text='\n    Is there a golf gps system that can be used world wide?\n  ', type='nav'), TrecSubtopic(number='6', text='\n    Where can I get a used golf gps device?\n  ', type='inf'))),
            49: TrecWebTrackQuery('250', 'ford edge problems', '\n    What problems have afflicted the Ford Edge car model?\n  ', 'single', ()),
        })
        self._test_queries('clueweb12/trec-web-2014', count=50, items={
            0: TrecWebTrackQuery('251', 'identifying spider bites', '\n  \tFind data on how to identify spider bites.\n  ', 'single', ()),
            9: TrecWebTrackQuery('260', 'the american revolutionary', '\n  \tFind a list of the major battles of the American Revolution.\n  ', 'faceted', (TrecSubtopic(number='1', text='\n  \tFind a list of the major battles of the American Revolution.\n  ', type='nav'), TrecSubtopic(number='2', text='\n  \tFind a time line of the American Revolution.\n  ', type='nav'), TrecSubtopic(number='3', text='\n  \tFind images of the American Revolution.\n  ', type='inf'), TrecSubtopic(number='4', text='\n  \tWhat were the causes of the American revolutionary war?\n  ', type='inf'), TrecSubtopic(number='5', text='\n  \tWhat is the history of the American revolutionary war?\n  ', type='inf'))),
            49: TrecWebTrackQuery('300', 'how to find the mean', '\n  \tFind a page that explains how to compute the mean of a set of numbers.\n  ', 'single', ()),
        })
        self._test_queries('clueweb12/b13/ntcir-www-1', count=100, items={
            0: GenericQuery('0001', 'ascii code'),
            9: GenericQuery('0010', 'Jurassic World'),
            99: GenericQuery('0100', 'weight loss'),
        })
        self._test_queries('clueweb12/b13/ntcir-www-2', count=80, items={
            0: NtcirQuery('0001', 'Halloween picture', 'Halloween is coming. You want to find some pictures about Halloween to introduce it to your children.'),
            9: NtcirQuery('0010', 'career plan', 'You are an undergraduate student who is about to graduate. You want to search some information about how to plan your career.'),
            79: NtcirQuery('0080', 'www.gardenburger.com', 'You want to find the website "www.gardenburger.com"'),
        })
        self._test_queries('clueweb12/b13/ntcir-www-3', count=160, items={
            0: NtcirQuery('0001', 'Halloween picture', 'Halloween is coming. You want to find some pictures about Halloween to introduce it to your children.'),
            9: NtcirQuery('0010', 'career plan', 'You are an undergraduate student who is about to graduate. You want to search some information about how to plan your career.'),
            159: NtcirQuery('0180', 'quincy jones productions', 'You want a list of famous records produced by Quincy Jones.'),
        })
        self._test_queries('clueweb12/b13/trec-misinfo-2019', count=51, items={
            0: MisinfoQuery('1', 'cranberries urinary tract infections', '10.1002/14651858.CD001321.pub5', 'Can cranberries prevent urinary tract infections?', 'Symptoms of a urinary tract infection (UTI) include burning while urinating and a persistent urge to urinate. Relevant documents should discuss the effectiveness of consuming cranberries or cranberry juice for prevention of UTIs.  This topic is specifically about prevention rather than treatment of an existing infection.'),
            9: MisinfoQuery('10', 'gene therapy sickle cell', '10.1002/14651858.CD007652.pub6', 'Can gene therapy prevent complications caused by sickle cell disease?', 'Sickle cell disease (SCD) is an inherited blood disorder that affects the development of healthy red blood cells and causes red blood cells to change their form from a normal round shape to a crescent and rigid shape. People with sickle cell disease have fewer healthy blood cells, which can affect their oxygen carrying capacity and lead to serious or life-threatening complications. Gene therapy, as a newly advanced field, is claimed to be helpful for this disease. A relevant document discusses using gene therapy for preventing the symptoms and complications of SCD.'),
            50: MisinfoQuery('51', 'dehumidifiers asthma', '10.1002/14651858.CD003563.pub2', 'Can dehumidifiers be used to control asthma?', 'Dehumidification homes might improve lives of people with asthma. Dehumidifiers are electronic devices to control the level of humidity of environment which is suggested to contribute to factors that might affect asthma. A relevant document should discuss whether or not dehumidifiers can be used to control asthma symptoms or can improve lives of people with asthma.'),
        })

    def test_clueweb12_qrels(self):
        self._test_qrels('clueweb12/trec-web-2013', count=14474, items={
            0: TrecQrel('201', 'clueweb12-0000tw-05-12114', 1, '0'),
            9: TrecQrel('201', 'clueweb12-0108wb-22-26598', 0, '0'),
            14473: TrecQrel('250', 'clueweb12-1914wb-21-25488', 0, '0'),
        })
        self._test_qrels('clueweb12/trec-web-2014', count=14432, items={
            0: TrecQrel('251', 'clueweb12-0000tw-34-04382', 1, '0'),
            9: TrecQrel('251', 'clueweb12-0000wb-90-35684', 1, '0'),
            14431: TrecQrel('300', 'clueweb12-1911wb-40-07107', 0, '0'),
        })
        self._test_qrels('clueweb12/b13/ntcir-www-1', count=25465, items={
            0: TrecQrel('0001', 'clueweb12-0000wb-16-36432', 3, '0'),
            9: TrecQrel('0001', 'clueweb12-0002wb-06-22258', 1, '0'),
            25464: TrecQrel('0100', 'clueweb12-1913wb-56-22315', 2, '0'),
        })
        self._test_qrels('clueweb12/b13/ntcir-www-2', count=27627, items={
            0: TrecQrel('0001', 'clueweb12-0000wb-83-31594', 0, '0'),
            9: TrecQrel('0001', 'clueweb12-0003wb-36-30766', 0, '0'),
            27626: TrecQrel('0080', 'clueweb12-1910wb-52-19011', 0, '0'),
        })
        self._test_qrels('clueweb12/b13/trec-misinfo-2019', count=22859, items={
            0: MisinfoQrel('1', 'clueweb12-0000wb-03-01030', 1, 2, 0),
            9: MisinfoQrel('1', 'clueweb12-0002wb-45-16639', 0, -1, -1),
            22858: MisinfoQrel('51', 'clueweb12-1913wb-78-31232', 0, -1, -1),
        })


if __name__ == '__main__':
    unittest.main()
