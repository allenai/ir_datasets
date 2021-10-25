import re
import unittest
import datetime
import ir_datasets
from ir_datasets.formats import GenericQuery, TrecQrel
from ir_datasets.datasets.aol_ia import AolIaDoc, AolQlog, LogItem
from .base import DatasetIntegrationTest


class TestAolIa(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('aol-ia', count=1525586, items={
            0: AolIaDoc('00002a94464d', 'Alchimie Forever skin care for men and women', re.compile("^require \\( `` include/config\\.php '' \\) ; if \\( \\$ PHPSESSID \\) session_start \\( \\$ PHPSESSID \\) ; else sessi.{328} Terms and conditions © 2005 Alchimie Forever Sàrl \\. All rights reserved \\. Design : Agence Virtuelle$", flags=48), 'http://www.alchimie-forever.com', 'https://web.archive.org/web/20060218092031/http://www.alchimie-forever.com:80/'),
            9: AolIaDoc('00007d6c3dd3', 'Pinehurst Tea Room & Caterering', re.compile('^We have had visitors \\. Welcome to Pinehurst Tea Room \\. This beautifully restored Victorian house is .{456}n please contact Lynda Dubbs at 770\\-474\\-7997 or feel free to email her at pinehursttearoom @ aol\\.com$', flags=48), 'http://www.pinehursttearoom.com', 'https://web.archive.org/web/20060209164740/http://www.pinehursttearoom.com:80/'),
            1525585: AolIaDoc('fffff6b18440', 'Golf School - Arizona Golf School , Florida Golf School , Calfornia Golf School', '', 'http://lvgolfschools.com', 'https://web.archive.org/web/20060211025934/http://www.lvgolfschools.com:80/'),
        })

    def test_queries(self):
        self._test_queries('aol-ia', count=9966939, items={
            0: GenericQuery('8c418e7c9e5993', 'rentdirect com'),
            9: GenericQuery('c8476c36af8761', 'www elaorg'),
            9966938: GenericQuery('bba88dc56436eb', 'c21curabba'),
        })

    def test_qrels(self):
        self._test_qrels('aol-ia', count=19442629, items={
            0: TrecQrel('50aa67fe786ca7', '430d8aa747a3', 1, '142'),
            9: TrecQrel('f6eff9e0848e2d', 'ecd6d884243b', 1, '217'),
            19442628: TrecQrel('14c1b5b54212ad', 'a114f6d94af0', 1, '24967361'),
        })

    def test_qlog(self):
        self._test_qlogs('aol-ia', count=36389567, items={
            0: AolQlog('142', '8c418e7c9e5993', 'rentdirect com', 'rentdirect.com', datetime.datetime(2006, 3, 1, 7, 17, 12), ()),
            6: AolQlog('142', '50aa67fe786ca7', 'westchester gov', 'westchester.gov', datetime.datetime(2006, 3, 20, 3, 55, 57), (LogItem('430d8aa747a3', '1', True),)),
            9: AolQlog('142', 'b52c96bea30646', 'dfdf', 'dfdf', datetime.datetime(2006, 3, 24, 22, 23, 14), ()),
            36389566: AolQlog('24969339', 'a03587795a216c', 'free credit report', 'free credit report', datetime.datetime(2006, 5, 31, 0, 42, 17), ()),
        })


if __name__ == '__main__':
    unittest.main()
