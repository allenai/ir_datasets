import re
import unittest
import ir_datasets
from ir_datasets.datasets.gov import GovWeb02Query, GovDoc
from ir_datasets.formats import TrecQrel, TrecQuery, GenericQuery
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestGov(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('gov', count=1247753, items={
            0: GovDoc('G00-00-0000000', 'http://www.aspe.hhs.gov', 'HTTP/1.0 200 OK\r\nDate: Wed, 30 Jan 2002 17:00:23 GMT\r\nServer: WebSitePro/3.0.37\r\nAccept-ranges: bytes\r\nContent-type: text/html\r\nLast-modified: Fri, 18 Jan 2002 19:04:17 GMT\r\nContent-length: 8228\r\n', re.compile(b'^<!DOCTYPE HTML PUBLIC "\\-//w3c//dtd html 4\\.0 transitional//en" "http://www\\.w3\\.org/TR/REC\\-html40/loose.{8029} \n\\\t\\\t<P> <FONT SIZE="\\-1">Last updated on January 18, 2002\\.</FONT>\n\\\t\\\t  </P></CENTER> </BODY>\n</HTML>\n\n$', flags=16), 'text/html'),
            9: GovDoc('G00-00-0066311', 'http://www.oso.noaa.gov', re.compile('^HTTP/1\\.1 200 OK\\\r\nServer: Microsoft\\-IIS/4\\.0\\\r\nContent\\-Location: http://www\\.oso\\.noaa\\.gov/Index\\.htm\\\r\nDat.{80}\nLast\\-Modified: Mon, 07 Jan 2002 16:16:17 GMT\\\r\nETag: "28b3d1a29697c11:3a83"\\\r\nContent\\-Length: 12431\\\r\n$', flags=48), re.compile(b'^<html>\\\r\n\\\r\n<head lang="en\\-US">\\\r\n<meta http\\-equiv="Content\\-Type" content="text/html; charset=windows\\-1.{12232}tranpix\\.GIF" width="1" height="1" alt=""></td>\\\r\n    </tr>\\\r\n  </table>\\\r\n</div>\\\r\n\\\r\n</body>\\\r\n</html>\\\r\n\n$', flags=16), 'text/html'),
            1247752: GovDoc('G46-12-4054845', 'http://www.tfhrc.gov/pavement/ltpp/pdf/01-024d.pdf', re.compile('^HTTP/1\\.1 200 OK\\\r\nDate: Mon, 04 Feb 2002 01:07:07 GMT\\\r\nServer: Apache/1\\.3\\.6 \\(Unix\\)\\\r\nLast\\-Modified: We.{82}Content\\-Length: 977004\\\r\nConnection: close\\\r\nContent\\-Type: application/pdf\\\r\nX\\-Pad: avoid browser bug\\\r\n$', flags=48), re.compile(b'^           Mean\\(Neg Area\\)\n\n                        0                               \\.001 \\.01 \\.05\\.10 \\..{101813}       0\\.0%              0\n\n   Moments\n\n   Mean                        10722\\.77\n   Std Dev         \n$', flags=16), 'application/pdf'),
        })

    def test_queries(self):
        self._test_queries('gov/trec-web-2002', count=50, items={
            0: TrecQuery('551', 'intellectual property', 'Find documents related to laws or regulations that protect\nintellectual property.', 'Relevant documents describe legislation or federal regulations that\nprotect authors or composers from copyright infringement, or from\npiracy of their creative work. These regulations may also be related\nto fair use or to encryption.'),
            9: TrecQuery('560', 'Symptoms of diabetes', 'Find documents that list and explain the danger signals of Type I and\nType II diabetes.', 'Relevant documents give the symptoms/danger signals that\nconsumers need to recognize as warnings of the onset of Type I and\nType II diabetes. Exclude documents directed at medical personnel.'),
            49: TrecQuery('600', 'highway safety', 'Find documents related to improving highway safety in the U.S.', 'Relevant documents include those related to the improvement of safety\nof all vehicles driven on highways, including cars, trucks, vans, and\ntractor trailers. Ways to reduce accidents through legislation,\nvehicle checks, and drivers education programs are all relevant.'),
        })
        self._test_queries('gov/trec-web-2002/named-page', count=150, items={
            0: GenericQuery('1', "America's Century Farms"),
            9: GenericQuery('10', "FBI's most wanted list"),
            149: GenericQuery('150', 'employee access FDIC DC'),
        })
        self._test_queries('gov/trec-web-2003', count=50, items={
            0: GovWeb02Query('1', 'mining gold silver coal', 'What can be learned about the location of mines\nin the U.S., about the extent of mineral resources,\nand about careers in the mining industry?'),
            9: GovWeb02Query('10', 'Physical Fitness', 'Information on Physical Fitness'),
            49: GovWeb02Query('50', 'anthrax', 'Info regarding prevention and treatment of the disease anthrax.'),
        })
        self._test_queries('gov/trec-web-2003/named-page', count=300, items={
            0: GenericQuery('151', 'ADA Enforcement'),
            9: GenericQuery('160', 'NSF Fact Sheet January 2002'),
            299: GenericQuery('450', 'Surgeon General report on schizophrenia'),
        })
        self._test_queries('gov/trec-web-2004', count=225, items={
            0: GenericQuery('1', 'Electoral College'),
            9: GenericQuery('10', 'well water contamination'),
            224: GenericQuery('225', 'Japanese surrender document'),
        })

    def test_gov2_qrels(self):
        self._test_qrels('gov/trec-web-2002', count=56650, items={
            0: TrecQrel('551', 'G14-77-3709129', 0, '0'),
            9: TrecQrel('551', 'G22-94-3703003', 1, '0'),
            56649: TrecQrel('600', 'G12-45-0115310', 0, '0'),
        })
        self._test_qrels('gov/trec-web-2002/named-page', count=170, items={
            0: TrecQrel('1', 'G00-04-3805407', 1, '0'),
            9: TrecQrel('8', 'G00-90-0514219', 1, '0'),
            169: TrecQrel('150', 'G00-48-3849824', 1, '0'),
        })
        self._test_qrels('gov/trec-web-2003', count=51062, items={
            0: TrecQrel('1', 'G00-00-0681214', 0, '0'),
            9: TrecQrel('1', 'G00-01-0682299', 0, '0'),
            51061: TrecQrel('50', 'G45-99-1311180', 0, '0'),
        })
        self._test_qrels('gov/trec-web-2003/named-page', count=352, items={
            0: TrecQrel('151', 'G02-86-0432155', 1, '0'),
            9: TrecQrel('160', 'G29-09-0183573', 1, '0'),
            351: TrecQrel('450', 'G44-36-3557956', 1, '0'),
        })
        self._test_qrels('gov/trec-web-2004', count=88566, items={
            0: TrecQrel('1', 'G00-00-2869955', 0, '0'),
            9: TrecQrel('1', 'G00-06-0365563', 0, '0'),
            88565: TrecQrel('225', 'G45-68-2130931', 0, '0'),
        })


if __name__ == '__main__':
    unittest.main()
