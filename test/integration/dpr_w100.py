import re
import unittest
from ir_datasets.datasets.dpr_w100 import DprW100Doc, DprW100Query
from ir_datasets.formats import TrecQrel, TrecQuery
from .base import DatasetIntegrationTest


class TestDprW100(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('dpr-w100', count=21015324, items={
            0: DprW100Doc('1', re.compile('^"Aaron Aaron \\( or ; ""Ahärôn""\\) is a prophet, high priest, and the brother of Moses in the Abrahamic.{412} brother\'s spokesman \\(""prophet""\\) to the Pharaoh\\. Part of the Law \\(Torah\\) that Moses received from"$', flags=48), 'Aaron'),
            9: DprW100Doc('10', re.compile('^"families some time in Israel\'s past\\. Others argue that the story simply shows what can happen if th.{397}ho affirmed Moses\' uniqueness as the one with whom the spoke face to face\\. Miriam was punished with"$', flags=48), 'Aaron'),
            21015323: DprW100Doc('21015324', re.compile('^"committee was established before the building was opened\\. It is the District Nursing base for North.{425}ontains 81 extra care apartments two GP surgeries, a public library, a community café, an optician,"$', flags=48), '"Limelight centre"'),
        })

    def test_queries(self):
        self._test_queries('dpr-w100/nq/train', count=58880, items={
            0: DprW100Query('0', 'big little lies season 2 how many episodes', ('seven',)),
            9: DprW100Query('9', 'who is in charge of enforcing the pendleton act of 1883', ('United States Civil Service Commission',)),
            58879: DprW100Query('58879', 'who plays the army guy in pitch perfect 3', ('Matt Lanter', 'Troy Ian Hall')),
        })
        self._test_queries('dpr-w100/nq/dev', count=6515, items={
            0: DprW100Query('0', 'who sings does he love me with reba', ('Linda Davis',)),
            9: DprW100Query('9', 'what is the name of wonder womans mother', ('Queen Hippolyta',)),
            6514: DprW100Query('6514', 'girl from the shut up and dance video', ('Lauren Taft',)),
        })

    def test_qrels(self):
        self._test_qrels('dpr-w100/nq/train', count=8856662, items={
            0: TrecQrel('0', '18768923', 2, '0'),
            9: TrecQrel('0', '928112', 0, '0'),
            8856661: TrecQrel('58879', '14546521', -1, '0'),
        })
        self._test_qrels('dpr-w100/nq/dev', count=979893, items={
            0: TrecQrel('0', '11828866', 2, '0'),
            9: TrecQrel('0', '9446572', 0, '0'),
            979892: TrecQrel('6514', '11133390', -1, '0'),
        })


if __name__ == '__main__':
    unittest.main()
