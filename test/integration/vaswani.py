import re
import unittest
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel
from .base import DatasetIntegrationTest


class TestVaswani(DatasetIntegrationTest):
    def test_vaswani_docs(self):
        self._test_docs('vaswani', count=11429, items={
            0: GenericDoc('1', 'compact memories have flexible capacities  a digital data storage\nsystem with capacity up to bits and random and or sequential access\nis described\n'),
            9: GenericDoc('10', 'highspeed microwave switching of semiconductors part\n'),
            11428: GenericDoc('11429', re.compile('^pattern detection and recognition  both processes have been carried\nout on an ibm computer which was.{56} tested included the recognition process for\nreading handlettered sansserif alphanumeric characters\n$', flags=48)),
        })

    def test_vaswani_queries(self):
        self._test_queries('vaswani', count=93, items={
            0: GenericQuery('1', 'MEASUREMENT OF DIELECTRIC CONSTANT OF LIQUIDS BY THE USE OF MICROWAVE TECHNIQUES\n'),
            9: GenericQuery('10', 'METHODS OF CALCULATING INSTANTANEOUS POWER DISSIPATION IN REACTIVE CIRCUITS\n'),
            92: GenericQuery('93', 'HIGH FREQUENCY OSCILLATORS USING TRANSISTORS THEORETICAL TREATMENT AND PRACTICAL CIRCUIT DETAILS\n'),
        })

    def test_vaswani_qrels(self):
        self._test_qrels('vaswani', count=2083, items={
            0: TrecQrel('1', '1239', 1, '0'),
            9: TrecQrel('1', '6824', 1, '0'),
            2082: TrecQrel('93', '11318', 1, '0'),
        })


if __name__ == '__main__':
    unittest.main()
