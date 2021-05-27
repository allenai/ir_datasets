import re
import unittest
from ir_datasets.datasets.cranfield import CranfieldDoc
from ir_datasets.formats import TrecQrel, GenericQuery
from .base import DatasetIntegrationTest


class TestCranfield(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('cranfield', count=1400, items={
            0: CranfieldDoc('1', 'experimental investigation of the aerodynamics of a\nwing in a slipstream .', re.compile('^experimental investigation of the aerodynamics of a\nwing in a slipstream \\.\n  an experimental study o.{710}cal evaluation of the destalling effects was made for\nthe specific configuration of the experiment \\.$', flags=48), 'brenckman,m.', 'j. ae. scs. 25, 1958, 324.'),
            9: CranfieldDoc('10', 'the theory of the impact tube at low pressure .', re.compile('^the theory of the impact tube at low pressure \\.\n  a theoretical analysis has been made for an impact.{131}sures \\.\nit is shown that the results differ appreciably from the\ncorresponding continuum relations \\.$', flags=48), 'chambre,p.l. and schaaf,s.a.', 'j. ae. scs. 15, 1948, 735.'),
            1399: CranfieldDoc('1400', 'the buckling shear stress of simply-supported infinitely\nlong plates with transverse stiffeners .', re.compile('^the buckling shear stress of simply\\-supported infinitely\nlong plates with transverse stiffeners \\.\n  .{466}lete range\nof stiffnesses, for panels with ratios of width to stiffener spacing of\ngraphical forms \\.$', flags=48), 'kleeman,p.w.', 'arc r + m.2971, 1953.'),
        })

    def test_queries(self):
        self._test_queries('cranfield', count=225, items={
            0: GenericQuery('1', 'what similarity laws must be obeyed when constructing aeroelastic models\nof heated high speed aircraft .'),
            9: GenericQuery('18', 'are real-gas transport properties for air available over a wide range of\nenthalpies and densities .'),
            224: GenericQuery('365', 'what design factors can be used to control lift-drag ratios at mach\nnumbers above 5 .'),
        })

    def test_qrels(self):
        self._test_qrels('cranfield', count=1837, items={
            0: TrecQrel('1', '184', 2, '0'),
            9: TrecQrel('1', '57', 2, '0'),
            1836: TrecQrel('225', '1188', -1, '0'),
        })

if __name__ == '__main__':
    unittest.main()
