import re
import unittest
import ir_datasets
from ir_datasets.datasets.medline import MedlineDoc, TrecGenomicsQuery
from ir_datasets.formats import GenericQuery, TrecQrel
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestMedline(DatasetIntegrationTest):
    def test_medline_docs(self):
        self._test_docs('medline', count=3672808, items={
            0: MedlineDoc('10605436', 'Concerning the localization of steroids in centrioles and basal bodies by immunofluorescence.', re.compile('^Specific steroid antibodies, by the immunofluorescence technique, regularly reveal fluorescent centr.{374}y affect cell growth and differentiation in some way different from the two\\-step receptor mechanism\\.$', flags=48)),
            9: MedlineDoc('10605445', 'Intracellular divalent cation release in pancreatic acinar cells during stimulus-secretion coupling. II. Subcellular localization of the fluorescent probe chlorotetracycline.', re.compile('^Subcellular distribution of the divalent cation\\-sensitive probe chlorotetracycline \\(CTC\\) was observe.{1665}ase of calcium from either mitochondria or another organelle that requires ATP to sequester calcium\\.$', flags=48)),
            3672807: MedlineDoc('9864604', '[Is the treatment of left ventricular systolic dysfunction different according to the etiology?]', re.compile('^Cardiac failure is the terminal stage of evolution, the finality of many valvular, vascular, myocard.{836}negligible: the medications are based on the results of large scale, controlled, therapeutic trials\\.$', flags=48)),
        })

    def test_medline_queries(self):
        self._test_queries('medline/trec-genomics-2004', count=50, items={
            0: TrecGenomicsQuery('1', 'Ferroportin-1 in humans', 'Find articles about Ferroportin-1, an iron transporter, in humans.', 'Ferroportin1 (also known as SLC40A1; Ferroportin 1; FPN1; HFE4; IREG1; Iron regulated gene 1; Iron-regulated transporter 1; MTP1; SLC11A3; and Solute carrier family 11 (proton-coupled divalent metal ion transporters), member 3) may play a role in iron transport.'),
            9: TrecGenomicsQuery('10', 'NEIL1', 'Find articles about the role of NEIL1 in repair of DNA.', 'Interested in role that NEIL1 plays in DNA repair.'),
            49: TrecGenomicsQuery('50', 'Low temperature protein expression in E. coli', 'Find research on improving protein expressions at low temperature in Escherichia coli bacteria.', 'The researcher is not satisfied with the yield of expressing a protein in E. coli when grown at low temperature and is searching for a better solution. The researcher is willing to try a different organism and/or method.'),
        })
        self._test_queries('medline/trec-genomics-2005', count=50, items={
            0: GenericQuery('100', 'Describe the procedure or methods for how to "open up" a cell through a process called "electroporation."'),
            9: GenericQuery('109', "Describe the procedure or methods for fluorogenic 5'-nuclease assay."),
            49: GenericQuery('149', 'Provide information about Mutations of the alpha 4-GABAA receptor and its/their impact on behavior.'),
        })

    def test_medline_qrels(self):
        self._test_qrels('medline/trec-genomics-2004', count=8268, items={
            0: TrecQrel('1', '10077651', 2, '0'),
            9: TrecQrel('1', '10449402', 2, '0'),
            8267: TrecQrel('50', '9951698', 1, '0'),
        })
        self._test_qrels('medline/trec-genomics-2005', count=39958, items={
            0: TrecQrel('100', '10023709', 0, '0'),
            9: TrecQrel('100', '10138840', 0, '0'),
            39957: TrecQrel('149', '9989364', 0, '0'),
        })


if __name__ == '__main__':
    unittest.main()
