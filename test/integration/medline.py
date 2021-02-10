import re
import unittest
import ir_datasets
from ir_datasets.datasets.medline import MedlineDoc, TrecGenomicsQuery, TrecPmQuery, TrecPm2017Query
from ir_datasets.formats import GenericQuery, TrecQrel
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestMedline(DatasetIntegrationTest):
    def test_medline_docs(self):
        self._test_docs('medline/2004', count=3672808, items={
            0: MedlineDoc('10605436', 'Concerning the localization of steroids in centrioles and basal bodies by immunofluorescence.', re.compile('^Specific steroid antibodies, by the immunofluorescence technique, regularly reveal fluorescent centr.{374}y affect cell growth and differentiation in some way different from the two\\-step receptor mechanism\\.$', flags=48)),
            9: MedlineDoc('10605445', 'Intracellular divalent cation release in pancreatic acinar cells during stimulus-secretion coupling. II. Subcellular localization of the fluorescent probe chlorotetracycline.', re.compile('^Subcellular distribution of the divalent cation\\-sensitive probe chlorotetracycline \\(CTC\\) was observe.{1665}ase of calcium from either mitochondria or another organelle that requires ATP to sequester calcium\\.$', flags=48)),
            3672807: MedlineDoc('9864604', '[Is the treatment of left ventricular systolic dysfunction different according to the etiology?]', re.compile('^Cardiac failure is the terminal stage of evolution, the finality of many valvular, vascular, myocard.{836}negligible: the medications are based on the results of large scale, controlled, therapeutic trials\\.$', flags=48)),
        })
        self._test_docs('medline/2017', count=26740025, items={
            0: MedlineDoc('AACR_2014-3448', re.compile('^Fibroblast growth factor receptor is expressed as a constitutively active receptor tyrosine kinase i.{2}chronic lymphocytic leukemia B cells and exists in an active complex with Axl: Dual targeting in CLL$', flags=48), re.compile('^B cell chronic lymphocytic leukemia \\(CLL\\) is an incurable disease and represents a significant healt.{2636}gation as a way to develop a more effective and efficient therapeutic intervention for CLL patients\\.$', flags=48)),
            9: MedlineDoc('ASCO_189985-199', 'Comprehensive molecular and immune profiling of non-small cell lung cancer and matched distant metastases to suggest distinct molecular mechanisms underlying metastasis.', re.compile('^Background: Despite complete resection, many non\\-small cell lung cancer \\(NSCLC\\) patients still devel.{1949}nts\\. Furthermore, immune suppression may be a characteristic of cancer cells of metastatic capacity\\.$', flags=48)),
            26740024: MedlineDoc('27868941', 'Vote of no confidence in trust board.', 'PROPOSALS TO close two medical wards have led nurses at Llandough Hospital, South Glamorgan, to pass a vote of no confidence in their trust board.'),
        })

    def test_medline_queries(self):
        self._test_queries('medline/2004/trec-genomics-2004', count=50, items={
            0: TrecGenomicsQuery('1', 'Ferroportin-1 in humans', 'Find articles about Ferroportin-1, an iron transporter, in humans.', 'Ferroportin1 (also known as SLC40A1; Ferroportin 1; FPN1; HFE4; IREG1; Iron regulated gene 1; Iron-regulated transporter 1; MTP1; SLC11A3; and Solute carrier family 11 (proton-coupled divalent metal ion transporters), member 3) may play a role in iron transport.'),
            9: TrecGenomicsQuery('10', 'NEIL1', 'Find articles about the role of NEIL1 in repair of DNA.', 'Interested in role that NEIL1 plays in DNA repair.'),
            49: TrecGenomicsQuery('50', 'Low temperature protein expression in E. coli', 'Find research on improving protein expressions at low temperature in Escherichia coli bacteria.', 'The researcher is not satisfied with the yield of expressing a protein in E. coli when grown at low temperature and is searching for a better solution. The researcher is willing to try a different organism and/or method.'),
        })
        self._test_queries('medline/2004/trec-genomics-2005', count=50, items={
            0: GenericQuery('100', 'Describe the procedure or methods for how to "open up" a cell through a process called "electroporation."'),
            9: GenericQuery('109', "Describe the procedure or methods for fluorogenic 5'-nuclease assay."),
            49: GenericQuery('149', 'Provide information about Mutations of the alpha 4-GABAA receptor and its/their impact on behavior.'),
        })
        self._test_queries('medline/2017/trec-pm-2017', count=30, items={
            0: TrecPm2017Query('1', 'Liposarcoma', 'CDK4 Amplification', '38-year-old male', 'GERD'),
            9: TrecPm2017Query('10', 'Lung adenocarcinoma', 'KRAS (G12C)', '61-year-old female', 'Hypertension, Hypercholesterolemia'),
            29: TrecPm2017Query('30', 'Pancreatic adenocarcinoma', 'RB1, TP53, KRAS', '57-year-old female', 'None'),
        })
        self._test_queries('medline/2017/trec-pm-2018', count=50, items={
            0: TrecPmQuery('1', 'melanoma', 'BRAF (V600E)', '64-year-old male'),
            9: TrecPmQuery('10', 'melanoma', 'KIT (L576P)', '65-year-old female'),
            49: TrecPmQuery('50', 'acute myeloid leukemia', 'FLT3', '13-year-old male'),
        })

    def test_medline_qrels(self):
        self._test_qrels('medline/2004/trec-genomics-2004', count=8268, items={
            0: TrecQrel('1', '10077651', 2, '0'),
            9: TrecQrel('1', '10449402', 2, '0'),
            8267: TrecQrel('50', '9951698', 1, '0'),
        })
        self._test_qrels('medline/2004/trec-genomics-2005', count=39958, items={
            0: TrecQrel('100', '10023709', 0, '0'),
            9: TrecQrel('100', '10138840', 0, '0'),
            39957: TrecQrel('149', '9989364', 0, '0'),
        })
        self._test_qrels('medline/2017/trec-pm-2017', count=22642, items={
            0: TrecQrel('1', '10065107', 0, '0'),
            9: TrecQrel('1', '10755400', 2, '0'),
            22641: TrecQrel('30', 'ASCO_88462-115', 2, '0'),
        })
        self._test_qrels('medline/2017/trec-pm-2018', count=22429, items={
            0: TrecQrel('1', '1007359', 0, '0'),
            9: TrecQrel('1', '13188512', 0, '0'),
            22428: TrecQrel('50', 'ASCO_35470-65', 2, '0'),
        })


if __name__ == '__main__':
    unittest.main()
