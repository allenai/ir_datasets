import re
import unittest
import ir_datasets
from ir_datasets.datasets.clinicaltrials import ClinicalTrialsDoc
from ir_datasets.datasets.medline import TrecPmQuery, TrecPm2017Query
from ir_datasets.formats import GenericQuery, TrecQrel
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestClinicalTrials(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('clinicaltrials/2017', count=241006, items={
            0: ClinicalTrialsDoc('NCT00530868', 'Comparing Letrozole Given Alone to Letrozole Given With Avastin in Post-Menopausal Women Breast Cancer', '', re.compile('^\n    \n      This purpose of this trial is to show that the combination of Avastin and hormone therap.{3}     should be more effective than hormone therapy alone for the treatment of breast cancer\\.\n    \n  $', flags=48), re.compile('^\n    \n      Preclinical and clinical data have demonstrated that up\\-regulation of tumor cell VEGF is.{329}ould be more effective than hormonal therapy alone for the\n      treatment of breast cancer\\.\n    \n  $', flags=48), re.compile('^\n      \n        Inclusion Criteria:\n\n        All patients must meet the following criteria to be eli.{4158}and carcinoma in\\-situ of uterine cervix\\.\n\n          \\-  Patients with metastatic disease\\.\n      \n    $', flags=48)),
            9: ClinicalTrialsDoc('NCT00530101', 'The Magnetic Resonance Imaging Evaluation of Doxorubicin Cardiotoxicity', '', re.compile('^\n    \n      The purpose of this research study is to evaluate MR imaging in subjects receiving\n     .{166}ely 10 subjects over 12 months at the\n      University of Miami / Miller School of Medicine\\.\n    \n  $', flags=48), re.compile('^\n    \n      Doxorubicin \\(Adriamycin\\) is one of the most widely used chemotherapy agents, despite its.{9660}ocardium\\.\n\n      Medical records will provide data regarding cardiac morbidity or mortality\\.\n    \n  $', flags=48), re.compile('^\n      \n        Inclusion Criteria:\n\n          \\-  Subject must have breast cancer and undergoing rad.{112}      \\-  Healthy subjects\n\n          \\-  Males\n\n          \\-  Subjects under the age of 18\n      \n    $', flags=48)),
            241005: ClinicalTrialsDoc('NCT00074646', 'Phase I Trial of CC-8490 for the Treatment of Subjects With Recurrent/Refractory High-Grade Gliomas', '', '\n    \n      Phase I trial of CC-8490 for the treatment of subjects with recurrent/refractory high-grade\n      gliomas\n    \n  ', '', re.compile('^\n      \n        Inclusion Criteria:\n\n          \\-  Patients with glioblastoma multiforme \\(GBM\\), glios.{4283}lism\\.\n\n          \\-  Use of other experimental study drug within 28 days of registration\\.\n      \n    $', flags=48)),
        })
        self._test_docs('clinicaltrials/2019', count=306238, items={
            0: ClinicalTrialsDoc('NCT00704457', 'Impact Of Sacral Neuromodulation On Urine Markers For Interstitial Cystitis (IC)', '', '\n    \n      Urine will be collected and sent to the University of Maryland. Urines will be analyzed for\n      urine markers.\n    \n  ', re.compile('^\n    \n      Urine will be collected and flash frozen in liquid nitrogen then placed in a \\-70 C freez.{376} in urine\n      marker levels will be analyzed and correlated with change in symptom scores\\.\n    \n  $', flags=48), '\n      \n        Inclusion Criteria:\n\n          -  Patients will be drawn from Dr. Peters patient base that covers Southeast Michigan.\n\n        Exclusion Criteria:\n\n          -  Male\n      \n    '),
            9: ClinicalTrialsDoc('NCT00705887', 'A Motivational Enhancement Approach to Skin Cancer Prevention', '', re.compile('^\n    \n      The specific aims of this research are:\n\n      Aim 1 \\- To describe the UV protection beh.{579}      protection stages of change, UV protection self\\-efficacy, and UV protection attitudes\\.\n    \n  $', flags=48), re.compile('^\n    \n      Although skin cancer is the most common form of cancer in the United States, it is highl.{806}nvestigated the application of these techniques to\n      skin cancer prevention discussions\\.\n    \n  $', flags=48), re.compile('^\n      \n        Inclusion Criteria:\n\n          \\-  Dermatology patient presenting for scheduled appoi.{157}lish\n\n          \\-  Having previously received medical treatment from the interventionist\n      \n    $', flags=48)),
            306237: ClinicalTrialsDoc('NCT03548415', 'Safety, Tolerability, and Efficacy of IONIS-GHR-LRx in up to 42 Adult Patients With Acromegaly Being Treated With Long-acting Somatostatin Receptor Ligands', '', '\n    \n      The purpose is to assess the Safety, Tolerability, and Efficacy of IONIS-GHR-LRx in up to 42\n      Patients with Acromegaly\n    \n  ', re.compile('^\n    \n      This short\\-term study will assess changes in serum insulin\\-like growth factor 1 \\(IGF\\-1\\) .{68}sed with Acromegaly being treated\n      with Long\\-acting Somatostatin Receptor Ligands \\(SRL\\)\n    \n  $', flags=48), re.compile('^\n      \n        Inclusion Criteria:\n\n          1\\. Males or females with documented diagnosis of Acro.{2002}     stable dose and regimen for >= 3 months prior to screening and throughout the trial\n      \n    $', flags=48)),
        })
        self._test_docs('clinicaltrials/2021', count=375580, items={
            0: ClinicalTrialsDoc('NCT00000102', 'Congenital Adrenal Hyperplasia: Calcium Channels as Therapeutic Targets', '', re.compile('^\n    \n      This study will test the ability of extended release nifedipine \\(Procardia XL\\), a blood\\\r.{66}ucocorticoid medication children\\\r\n      take to treat congenital adrenal hyperplasia \\(CAH\\)\\.\\\r\n    \n  $', flags=48), re.compile('^\n    \n      This protocol is designed to assess both acute and chronic effects of the calcium channe.{716}e would, in turn, reduce the deleterious effects of glucocorticoid\\\r\n      treatment in CAH\\.\\\r\n    \n  $', flags=48), re.compile('^\n      \n        Inclusion Criteria:\\\r\n\\\r\n          \\-  diagnosed with Congenital Adrenal Hyperplasia \\(C.{126}ase, or elevated liver function tests\\\r\n\\\r\n          \\-  history of cardiovascular disease\\\r\n      \n    $', flags=48)),
            9: ClinicalTrialsDoc('NCT00000113', 'Correction of Myopia Evaluation Trial (COMET)', '', re.compile('^\n    \n      To evaluate whether progressive addition lenses \\(PALs\\) slow the rate of progression of\\\r\n.{291}opia in a group of children receiving\\\r\n      conventional treatment \\(single vision lenses\\)\\.\\\r\n    \n  $', flags=48), re.compile('^\n    \n      Myopia \\(nearsightedness\\) is an important public health problem, which entails substantia.{3027}e secondary outcome of the study is axial length measured by A\\-scan\\\r\n      ultrasonography\\.\\\r\n    \n  $', flags=48), re.compile('^\n      \n        Children between the ages of 6 and 12 years with myopia in both eyes \\(defined as sph.{484}ssive\\\r\n        addition lenses, or any conditions precluding adherence to the protocol\\.\\\r\n      \n    $', flags=48)),
            375579: ClinicalTrialsDoc('NCT04862312', 'Video Chat During Meals to Improve Nutritional Intake in Older Adults', '', re.compile('^\n    \n      The VideoDining study is a Stage IB behavioral intervention development project\\. The\\\r\n  .{184}to\\\r\n      evaluate changes in nutritional intake and loneliness in response to VideoDining\\.\\\r\n    \n  $', flags=48), re.compile('^\n    \n      The U\\.S\\. population is growing older and more adults are aging at home alone, by choice,.{1998}efinement of the\\\r\n      VideoDining intervention are additional key outcomes of this study\\.\\\r\n    \n  $', flags=48), re.compile('^\n      \n        Inclusion Criteria:\\\r\n\\\r\n          1\\. Receive Meals\\-on\\-Wheels meals from Foodnet in To.{370} and participate in the study\\.\\\r\n\\\r\n          5\\. Already own and use an Amazon Echo Show\\.\\\r\n      \n    $', flags=48)),
        })

    def test_queries(self):
        self._test_queries('clinicaltrials/2017/trec-pm-2017', count=30, items={
            0: TrecPm2017Query('1', 'Liposarcoma', 'CDK4 Amplification', '38-year-old male', 'GERD'),
            9: TrecPm2017Query('10', 'Lung adenocarcinoma', 'KRAS (G12C)', '61-year-old female', 'Hypertension, Hypercholesterolemia'),
            29: TrecPm2017Query('30', 'Pancreatic adenocarcinoma', 'RB1, TP53, KRAS', '57-year-old female', 'None'),
        })
        self._test_queries('clinicaltrials/2017/trec-pm-2018', count=50, items={
            0: TrecPmQuery('1', 'melanoma', 'BRAF (V600E)', '64-year-old male'),
            9: TrecPmQuery('10', 'melanoma', 'KIT (L576P)', '65-year-old female'),
            49: TrecPmQuery('50', 'acute myeloid leukemia', 'FLT3', '13-year-old male'),
        })
        self._test_queries('clinicaltrials/2019/trec-pm-2019', count=40, items={
            0: TrecPmQuery('1', 'melanoma', 'BRAF (E586K)', '64-year-old female'),
            9: TrecPmQuery('10', 'mucosal melanoma', 'KIT (L576P), KIT amplification', '62-year-old female'),
            39: TrecPmQuery('40', 'malignant hyperthermia', 'RYR1', '54-year-old male'),
        })

    def test_qrels(self):
        self._test_qrels('clinicaltrials/2017/trec-pm-2017', count=13019, items={
            0: TrecQrel('1', 'NCT00001188', 0, '0'),
            9: TrecQrel('1', 'NCT00002898', 0, '0'),
            13018: TrecQrel('30', 'NCT03080974', 0, '0'),
        })
        self._test_qrels('clinicaltrials/2017/trec-pm-2018', count=14188, items={
            0: TrecQrel('1', 'NCT00001452', 0, '0'),
            9: TrecQrel('1', 'NCT00341991', 0, '0'),
            14187: TrecQrel('50', 'NCT03096782', 0, '0'),
        })
        self._test_qrels('clinicaltrials/2019/trec-pm-2019', count=12996, items={
            0: TrecQrel('1', 'NCT00001685', 0, '0'),
            9: TrecQrel('1', 'NCT00119249', 1, '0'),
            12995: TrecQrel('40', 'NCT03955640', 0, '0'),
        })

if __name__ == '__main__':
    unittest.main()
