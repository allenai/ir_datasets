import re
import unittest
from ir_datasets.formats import TrecQrel, GenericScoredDoc
from ir_datasets.datasets.sr_qa import SrQaPassage, SrQaQuery
from .base import DatasetIntegrationTest


class TestSrQa(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('sr-qa/train-2025', count=1787826, items={
            0: SrQaPassage(doc_id='PMC10001629:0', text='With countries progressing towards high COVID-19 vaccination rates, strategies for border reopening are required.', type='sentence', section='S1'),
            9: SrQaPassage(doc_id='PMC10001629:9', text='Provided healthcare systems have sufficient capacity, great economic benefits can be gained for both countries by relaxing border control measures.', type='sentence', section='S1'),
            1787825: SrQaPassage(doc_id='PMC9998919:402', text='SMI, skeletal muscle index.', type='sentence', section='S25'),
        })
        self._test_docs('sr-qa/train-2026', count=2858715, items={
            0: SrQaPassage(doc_id='PMC10000131:0', text='Simple SummaryThe impact of sarcopenia (i.e., progressive and generalised loss of skeletal muscle mass) and obesity on survival are substantially investigated in cancer patients.', type='sentence', section='S1'),
            9: SrQaPassage(doc_id='PMC10000131:9', text='Increases of 10% in intramuscular adipose tissue were significantly associated with DFS (HR: 0.60, 95% CI: 0.38 to 0.95) and OS (HR: 0.60, 95% CI: 0.37 to 0.95), while increases of 10% in subcutaneous adipose tissue were associated with DFS (HR: 0.59, 95% CI: 0.36 to 0.95).', type='sentence', section='S1'),
            2858714: SrQaPassage(doc_id='PMC9999554:441', text='The participant did not however fulfil the criteria for Scheuermann´s disease', type='sentence', section='S21'),
        })
        self._test_docs('sr-qa/dev-2026', count=426791, items={
            0: SrQaPassage(doc_id='PMC10000441:0', text='Purpose: The detection of where an organ starts and where it ends is achievable and, since this information can be delivered in real time, it could be quite important for several reasons.', type='sentence', section='S1'),
            9: SrQaPassage(doc_id='PMC10000441:9', text='The test dataset was further evaluated by one endoscopist, and his findings were compared with CNN-based results.', type='sentence', section='S1'),
            426790: SrQaPassage(doc_id='PMC9994387:653', text='5: Calibration curves of a the training set and b testing set', type='sentence', section='S17'),
        })
        self._test_docs('sr-qa/test-2026', count=1024624, items={
            0: SrQaPassage(doc_id='PMC10001111:0', text='Simple SummaryThe treatment strategies of patients with melanoma brain metastases are continually evolving, although this remains a poor prognostic subset.', type='sentence', section='S1'),
            9: SrQaPassage(doc_id='PMC10001111:9', text='Lactate dehydrogenase (LDH) levels two times higher than the upper limit normal (ULN) at the time of brain metastasis onset was associated with poor prognosis (p = 0.0452) and identified those patients who did not benefit from eRT.', type='sentence', section='S1'),
            1024623: SrQaPassage(doc_id='PMC9998614:382', text='The data are shown as the mean ± SD (n = 3). *p < 0.05, **p < 0.01.', type='sentence', section='S23'),
        })
        self._test_docs('sr-qa/test-2025', count=488756, items={
            0: SrQaPassage(doc_id='PMC10001592:0', text='International migrant workers contribute significantly to the economic growth of the receiving country, and yet their health, especially their mental health, has long been overlooked.', type='sentence', section='S1'),
            9: SrQaPassage(doc_id='PMC10001592:9', text='One of the most important social phenomena in the global context is the flow of international migrant workers.', type='sentence', section='S2'),
            488755: SrQaPassage(doc_id='PMC9996048:212', text='(B) Duration of the common climate risk window weeks in each prefecture-level administrative region, with the first green point denoting the starting week and the second green point denoting the ending week, respectively.', type='sentence', section='S21'),
        })

    def test_queries(self):
        self._test_queries('sr-qa/train-2025', count=14062, items={
            0: SrQaQuery(query_id='PMC12648963-t4-g7-c5', text='What economic evaluation type, modelling approach, perspective, time horizon, and discounting rate were used in this study?', pmcid='PMC10001629', content='characteristic'),
            9: SrQaQuery(query_id='PMC12684331-t2-g2-c12', text='What was the length of follow-up for this group? (group: Brace)', pmcid='PMC10004240', content='characteristic'),
            14061: SrQaQuery(query_id='PMC12665920-t1-g1-c6', text='What was the therapeutic response to TNF inhibitor treatment?', pmcid='PMC9998688', content='result'),
        })
        self._test_queries('sr-qa/train-2026', count=20106, items={
            0: SrQaQuery(query_id='PMC13073668-t1-g21-c1', text='In which country was the study conducted?', pmcid='PMC10000131', content='characteristic'),
            9: SrQaQuery(query_id='PMC12954728-t1-g14-c5', text='How many patients in the overall study population had MVI?', pmcid='PMC10001339', content='characteristic'),
            20105: SrQaQuery(query_id='PMC13124787-t5-g22-c8', text='How many participants (and how many female) were in the control group?', pmcid='PMC9999554', content='characteristic'),
        })
        self._test_queries('sr-qa/dev-2026', count=3227, items={
            0: SrQaQuery(query_id='PMC13162847-t1-g47-c10', text='What was the study design (e.g., retrospective or prospective)?', pmcid='PMC10000441', content='characteristic'),
            9: SrQaQuery(query_id='PMC13026923-t2-g13-c5', text='What was the total sample size included in the study?', pmcid='PMC10002011', content='characteristic'),
            3226: SrQaQuery(query_id='PMC13286850-t4-g16-c8', text='What was the negative predictive value (NPV) of the model on the testing set?', pmcid='PMC9994387', content='result'),
        })
        self._test_queries('sr-qa/test-2026', count=7500, items={
            0: SrQaQuery(query_id='PMC13026811-t1-g21-c5', text='Which survival outcomes (e.g., overall survival, progression-free survival) were assessed in this study?', pmcid='PMC10001111', content='characteristic'),
            9: SrQaQuery(query_id='PMC12823546-t1-g12-c2', text='In which country was the study conducted?', pmcid='PMC10011458', content='characteristic'),
            7499: SrQaQuery(query_id='PMC13180913-t1-g7-c5', text='What dose(s) of sinomenine (mg/kg/day) were administered?', pmcid='PMC9998614', content='characteristic'),
        })
        self._test_queries('sr-qa/test-2025', count=3324, items={
            0: SrQaQuery(query_id='PMC12597289-t1-g3-c2', text='What is the country of origin of the migrant workers studied?', pmcid='PMC10001592', content='characteristic'),
            9: SrQaQuery(query_id='PMC12690695-t1-g27-c9', text='What cost-effectiveness threshold was used to judge the intervention? (group: Zimbabwe, Goals-ASM)', pmcid='PMC10005968', content='characteristic'),
            3323: SrQaQuery(query_id='PMC12570654-t1-g9-c7', text='What statistical method or model was used to analyze the association between meteorological factors and scrub typhus incidence?', pmcid='PMC9996048', content='characteristic'),
        })

    def test_qrels(self):
        self._test_qrels('sr-qa/train-2025', count=44201, items={
            0: TrecQrel(query_id='PMC12648963-t4-g7-c5', doc_id='PMC10001629:50', relevance=1, iteration='0'),
            9: TrecQrel(query_id='PMC12735994-t1-g1-c4', doc_id='PMC10002462:59', relevance=2, iteration='0'),
            44200: TrecQrel(query_id='PMC12665920-t1-g1-c6', doc_id='PMC9998688:31', relevance=2, iteration='0'),
        })
        self._test_qrels('sr-qa/train-2026', count=63390, items={
            0: TrecQrel(query_id='PMC13073668-t1-g21-c1', doc_id='PMC10000131:28', relevance=2, iteration='0'),
            9: TrecQrel(query_id='PMC13203857-t1-g107-c4', doc_id='PMC10001175:4', relevance=2, iteration='0'),
            63389: TrecQrel(query_id='PMC13124787-t5-g22-c8', doc_id='PMC9999554:281', relevance=1, iteration='0'),
        })
        self._test_qrels('sr-qa/dev-2026', count=9773, items={
            0: TrecQrel(query_id='PMC13162847-t1-g47-c10', doc_id='PMC10000441:33', relevance=2, iteration='0'),
            9: TrecQrel(query_id='PMC13197355-t2-g52-c2', doc_id='PMC10001987:29', relevance=1, iteration='0'),
            9772: TrecQrel(query_id='PMC13286850-t4-g16-c8', doc_id='PMC9994387:629', relevance=1, iteration='0'),
        })
        self._test_qrels('sr-qa/test-2026', count=23277, items={
            0: TrecQrel(query_id='PMC13026811-t1-g21-c5', doc_id='PMC10001111:5', relevance=2, iteration='0'),
            9: TrecQrel(query_id='PMC13158234-t1-g21-c1', doc_id='PMC10008929:163', relevance=2, iteration='0'),
            23276: TrecQrel(query_id='PMC13180913-t1-g7-c5', doc_id='PMC9998614:136', relevance=2, iteration='0'),
        })
        self._test_qrels('sr-qa/test-2025', count=11210, items={
            0: TrecQrel(query_id='PMC12597289-t1-g3-c2', doc_id='PMC10001592:30', relevance=2, iteration='0'),
            9: TrecQrel(query_id='PMC12597289-t1-g3-c5', doc_id='PMC10001592:78', relevance=2, iteration='0'),
            11209: TrecQrel(query_id='PMC12570654-t1-g9-c7', doc_id='PMC9996048:70', relevance=2, iteration='0'),
        })

    def test_scoreddocs(self):
        self._test_scoreddocs('sr-qa/train-2025', count=6004195, items={
            0: GenericScoredDoc(query_id='PMC12648963-t4-g7-c5', doc_id='PMC10001629:0', score=0.0),
            9: GenericScoredDoc(query_id='PMC12648963-t4-g7-c5', doc_id='PMC10001629:9', score=0.0),
            6004194: GenericScoredDoc(query_id='PMC12665920-t1-g1-c6', doc_id='PMC9998688:230', score=0.0),
        })
        self._test_scoreddocs('sr-qa/train-2026', count=8190114, items={
            0: GenericScoredDoc(query_id='PMC13073668-t1-g21-c1', doc_id='PMC10000131:0', score=0.0),
            9: GenericScoredDoc(query_id='PMC13073668-t1-g21-c1', doc_id='PMC10000131:9', score=0.0),
            8190113: GenericScoredDoc(query_id='PMC13124787-t5-g22-c8', doc_id='PMC9999554:441', score=0.0),
        })
        self._test_scoreddocs('sr-qa/dev-2026', count=1374542, items={
            0: GenericScoredDoc(query_id='PMC13162847-t1-g47-c10', doc_id='PMC10000441:0', score=0.0),
            9: GenericScoredDoc(query_id='PMC13162847-t1-g47-c10', doc_id='PMC10000441:9', score=0.0),
            1374541: GenericScoredDoc(query_id='PMC13286850-t4-g16-c8', doc_id='PMC9994387:653', score=0.0),
        })
        self._test_scoreddocs('sr-qa/test-2026', count=3024342, items={
            0: GenericScoredDoc(query_id='PMC13026811-t1-g21-c5', doc_id='PMC10001111:0', score=0.0),
            9: GenericScoredDoc(query_id='PMC13026811-t1-g21-c5', doc_id='PMC10001111:9', score=0.0),
            3024341: GenericScoredDoc(query_id='PMC13180913-t1-g7-c5', doc_id='PMC9998614:382', score=0.0),
        })
        self._test_scoreddocs('sr-qa/test-2025', count=1350511, items={
            0: GenericScoredDoc(query_id='PMC12597289-t1-g3-c2', doc_id='PMC10001592:0', score=0.0),
            9: GenericScoredDoc(query_id='PMC12597289-t1-g3-c2', doc_id='PMC10001592:9', score=0.0),
            1350510: GenericScoredDoc(query_id='PMC12570654-t1-g9-c7', doc_id='PMC9996048:212', score=0.0),
        })


if __name__ == '__main__':
    unittest.main()
